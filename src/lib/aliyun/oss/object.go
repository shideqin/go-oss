package oss

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"mime"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ListObjectResult struct {
	Name        string               `xml:"Name"`
	Prefix      string               `xml:"Prefix"`
	Marker      string               `xml:"Marker"`
	MaxKeys     string               `xml:"MaxKeys"`
	Delimiter   string               `xml:"Delimiter"`
	IsTruncated string               `xml:"IsTruncated"`
	Contents    []ListObjectContents `xml:"Contents"`
}

type ListObjectContents struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Type         string `xml:"Type"`
	Size         string `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

func (this *Client) UploadFile(filePath, bucket, object string, options map[string]string) (map[string]string, error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("UploadFile:", err)
			os.Exit(2)
		}
	}()
	fd, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer fd.Close()
	body, err := ioutil.ReadAll(fd)
	if err != nil {
		panic(err)
	}
	if object == "" {
		object = path.Base(filePath)
	}
	if strings.TrimRight(object, "/") == path.Dir(object) {
		object = strings.TrimRight(object, "/") + "/" + path.Base(filePath)
	}
	return this.Put(body, bucket, object, map[string]string{"disposition": options["disposition"]})
}

func (this *Client) Put(body []byte, bucket, object string, options map[string]string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s%s/%s", bucket, this.host, object)
	method := "PUT"
	contentType := mime.TypeByExtension(path.Ext(object))
	contentLength := strconv.Itoa(len(body))
	contentMd5 := this.base64(this.md5Byte(body))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(this.dateTimeGMT)
	headers := map[string]string{
		"Content-Md5":  contentMd5,
		"Content-Type": contentType,
		"Date":         date,
	}
	headers["Authorization"] = this.sign(method, headers, bucket, object)
	headers["Content-Length"] = contentLength
	if options["disposition"] != "" {
		headers["Content-Disposition"] = fmt.Sprintf(`attachment; filename="%s"`, options["disposition"])
	}
	res, err := this.curl(addr, method, headers, body)
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, err
	}
	return map[string]string{
		"X-Oss-Request-Id": res["X-Oss-Request-Id"],
		"StatusCode":       res["StatusCode"],
		"Location":         "http://" + bucket + this.host + "/" + object,
		"Bucket":           bucket,
		"ETag":             res["Etag"],
		"Key":              object,
	}, nil
}

func (this *Client) Copy(bucket, object, source string, options map[string]string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s%s/%s", bucket, this.host, object)
	method := "PUT"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(this.dateTimeGMT)
	headers := map[string]string{
		"Date":              date,
		"x-oss-copy-source": source,
	}
	LF := "\n"
	headers["Authorization"] = this.sign(method+LF+LF, headers, bucket, object)
	if options["disposition"] != "" {
		headers["response-content-disposition"] = fmt.Sprintf(`attachment; filename="%s"`, options["disposition"])
	}
	res, err := this.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (this *Client) Delete(bucket, object string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s%s/%s", bucket, this.host, object)
	method := "DELETE"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(this.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = this.sign(method+LF+LF, headers, bucket, object)
	res, err := this.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (this *Client) Head(bucket, object string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s%s/%s", bucket, this.host, object)
	method := "HEAD"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(this.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = this.sign(method+LF+LF, headers, bucket, object)
	res, err := this.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (this *Client) Get(bucket, object, localfile string) (map[string]string, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	objectHead, err := this.Head(bucket, object)
	if err != nil {
		return nil, err
	}
	if objectHead["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + objectHead["StatusCode"])
	}
	objectSize, err := strconv.Atoi(objectHead["Content-Length"])
	if err != nil {
		return nil, err
	}
	//当没指定文件名时，默认使用object的文件名
	if strings.TrimRight(localfile, "/") == path.Dir(localfile) {
		localfile = strings.TrimRight(localfile, "/") + "/" + path.Base(object)
	}

	var total = (objectSize + this.RecvBufferSize - 1) / this.RecvBufferSize
	if total < this.threadMaxNum {
		this.threadMaxNum = total
	}

	var queueMaxSize = make(chan bool, this.threadMaxNum)
	var writePercent = make(chan bool)
	var writeDone = make(chan struct{})

	//实时进度
	go func() {
		finishNum := 0
		for {
			_, ok := <-writePercent
			if !ok {
				close(writeDone)
				break
			}
			finishNum++
			fmt.Printf("\r%.0f%%", float64(finishNum)/float64(total)*100)
		}
	}()

	//创建local文件
	file, err := os.OpenFile(localfile, os.O_CREATE|os.O_WRONLY, 0600)
	defer file.Close()

	partNum := 0
	for {
		if partNum >= total {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(partNum int) {
			defer wg.Done()
			//part范围,如：0-1023
			tmpStart := partNum * this.RecvBufferSize
			tmpEnd := (partNum+1)*this.RecvBufferSize - 1
			if tmpEnd > objectSize {
				tmpEnd = tmpStart + objectSize%this.RecvBufferSize - 1
			}
			partRange := fmt.Sprintf("bytes=%d-%d", tmpStart, tmpEnd)
			isWriteSuccess := false
			for i := 0; i < this.maxRetryNum; i++ {
				tmp, err := this.Cat(bucket, object, partRange)
				if err != nil {
					continue
				}
				if tmp["StatusCode"] != "200" && tmp["StatusCode"] != "206" {
					continue
				}
				_, err = file.WriteAt([]byte(tmp["Body"]), int64(tmpStart))
				if err != nil {
					continue
				}
				isWriteSuccess = true
				break
			}
			if !isWriteSuccess {
				fmt.Printf("\nWrite Part Fail,PartNum:%d\n", partNum)
				os.Exit(2)
			}
			writePercent <- true
			<-queueMaxSize
		}(partNum)
		partNum++
	}
	wg.Wait()
	close(writePercent)
	<-writeDone
	return map[string]string{"object": object, "localfile": localfile}, nil
}

func (this *Client) Cat(bucket, object string, param ...string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s%s/%s", bucket, this.host, object)
	method := "GET"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(this.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = this.sign(method+LF+LF, headers, bucket, object)
	//分片请求
	partRange := ""
	if len(param) > 0 {
		partRange = param[0]
	}
	if partRange != "" {
		headers["Range"] = partRange
	}
	res, err := this.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (this *Client) UploadFromDir(localdir, bucket, prefix string, options map[string]string) (map[string]int, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	suffix := ""
	if options["suffix"] != "" {
		suffix = options["suffix"]
	}
	localdir = strings.TrimRight(localdir, "/") + "/"
	fileList := this.walkdir(localdir, suffix)
	total := len(fileList)
	tmpSkip := int64(0)
	tmpFinish := int64(0)
	if options["thread_num"] != "" {
		threadNum, err := strconv.Atoi(options["thread_num"])
		if err == nil && threadNum <= this.threadMaxNum && threadNum >= this.threadMinNum {
			this.threadMaxNum = threadNum
		}
	}
	if total < this.threadMaxNum {
		this.threadMaxNum = total
	}

	var queueMaxSize = make(chan bool, this.threadMaxNum)
	var filePercent = make(chan bool)
	var fileDone = make(chan struct{})

	//实时进度
	go func() {
		finishNum := 0
		for {
			_, ok := <-filePercent
			if !ok {
				close(fileDone)
				break
			}
			finishNum++
			fmt.Printf("\r%.0f%%", float64(finishNum)/float64(total)*100)
		}
	}()

	fileNum := 0
	for {
		if fileNum >= total {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(fileName string) {
			defer wg.Done()
			object := strings.TrimRight(prefix, "/") + "/"
			//if dir := path.Dir(fileName); dir != "." {
			//	object += dir + "/"
			//}
			//object += this.md5([]byte(path.Base(fileName))) + path.Ext(fileName)
			object += fileName
			object = strings.TrimLeft(object, "/")
			isSkipped := false
			if options["replace"] != "true" {
				localFileStat, err := os.Stat(localdir + fileName)
				if err != nil {
					fmt.Printf("UploadFromDir::Stat Fail,FileName: %s\n", fileName)
					os.Exit(2)
				}
				localFileSize := localFileStat.Size()
				localFileModifyTime := time.Unix(localFileStat.ModTime().Unix()-8*3600, 0).Format(this.dateTimeGMT)
				objectHead, _ := this.Head(bucket, object)
				objectHeadSize, _ := strconv.ParseInt(objectHead["Content-Length"], 10, 64)
				if objectHead["StatusCode"] == "200" && localFileSize == objectHeadSize {
					objectTime, _ := time.Parse(this.dateTimeGMT, objectHead["Last-Modified"])
					localTime, _ := time.Parse(this.dateTimeGMT, localFileModifyTime)
					if objectTime.Unix() >= localTime.Unix() {
						isSkipped = true
						atomic.AddInt64(&tmpSkip, 1)
					}
				}
			}
			if !isSkipped {
				fd, err := os.Open(localdir + fileName)
				if err != nil {
					fmt.Printf("UploadFromDir::Open Fail,FileName: %s\n", fileName)
					os.Exit(2)
				}
				defer fd.Close()
				body, err := ioutil.ReadAll(fd)
				if err != nil {
					fmt.Printf("UploadFromDir::ReadAll Fail,FileName: %s\n", fileName)
					os.Exit(2)
				}
				isUploadSuccess := false
				for i := 0; i < this.maxRetryNum; i++ {
					res, err := this.Put(body, bucket, object, map[string]string{"disposition": fileName})
					if err != nil {
						continue
					}
					isUploadSuccess = true
					if res["StatusCode"] == "200" {
						atomic.AddInt64(&tmpFinish, 1)
					}
					break
				}
				if !isUploadSuccess {
					fmt.Printf("\nUploadFromDir::Fail,FileName: %s\n", fileName)
					os.Exit(2)
				}
			}
			filePercent <- true
			<-queueMaxSize
		}(fileList[fileNum])
		fileNum++
	}
	wg.Wait()
	close(filePercent)
	<-fileDone
	skip := int(atomic.LoadInt64(&tmpSkip))
	finish := int(atomic.LoadInt64(&tmpFinish))
	return map[string]int{"total": total, "skip": skip, "finish": finish}, nil
}

func (this *Client) ListObject(bucket string, options map[string]string) (*ListObjectResult, error) {
	param := ""
	if options["delimiter"] != "" {
		param += "&delimiter=" + options["delimiter"]
	}
	if options["marker"] != "" {
		param += "&marker=" + options["marker"]
	}
	if options["max-keys"] != "" {
		param += "&max-keys=" + options["max-keys"]
	}
	if options["prefix"] != "" {
		param += "&prefix=" + options["prefix"]
	}
	addr := "http://" + bucket + this.host + "/?" + strings.TrimLeft(param, "&")
	method := "GET"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(this.dateTimeGMT)
	headers := map[string]string{
		"Date": date,
	}
	LF := "\n"
	headers["Authorization"] = this.sign(method+LF+LF, headers, bucket, "")
	res, err := this.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	var listObject ListObjectResult
	if err := xml.Unmarshal([]byte(res["Body"]), &listObject); err != nil {
		return nil, err
	}
	return &listObject, nil
}

func (this *Client) CopyAllObject(bucket, prefix, source string, options map[string]string) (map[string]int, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	marker := ""
	tmpSourceInfo := strings.Split(source, "/")
	sourceBucket := tmpSourceInfo[1]
	sourcePrefix := strings.Join(tmpSourceInfo[2:], "/")
	total := 0
	tmpSkip := int64(0)
	tmpFinish := int64(0)
	if options["thread_num"] != "" {
		threadNum, err := strconv.Atoi(options["thread_num"])
		if err == nil && threadNum <= this.threadMaxNum && threadNum >= this.threadMinNum {
			this.threadMaxNum = threadNum
		}
	}
	var queueMaxSize = make(chan bool, this.threadMaxNum)
	var copyPercent = make(chan bool)
	var copyDone = make(chan struct{})

	//实时进度
	go func() {
		finishNum := 0
		for {
			_, ok := <-copyPercent
			if !ok {
				close(copyDone)
				break
			}
			finishNum++
			fmt.Printf("\r%.0f%%", float64(finishNum)/float64(total)*100)
		}
	}()
LIST:
	sourceList, err := this.ListObject(sourceBucket, map[string]string{"prefix": sourcePrefix, "marker": marker, "max-keys": "1000"})
	if err != nil {
		return nil, err
	}
	sourceObjectNum := len(sourceList.Contents)
	total += sourceObjectNum
	fileNum := 0
	for {
		if fileNum >= sourceObjectNum {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(objectInfo ListObjectContents) {
			defer wg.Done()
			object := strings.TrimRight(prefix, "/") + "/" + path.Base(objectInfo.Key)
			sourceObject := "/" + sourceBucket + "/" + objectInfo.Key
			isSkipped := false
			if options["replace"] != "true" {
				objectHead, _ := this.Head(bucket, object)
				if objectHead["StatusCode"] == "200" {
					objectHeadSize, _ := strconv.ParseInt(objectHead["Content-Length"], 10, 64)
					sourceHead, _ := this.Head(sourceBucket, objectInfo.Key)
					sourceHeadSize, _ := strconv.ParseInt(sourceHead["Content-Length"], 10, 64)
					if sourceHead["StatusCode"] == "200" && objectHeadSize == sourceHeadSize {
						objectTime, _ := time.Parse(this.dateTimeGMT, objectHead["Last-Modified"])
						sourceTime, _ := time.Parse(this.dateTimeGMT, sourceHead["Last-Modified"])
						if objectTime.Unix() >= sourceTime.Unix() {
							isSkipped = true
							atomic.AddInt64(&tmpSkip, 1)
						}
					}
				}
			}
			if !isSkipped {
				isCopySuccess := false
				for i := 0; i < this.maxRetryNum; i++ {
					res, err := this.Copy(bucket, object, sourceObject, map[string]string{"disposition": options["disposition"]})
					if err != nil {
						continue
					}
					isCopySuccess = true
					if res["StatusCode"] == "200" {
						atomic.AddInt64(&tmpFinish, 1)
					}
					break
				}
				if !isCopySuccess {
					fmt.Printf("\nCopy File Fail,object:%s\n", object)
					os.Exit(2)
				}
			}
			copyPercent <- true
			<-queueMaxSize
		}(sourceList.Contents[fileNum])
		fileNum++
	}
	wg.Wait()
	if sourceList.IsTruncated == "true" {
		marker = sourceList.Contents[sourceObjectNum-1].Key
		goto LIST
	}
	close(copyPercent)
	<-copyDone
	skip := int(atomic.LoadInt64(&tmpSkip))
	finish := int(atomic.LoadInt64(&tmpFinish))
	return map[string]int{"total": total, "skip": skip, "finish": finish}, nil
}

func (this *Client) DeleteAllObject(bucket, prefix string, options map[string]string) (map[string]int, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	bodyList := make([]string, 0)
	bodyListNum := make([]int, 0)
	marker := ""
	total := 0
	tmpFinish := int64(0)
LIST:
	list, err := this.ListObject(bucket, map[string]string{"prefix": prefix, "marker": marker, "max-keys": "1000"})
	if err != nil {
		return nil, err
	}
	total += len(list.Contents)
	if total <= 0 {
		return map[string]int{"total": 0, "finish": 0}, nil
	}
	body := "<Delete>"
	body += "<Quiet>true</Quiet>"
	for _, v := range list.Contents {
		body += "<Object><Key>" + v.Key + "</Key></Object>"
		marker = v.Key
	}
	body += "</Delete>"
	bodyList = append(bodyList, body)
	bodyListNum = append(bodyListNum, len(list.Contents))
	if list.IsTruncated == "true" {
		goto LIST
	}

	if options["thread_num"] != "" {
		threadNum, err := strconv.Atoi(options["thread_num"])
		if err == nil && threadNum <= this.threadMaxNum && threadNum >= this.threadMinNum {
			this.threadMaxNum = threadNum
		}
	}
	var bodyNum = len(bodyList)
	if bodyNum < this.threadMaxNum {
		this.threadMaxNum = bodyNum
	}
	var queueMaxSize = make(chan bool, this.threadMaxNum)
	var deletePercent = make(chan bool)
	var deleteDone = make(chan struct{})

	//实时进度
	go func() {
		finishNum := 0
		for {
			_, ok := <-deletePercent
			if !ok {
				close(deleteDone)
				break
			}
			finishNum++
			fmt.Printf("\r%.0f%%", float64(finishNum)/float64(bodyNum)*100)
		}
	}()

	fileNum := 0
	for {
		if fileNum >= bodyNum {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(fileNum int, body string) {
			defer wg.Done()
			addr := "http://" + bucket + this.host + "/?delete"
			method := "POST"
			date := time.Unix(time.Now().Unix()-8*3600, 0).Format(this.dateTimeGMT)
			contentLength := strconv.Itoa(len(body))
			contentMd5 := this.base64(this.md5Byte([]byte(body)))
			headers := map[string]string{
				"Content-Md5": contentMd5 + "\n",
				"Date":        date,
			}
			headers["Authorization"] = this.sign(method, headers, bucket, "?delete")
			headers["Content-Length"] = contentLength
			headers["Content-Md5"] = strings.TrimRight(headers["Content-Md5"], "\n")
			isDeleteSuccess := false
			for i := 0; i < this.maxRetryNum; i++ {
				res, err := this.curl(addr, method, headers, []byte(body))
				if err != nil {
					continue
				}
				isDeleteSuccess = true
				if res["StatusCode"] == "200" {
					atomic.AddInt64(&tmpFinish, int64(bodyListNum[fileNum]))
				}
				break
			}
			if !isDeleteSuccess {
				fmt.Printf("\nDelete File Fail FileNum:%d\n", fileNum)
				os.Exit(2)
			}
			deletePercent <- true
			<-queueMaxSize
		}(fileNum, bodyList[fileNum])
		fileNum++
	}
	wg.Wait()
	close(deletePercent)
	<-deleteDone
	finish := int(atomic.LoadInt64(&tmpFinish))
	return map[string]int{"total": total, "finish": finish}, nil
}
