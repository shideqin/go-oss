package oss

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type InitUploadResult struct {
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	UploadId string `xml:"UploadId"`
}

type CompleteUploadResult struct {
	Location string `xml:"Location"`
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	ETag     string `xml:"ETag"`
}

func (this *Client) UploadLargeFile(filePath, bucket, object string, options map[string]string) (map[string]string, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	//open本地文件
	fd, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	if options["partsize"] != "" {
		partSize, err := strconv.Atoi(options["partsize"])
		if err == nil && partSize <= this.partMaxSize && partSize >= this.partMinSize {
			this.partMaxSize = partSize
		}
	}
	if options["thread_num"] != "" {
		threadNum, err := strconv.Atoi(options["thread_num"])
		if err == nil && threadNum <= this.threadMaxNum && threadNum >= this.threadMinNum {
			this.threadMaxNum = threadNum
		}
	}
	if object == "" {
		object = path.Base(filePath)
	}
	if strings.TrimRight(object, "/") == path.Dir(object) {
		object = strings.TrimRight(object, "/") + "/" + path.Base(filePath)
	}
	fileStat, _ := fd.Stat()
	fileSize := int(fileStat.Size())

	var total = (fileSize + this.partMaxSize - 1) / this.partMaxSize
	if total < this.threadMaxNum {
		this.threadMaxNum = total
	}
	//初化化上传
	initUpload, err := this.initUpload(bucket, object, map[string]string{"disposition": options["disposition"]})
	if err != nil {
		return nil, err
	}
	var uploadPartList = make([]map[string]string, total)
	var queueMaxSize = make(chan bool, this.threadMaxNum)
	var uploadPercent = make(chan bool)
	var uploadDone = make(chan struct{})

	//实时进度
	go func() {
		finishNum := 0
		for {
			_, ok := <-uploadPercent
			if !ok {
				close(uploadDone)
				break
			}
			finishNum++
			fmt.Printf("\r%.0f%%", float64(finishNum)/float64(total)*100)
		}
	}()

	partNum := 0
	for {
		off := partNum * this.partMaxSize
		if off >= fileSize {
			break
		}
		num := this.partMaxSize
		if fileSize-off < num {
			num = fileSize - off
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(partNum int, body *io.SectionReader) {
			defer wg.Done()
			isUploadSuccess := false
			for i := 0; i < this.maxRetryNum; i++ {
				uploadPart, err := this.uploadPart(body, bucket, object, partNum+1, initUpload["UploadId"])
				if err != nil {
					continue
				}
				uploadPartList[partNum] = uploadPart
				isUploadSuccess = true
				break
			}
			if !isUploadSuccess {
				fmt.Printf("\nUpload Part Fail,PartNum:%d\n", partNum)
				os.Exit(2)
			}
			uploadPercent <- true
			<-queueMaxSize
		}(partNum, io.NewSectionReader(fd, int64(off), int64(num)))
		partNum++
	}
	wg.Wait()
	close(uploadPercent)
	<-uploadDone
	//上传完成
	completeUploadInfo := "<CompleteMultipartUpload>"
	for partNum, part := range uploadPartList {
		completeUploadInfo += fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", partNum+1, part["Etag"])
	}
	completeUploadInfo += "</CompleteMultipartUpload>"
	return this.completeUpload([]byte(completeUploadInfo), bucket, object, initUpload["UploadId"])
}

func (this *Client) CopyLargeFile(bucket, object, source string, options map[string]string) (map[string]string, error) {
	var wg sync.WaitGroup
	runtime.GOMAXPROCS(runtime.NumCPU())
	tmpSourceInfo := strings.Split(source, "/")
	sourceBucket := tmpSourceInfo[1]
	sourceObject := strings.Join(tmpSourceInfo[2:], "/")
	sourceHead, err := this.Head(sourceBucket, sourceObject)
	if err != nil {
		return nil, err
	}
	if sourceHead["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + sourceHead["StatusCode"])
	}

	if options["partsize"] != "" {
		partSize, err := strconv.Atoi(options["partsize"])
		if err == nil && partSize <= this.partMaxSize && partSize >= this.partMinSize {
			this.partMaxSize = partSize
		}
	}
	if options["thread_num"] != "" {
		threadNum, err := strconv.Atoi(options["thread_num"])
		if err == nil && threadNum <= this.threadMaxNum && threadNum >= this.threadMinNum {
			this.threadMaxNum = threadNum
		}
	}

	if object == "" {
		object = path.Base(sourceObject)
	}
	if strings.TrimRight(object, "/") == path.Dir(object) {
		object = strings.TrimRight(object, "/") + "/" + path.Base(sourceObject)
	}
	objectSize, err := strconv.Atoi(sourceHead["Content-Length"])
	if err != nil {
		return nil, err
	}

	var total = (objectSize + this.partMaxSize - 1) / this.partMaxSize
	if total < this.threadMaxNum {
		this.threadMaxNum = total
	}

	//初化化上传
	initUpload, err := this.initUpload(bucket, object, map[string]string{"disposition": options["disposition"]})
	if err != nil {
		return nil, err
	}
	var copyPartList = make([]map[string]string, total)
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

	partNum := 0
	//copy分片
	for {
		if partNum >= total {
			break
		}
		wg.Add(1)
		queueMaxSize <- true
		go func(partNum int) {
			defer wg.Done()
			//part范围,如：0-1023
			tmpStart := partNum * this.partMaxSize
			tmpEnd := (partNum+1)*this.partMaxSize - 1
			if tmpEnd > objectSize {
				tmpEnd = tmpStart + objectSize%this.partMaxSize - 1
			}
			partRange := fmt.Sprintf("bytes=%d-%d", tmpStart, tmpEnd)
			isCopySuccess := false
			for i := 0; i < this.maxRetryNum; i++ {
				copyPart, err := this.copyPart(partRange, bucket, object, source, partNum+1, initUpload["UploadId"])
				if err != nil {
					continue
				}
				copyPartList[partNum] = copyPart
				isCopySuccess = true
				break
			}
			if !isCopySuccess {
				fmt.Printf("\nUpload Part Fail,PartNum:%d\n", partNum)
				os.Exit(2)
			}
			copyPercent <- true
			<-queueMaxSize
		}(partNum)
		partNum++
	}
	wg.Wait()
	close(copyPercent)
	<-copyDone
	//copy完成
	completeCopyInfo := "<CompleteMultipartUpload>"
	for partNum, part := range copyPartList {
		completeCopyInfo += fmt.Sprintf("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>", partNum+1, part["Etag"])
	}
	completeCopyInfo += "</CompleteMultipartUpload>"
	return this.completeUpload([]byte(completeCopyInfo), bucket, object, initUpload["UploadId"])
}

func (this *Client) initUpload(bucket, object string, options map[string]string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s%s/%s?uploads", bucket, this.host, object)
	method := "POST"
	contentType := mime.TypeByExtension(path.Ext(object))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(this.dateTimeGMT)
	headers := map[string]string{
		"Content-Type": contentType,
		"Date":         date,
	}
	LF := "\n"
	headers["Authorization"] = this.sign(method+LF, headers, bucket, fmt.Sprintf("%s?uploads", object))
	if options["disposition"] != "" {
		headers["Content-Disposition"] = fmt.Sprintf(`attachment; filename="%s"`, options["disposition"])
	}
	res, err := this.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	var initUpload InitUploadResult
	if err := xml.Unmarshal([]byte(res["Body"]), &initUpload); err != nil {
		return nil, err
	}
	return map[string]string{
		"Bucket":   initUpload.Bucket,
		"Key":      initUpload.Key,
		"UploadId": initUpload.UploadId,
	}, nil
}

func (this *Client) uploadPart(body *io.SectionReader, bucket, object string, partNumber int, uploadId string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s%s/%s?partNumber=%d&uploadId=%s", bucket, this.host, object, partNumber, uploadId)
	method := "PUT"
	contentType := mime.TypeByExtension(path.Ext(object))
	contentLength := strconv.Itoa(int(body.Size()))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(this.dateTimeGMT)
	headers := map[string]string{
		"Content-Type": contentType,
		"Date":         date,
	}
	LF := "\n"
	object += fmt.Sprintf("?partNumber=%d&uploadId=%s", partNumber, uploadId)
	headers["Authorization"] = this.sign(method+LF, headers, bucket, object)
	headers["Content-Length"] = contentLength
	res, err := this.curl2Reader(addr, method, headers, body)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (this *Client) copyPart(partRange, bucket, object, source string, partNumber int, uploadId string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s%s/%s?partNumber=%d&uploadId=%s", bucket, this.host, object, partNumber, uploadId)
	method := "PUT"
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(this.dateTimeGMT)
	headers := map[string]string{
		"Date":                    date,
		"x-oss-copy-source":       source,
		"x-oss-copy-source-range": partRange,
	}
	LF := "\n"
	object += fmt.Sprintf("?partNumber=%d&uploadId=%s", partNumber, uploadId)
	headers["Authorization"] = this.sign(method+LF+LF, headers, bucket, object)
	res, err := this.curl(addr, method, headers, []byte(""))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (this *Client) completeUpload(body []byte, bucket, object, uploadId string) (map[string]string, error) {
	addr := fmt.Sprintf("http://%s%s/%s?uploadId=%s", bucket, this.host, object, uploadId)
	method := "POST"
	contentType := mime.TypeByExtension(path.Ext(object))
	contentLength := strconv.Itoa(len(body))
	contentMd5 := this.base64(this.md5Byte(body))
	date := time.Unix(time.Now().Unix()-8*3600, 0).Format(this.dateTimeGMT)
	headers := map[string]string{
		"Content-Md5":  contentMd5,
		"Content-Type": contentType,
		"Date":         date,
	}

	headers["Authorization"] = this.sign(method, headers, bucket, fmt.Sprintf("%s?uploadId=%s", object, uploadId))
	headers["Content-Length"] = contentLength
	res, err := this.curl(addr, method, headers, body)
	if err != nil {
		return nil, err
	}
	if res["StatusCode"] != "200" {
		return nil, errors.New("StatusCode:" + res["StatusCode"])
	}
	var completeUpload CompleteUploadResult
	if err := xml.Unmarshal([]byte(res["Body"]), &completeUpload); err != nil {
		return nil, err
	}
	return map[string]string{
		"Location": completeUpload.Location,
		"Bucket":   completeUpload.Bucket,
		"Key":      completeUpload.Key,
		"ETag":     completeUpload.ETag,
	}, nil
}
