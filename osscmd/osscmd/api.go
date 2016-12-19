package osscmd

import (
	"bufio"
	"fmt"
	"github.com/Unknwon/goconfig"
	"io/ioutil"
	"lib/aliyun/oss"
	"os"
	"strconv"
	"strings"
	"time"
)

var client *oss.Client
var dateTimeFormat = "2006-01-02 15:04:05"

var ossConfigPath = "/root/.osscredentials"
var ossConfigSection = "OSSCredentials"

func New(host, accessKeyId, accessKeySecret string) {
	if accessKeyId == "" || accessKeySecret == "" {
		conf, err := goconfig.LoadConfigFile(ossConfigPath)
		if err != nil {
			fmt.Println("Your configuration is saved into /root/.osscredentials .")
			os.Exit(0)
		}
		section, err := conf.GetSection(ossConfigSection)
		if err != nil {
			fmt.Println("can't get accessid/accesskey, setup use : config --id=accessid --key=accesskey")
			os.Exit(0)
		}
		if host == "" {
			host = section["host"]
		}
		if accessKeyId == "" {
			accessKeyId = section["accessid"]
		}
		if accessKeySecret == "" {
			accessKeySecret = section["accesskey"]
		}
	}
	if host == "" {
		host = "oss-cn-hangzhou.aliyuncs.com"
	}
	if accessKeyId == "" || accessKeySecret == "" {
		fmt.Println("can't get accessid/accesskey, setup use : config --id=accessid --key=accesskey")
		os.Exit(0)
	}
	client = oss.New(host, accessKeyId, accessKeySecret)
}

func Upload(args []string, options map[string]string) {
	if len(args) < 3 {
		fmt.Println("upload miss parameters")
		os.Exit(0)
	}
	srcFile := args[1]
	bucket, object := parse_bucket_object(args[2])
	headers := parse_headers(options["headers"])

	tmp, err := client.UploadFile(srcFile, bucket, object, map[string]string{"disposition": headers["disposition"]})
	if err != nil {
		fmt.Println("upload::", err)
		os.Exit(2)
	}
	res := "\nObject URL is: " + tmp["Location"] + "\n"
	res += "Object abstract path is: oss://" + tmp["Bucket"] + "/" + tmp["Key"] + "\n"
	res += "ETag is " + tmp["ETag"]
	fmt.Println(res)
}

func UploadLargeFile(args []string, options map[string]string) {
	if len(args) < 3 {
		fmt.Println("uploadlarge miss parameters")
		os.Exit(0)
	}
	srcFile := args[1]
	bucket, object := parse_bucket_object(args[2])
	headers := parse_headers(options["headers"])

	tmp, err := client.UploadLargeFile(srcFile, bucket, object, map[string]string{
		"disposition": headers["disposition"],
		"partsize":    options["partsize"],
		"thread_num":  options["thread_num"],
	})
	if err != nil {
		fmt.Println("uploadlarge::", err)
		os.Exit(2)
	}
	res := "\nObject URL is: " + tmp["Location"] + "\n"
	res += "Object abstract path is: oss://" + tmp["Bucket"] + "/" + tmp["Key"] + "\n"
	res += "ETag is " + tmp["ETag"]
	fmt.Println(res)
}

func CopyBucket(args []string, options map[string]string) {
	if len(args) < 3 {
		fmt.Println("copybucket miss parameters")
		os.Exit(0)
	}
	sourceBucket, sourceObject := parse_bucket_object(args[1])
	sourceFullObject := "/" + sourceBucket + "/" + sourceObject
	bucket, object := parse_bucket_object(args[2])

	tmp, err := client.CopyAllObject(bucket, object, sourceFullObject, map[string]string{
		"replace":    options["replace"],
		"thread_num": options["thread_num"],
	})
	if err != nil {
		fmt.Println("copybucket::", err)
		os.Exit(2)
	}
	from := strings.Replace(args[1], "oss:/", "", 1)
	to := strings.Replace(args[2], "oss:/", "", 1)
	finish := strconv.Itoa(tmp["finish"])
	skip := strconv.Itoa(tmp["skip"])
	fail := strconv.Itoa(tmp["total"] - tmp["finish"] - tmp["skip"])
	res := "\nTotal being copied objects num: " + strconv.Itoa(tmp["total"]) + ", from " + from + " to " + to + "\n"
	res += "OK num:" + finish + ", SKIP num:" + skip + ", FAIL num:" + fail + "\n"
	fmt.Println(res)
}

func CopyBigObject(args []string, options map[string]string) {
	if len(args) < 3 {
		fmt.Println("copybigobject miss parameters")
		os.Exit(0)
	}
	sourceBucket, sourceObject := parse_bucket_object(args[1])
	sourceFullObject := "/" + sourceBucket + "/" + sourceObject
	bucket, object := parse_bucket_object(args[2])
	headers := parse_headers(options["headers"])

	tmp, err := client.CopyLargeFile(bucket, object, sourceFullObject, map[string]string{
		"disposition": headers["disposition"],
		"replace":     options["replace"],
		"partsize":    options["partsize"],
		"thread_num":  options["thread_num"],
	})
	if err != nil {
		fmt.Println("copybigobject::", err)
		os.Exit(2)
	}
	res := "\nObject URL is: " + tmp["Location"] + "\n"
	res += "Object abstract path is: oss://" + tmp["Bucket"] + "/" + tmp["Key"] + "\n"
	res += "ETag is " + tmp["ETag"]
	fmt.Println(res)
}

func UploadFromDir(args []string, options map[string]string) {
	if len(args) < 3 {
		fmt.Println("uploadfromdir miss parameters")
		os.Exit(0)
	}
	srcFile := args[1]
	bucket, object := parse_bucket_object(args[2])

	tmp, err := client.UploadFromDir(srcFile, bucket, object, map[string]string{
		"replace":    options["replace"],
		"suffix":     options["suffix"],
		"thread_num": options["thread_num"],
	})
	if err != nil {
		fmt.Println("uploadfromdir::", err)
		os.Exit(2)
	}
	finish := strconv.Itoa(tmp["finish"])
	skip := strconv.Itoa(tmp["skip"])
	fail := strconv.Itoa(tmp["total"] - tmp["finish"] - tmp["skip"])
	res := "\nTotal being uploaded localfiles num: " + strconv.Itoa(tmp["total"]) + "\n"
	res += "OK num:" + finish + ", SKIP num:" + skip + ", FAIL num:" + fail + "\n"
	fmt.Println(res)
}

func Delete(args []string) {
	if len(args) < 2 {
		fmt.Println("delete miss parameters")
		os.Exit(0)
	}
	bucket, object := parse_bucket_object(args[1])
	tmp, err := client.Delete(bucket, object)
	if err != nil {
		fmt.Println("delete::", err)
		os.Exit(2)
	}
	if tmp["StatusCode"] != "204" {
		fmt.Println("delete::", tmp["Body"])
		os.Exit(2)
	}
}

func DeleteAllObject(args []string, options map[string]string) {
	if len(args) < 2 {
		fmt.Println("deleteallobject miss parameters")
		os.Exit(0)
	}
	if options["force"] != "true" {
		fmt.Println("DELETE all objects? y/N, default is N: ")
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		if strings.ToUpper(strings.Trim(input, "\n")) != "Y" {
			fmt.Println("quit.")
			os.Exit(0)
		}
	}
	bucket, object := parse_bucket_object(args[1])
	tmp, err := client.DeleteAllObject(bucket, object, map[string]string{
		"thread_num": options["thread_num"],
	})
	if err != nil {
		fmt.Println("deleteallobject::", err)
		os.Exit(2)
	}
	finish := strconv.Itoa(tmp["finish"])
	fail := strconv.Itoa(tmp["total"] - tmp["finish"])
	res := "\nTotal being deleted objects num: " + strconv.Itoa(tmp["total"]) + "\n"
	res += "OK num:" + finish + ", FAIL num:" + fail + "\n"
	fmt.Println(res)
}

func ListAllObject(args []string, options map[string]string) {
	begin := time.Now()
	if len(args) < 2 {
		fmt.Println("list miss parameters")
		os.Exit(0)
	}
	totalNum := 0
	totalSize := 0
	bucket, prefix := parse_bucket_object(args[1])
	marker := ""
	maxkeys := 1000
LIST:
	list, err := client.ListObject(bucket, map[string]string{
		"marker":   marker,
		"prefix":   prefix,
		"max-keys": strconv.Itoa(maxkeys),
	})
	if err != nil {
		fmt.Println("list::", err)
		os.Exit(2)
	}
	totalNum += len(list.Contents)
	for _, v := range list.Contents {
		marker = v.Key
		lastModified, _ := time.Parse("2006-01-02T15:04:05.000Z", v.LastModified)
		tmpDatetime := time.Unix(lastModified.Unix(), 0).Format(dateTimeFormat)
		tmpSize, _ := strconv.Atoi(v.Size)
		totalSize += tmpSize
		content := tmpDatetime + " " + size_format(tmpSize) + " " + "oss://" + bucket + "/" + v.Key
		fmt.Println(content)
	}
	if list.IsTruncated == "true" {
		goto LIST
	}
	end := fmt.Sprintf("object list number is: %d\n", totalNum)
	end += fmt.Sprintf("totalsize is: real:%d, format:%s\n", totalSize, size_format(totalSize))
	end += fmt.Sprintf("request times is: %.2f", time.Now().Sub(begin).Seconds())
	fmt.Println(end)
}

func ListObject(args []string, options map[string]string) {
	if len(args) < 2 {
		fmt.Println("list miss parameters")
		os.Exit(0)
	}
	total := 0
	bucket, prefix := parse_bucket_object(args[1])
	marker := ""
	delimiter := ""
	maxkeys := 1000
	if options["marker"] != "" {
		marker = options["marker"]
	}
	if options["delimiter"] != "" {
		delimiter = options["delimiter"]
	}
	if options["maxkeys"] != "" {
		maxkeys, _ = strconv.Atoi(options["maxkeys"])
	}
	content := "prefix list is: \n"
	content += "object list is:"
	fmt.Println(content)
LIST:
	list, err := client.ListObject(bucket, map[string]string{
		"marker":    marker,
		"prefix":    prefix,
		"delimiter": delimiter,
		"max-keys":  strconv.Itoa(maxkeys),
	})
	if err != nil {
		fmt.Println("list::", err)
		os.Exit(2)
	}
	total += len(list.Contents)
	for _, v := range list.Contents {
		marker = v.Key
		lastModified, _ := time.Parse("2006-01-02T15:04:05.000Z", v.LastModified)
		tmpDatetime := time.Unix(lastModified.Unix(), 0).Format(dateTimeFormat)
		tmpSize, _ := strconv.Atoi(v.Size)
		content := tmpDatetime + " " + size_format(tmpSize) + " " + v.StorageClass + " " + "oss://" + bucket + "/" + v.Key
		fmt.Println(content)
	}
	if maxkeys != total && list.IsTruncated == "true" {
		goto LIST
	}
	end := "\nprefix list number is: 0 \n"
	end += "object list number is: " + strconv.Itoa(total)
	fmt.Println(end)
}

func Cat(args []string) {
	if len(args) < 2 {
		fmt.Println("cat miss parameters")
		os.Exit(0)
	}
	bucket, object := parse_bucket_object(args[1])
	objectHead, err := client.Head(bucket, object)
	if err != nil {
		fmt.Printf("get Head Error:\n%s", err)
		os.Exit(2)
	}
	if objectHead["StatusCode"] != "200" {
		fmt.Printf("Error Body:\n%s", objectHead["Body"])
		fmt.Printf("Error Status:\n%s\nget Failed!\n", objectHead["StatusCode"])
		os.Exit(0)
	}
	objectSize, err := strconv.Atoi(objectHead["Content-Length"])
	if err != nil {
		fmt.Printf("get strconv Error:\n%s", err)
		os.Exit(2)
	}
	var total = (objectSize + client.RecvBufferSize - 1) / client.RecvBufferSize
	for partNum := 0; partNum < total; partNum++ {
		//part范围,如：0-1023
		tmpStart := partNum * client.RecvBufferSize
		tmpEnd := (partNum+1)*client.RecvBufferSize - 1
		if tmpEnd > objectSize {
			tmpEnd = tmpStart + objectSize%client.RecvBufferSize - 1
		}
		partRange := fmt.Sprintf("bytes=%d-%d", tmpStart, tmpEnd)
		tmp, err := client.Cat(bucket, object, partRange)
		if err != nil {
			fmt.Println("cat::", err)
			os.Exit(2)
		}
		fmt.Println(tmp["Body"])
	}
}

func Get(args []string) {
	if len(args) < 3 {
		fmt.Println("get miss parameters")
		os.Exit(0)
	}
	bucket, object := parse_bucket_object(args[1])
	localfile := args[2]
	tmp, err := client.Get(bucket, object, localfile)
	if err != nil {
		fmt.Println("get::", err)
		os.Exit(2)
	}
	res := "  The object " + tmp["object"] + " is downloaded to " + tmp["localfile"] + ", please check."
	fmt.Println(res)
}

func Head(args []string) {
	if len(args) < 2 {
		fmt.Println("meta miss parameters")
		os.Exit(0)
	}
	bucket, object := parse_bucket_object(args[1])
	tmp, err := client.Head(bucket, object)
	if err != nil {
		fmt.Println("meta::", err)
		os.Exit(2)
	}
	res := fmt.Sprintf("%-20s: %s\n", "objectname", object)
	for k, v := range tmp {
		if v != "" {
			res += fmt.Sprintf("%-20s: %s\n", strings.ToLower(k), v)
		}
	}
	fmt.Println(res)
}

func Config(config map[string]string) {
	if config["accessid"] == "" || config["accesskey"] == "" {
		fmt.Println("config miss parameters, use --id=[accessid] --key=[accesskey] to specify id/key pair")
		os.Exit(0)
	}
	_, err := os.Stat(ossConfigPath)
	if err != nil && os.IsNotExist(err) {
		err := ioutil.WriteFile(ossConfigPath, []byte(""), 0644)
		if err != nil {
			fmt.Println("config::write::", err)
			os.Exit(2)
		}
	}
	conf, err := goconfig.LoadConfigFile(ossConfigPath)
	if err != nil {
		fmt.Println("config::load::", err)
		os.Exit(2)
	}
	for k, v := range config {
		if v != "" {
			_ = conf.SetValue(ossConfigSection, k, v)
		}
	}
	err = goconfig.SaveConfigFile(conf, ossConfigPath)
	if err != nil {
		fmt.Println("config::save::", err)
		os.Exit(2)
	}
	fmt.Printf("Your configuration is saved into %s .\n", ossConfigPath)
	os.Exit(0)
}

func parse_bucket_object(path string) (string, string) {
	path = strings.Replace(path, "oss://", "", 1)
	tmp := strings.Split(path, "/")
	bucket := tmp[0]
	object := strings.Replace(path, bucket, "", 1)

	if object != "" && !strings.Contains(object, "/") {
		fmt.Println("object name SHOULD NOT begin with /")
		os.Exit(0)
	}
	return bucket, strings.TrimLeft(object, "/")
}

func size_format(size int) string {
	unit := "B"
	b := float64(size)
	if size > (1 << 30) {
		unit = "GB"
		b = float64(size) / (1 << 30)
	} else if size > (1 << 20) {
		unit = "MB"
		b = float64(size) / (1 << 20)
	} else if size > (1 << 10) {
		unit = "KB"
		b = float64(size) / (1 << 10)
	}
	return fmt.Sprintf("%.2f%s", b, unit)
}

func parse_headers(headers string) map[string]string {
	ossHeader := []string{"disposition"}
	res := map[string]string{}
	if headers != "" {
		rows := strings.Split(headers, ",")
		for _, o := range ossHeader {
			for _, r := range rows {
				if !strings.Contains(r, o) {
					continue
				}
				tmp := strings.Split(r, o)
				res[o] = strings.TrimLeft(tmp[1], ":")
			}
		}
	}
	return res
}
