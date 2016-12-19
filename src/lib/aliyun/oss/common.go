package oss

import (
	"bytes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Client struct {
	host            string
	accessKeyId     string
	accessKeySecret string

	dateTimeGMT string
	dateTimeCST string

	partMaxSize  int
	partMinSize  int
	maxRetryNum  int
	threadMaxNum int
	threadMinNum int

	RecvBufferSize int
}

func New(host, accessKeyId, accessKeySecret string) *Client {
	return &Client{
		host:            "." + host,
		accessKeyId:     accessKeyId,
		accessKeySecret: accessKeySecret,

		dateTimeGMT: "Mon, 02 Jan 2006 15:04:05 GMT",
		dateTimeCST: "2006-01-02 15:04:05.00000 +0800 CST",

		partMaxSize:  100 * 1024 * 1024,
		partMinSize:  1 * 1024 * 1024,
		maxRetryNum:  3,
		threadMaxNum: 100,
		threadMinNum: 5,

		RecvBufferSize: 10 * 1024,
	}
}

func (this *Client) curl2Reader(addr string, method string, headers map[string]string, body io.Reader) (map[string]string, error) {
	client := http.Client{}
	req, _ := http.NewRequest(method, addr, body)
	for k, v := range headers {
		if req.Header.Get(k) != "" {
			req.Header.Set(k, v)
		} else {
			req.Header.Add(k, v)
		}
	}
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	str, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	result := map[string]string{
		"StatusCode": strconv.Itoa(res.StatusCode),
		"Body":       fmt.Sprintf("%s", str),
	}
	for k, v := range res.Header {
		result[k] = v[0]
	}
	return result, nil
}

func (this *Client) curl(addr string, method string, headers map[string]string, body []byte) (map[string]string, error) {
	return this.curl2Reader(addr, method, headers, bytes.NewReader(body))
}

func (this *Client) sign(method string, headers map[string]string, bucket, object string) string {
	var keyList []string
	LF := "\n"
	sign := method + LF
	for key, _ := range headers {
		keyList = append(keyList, key)
	}
	sort.Strings(keyList)
	for _, key := range keyList {
		if strings.Contains(key, "x-oss-") {
			sign += key + ":" + headers[key] + LF
		} else {
			sign += headers[key] + LF
		}
	}
	sign += "/" + bucket + "/" + object
	return "OSS " + this.accessKeyId + ":" + this.base64([]byte(this.hmac(sign, this.accessKeySecret)))
}

func (this *Client) hmac(sign string, key string) string {
	h := hmac.New(sha1.New, []byte(key))
	h.Write([]byte(sign))
	return string(h.Sum(nil))
}

func (this *Client) base64(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func (this *Client) md5Byte(data []byte) []byte {
	sum := md5.Sum(data)
	return sum[:]
}

func (this *Client) md5(data []byte) string {
	sum := md5.Sum(data)
	return fmt.Sprintf("%x", sum)
}

func (this *Client) walkdir(localdir string, suffix string) []string {
	var list = make([]string, 0)
	localdir = strings.TrimRight(localdir, "/") + "/"
	localdir = filepath.Dir(localdir)
	localdir = strings.Replace(localdir, "\\", "/", -1)
	filepath.Walk(localdir, func(fileName string, fi os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if fi == nil {
			return nil
		}
		if fi.IsDir() {
			return nil
		}
		fileName = strings.Replace(fileName, "\\", "/", -1)
		fileName = strings.Replace(fileName, localdir, "", 1)
		fileName = strings.TrimLeft(fileName, "/")

		allowed := true
		if suffix != "" {
			suffixList := strings.Split(suffix, ",")
			for _, tmpSuffix := range suffixList {
				if tmpSuffix != "" {
					allowed = false
					if strings.HasSuffix(strings.ToLower(fileName), tmpSuffix) {
						allowed = true
						break
					}
				}
			}
		}
		if allowed {
			list = append(list, fileName)
		}
		return nil
	})
	return list
}
