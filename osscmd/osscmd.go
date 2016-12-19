package main

import (
	"../osscmd/osscmd"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var host = flag.String("host", "", "specify host")
var access_id = flag.String("id", "", "specify access id")
var access_key = flag.String("key", "", "specify access key")

var headers = flag.String("headers", "", "HTTP headers for put object, input format SHOULE like --headers=\"key1:value1,key2:value2\"")
var force = flag.String("force", "FALSE", "if true, ignore interactive command, never prompt")
var replace = flag.String("replace", "FALSE", "replace the localfile or object if it is true")
var suffix = flag.String("suffix", "", "upload file suffix filter")

var marker = flag.String("marker", "", "get bucket(list objects) parameter")
var delimiter = flag.String("delimiter", "", "get bucket(list objects) parameter")
var maxkeys = flag.String("maxkeys", "", "get bucket(list objects) parameter")

var partsize = flag.Int("partsize", 10, "part file upload size")
var thread_num = flag.Int("thread_num", 10, "object group upload thread num")

var VERSION = "0.0.1"

const HELP = `
    ls(list)        oss://bucket/[prefix] --marker=xxx --delimiter=xxx --maxkeys=xxx
    uploadfromdir   localdir oss://bucket/[prefix] --replace=false --suffix=".mp3,.mp4"
    copy            oss://source_bucket/source_object oss://target_bucket/target_object --headers="key1:value1,key2:value2"
    copybucket      oss://source_bucket/[prefix] oss://target_bucket/[prefix] --replace=false --headers="key1:value1,key2:value2"
    copylargefile   oss://source_bucket/source_object oss://target_bucket/source_object --headers="key1:value1,key2:value2"

    get             oss://bucket/object localfile
    cat             oss://bucket/object
    meta            oss://bucket/object
    rm(delete,del)  oss://bucket/object

    listallobject   oss://bucket/[prefix]
    deleteallobject oss://bucket/[prefix] --force=false

    put             localfile oss://bucket/object --headers="key1:value1,key2:value2"
    upload          localfile oss://bucket/object --headers="key1:value1,key2:value2"
    uploadlargefile localfile oss://bucket/object --headers="key1:value1,key2:value2"
    config --host=oss.aliyuncs.com --id=accessid --key=accesskey
`

func main() {
	flag.Parse()
	args := flag.Args()

	if len(args) < 1 {
		fmt.Println("osscmd: version " + VERSION)
		fmt.Println(HELP)
		os.Exit(0)
	}

	//解析参数
	args = parse_args(args)

	//配置文件处理
	if args[0] == "config" {
		config := map[string]string{
			"host":      *host,
			"accessid":  *access_id,
			"accesskey": *access_key,
		}
		osscmd.Config(config)
	}

	//开始时间
	begin := time.Now()

	//初始化osscmd
	osscmd.New(*host, *access_id, *access_key)

	//options参数
	options := map[string]string{
		"headers":    *headers,
		"force":      *force,
		"replace":    *replace,
		"suffix":     *suffix,
		"marker":     *marker,
		"delimiter":  *delimiter,
		"maxkeys":    *maxkeys,
		"partsize":   strconv.Itoa(*partsize * 1024 * 1024),
		"thread_num": strconv.Itoa(*thread_num),
	}

	switch args[0] {
	case "uploadlargefile":
		osscmd.UploadLargeFile(args, options)
	case "upload":
		fallthrough
	case "put":
		osscmd.Upload(args, options)
	case "copylargefile":
		fallthrough
	case "copy":
		osscmd.CopyBigObject(args, options)
	case "copybucket":
		osscmd.CopyBucket(args, options)
	case "uploadfromdir":
		osscmd.UploadFromDir(args, options)
	case "deleteallobject":
		osscmd.DeleteAllObject(args, options)
	case "listallobject":
		osscmd.ListAllObject(args, options)
	case "list":
		fallthrough
	case "ls":
		osscmd.ListObject(args, options)
	case "delete":
		fallthrough
	case "del":
		fallthrough
	case "rm":
		osscmd.Delete(args)
	case "cat":
		osscmd.Cat(args)
	case "get":
		osscmd.Get(args)
	case "meta":
		osscmd.Head(args)
	case "help":
		fmt.Println(HELP)
		os.Exit(0)
	default:
		fmt.Println("unsupported command : " + args[0])
		fmt.Println("use --help for more information")
		os.Exit(0)
	}
	fmt.Printf("%.3f(s) elapsed\n", time.Now().Sub(begin).Seconds())
}

func parse_args(args []string) []string {
	var res = make([]string, 0)
	for k, v := range args {
		if !strings.Contains(v, "--") {
			res = append(res, args[k])
			continue
		}
		tmp := strings.Split(v, "=")
		name, value := strings.TrimLeft(tmp[0], "--"), tmp[1]
		if flag.Lookup(name) != nil {
			flag.Set(name, value)
		}
	}
	return res
}
