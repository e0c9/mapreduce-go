package mapreduce

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
)

// 是否开启 debug
const debugEnabled = true

func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

func checkError(msg string, err error) {
	if err != nil {
		log.Fatal(msg, err)
	}
}

type jobPhase string

const (
	mapPhase    jobPhase = "Map"
	reducePhase          = "Reduce"
)

// 键值类型
type KeyValue struct {
	Key   string
	Value string
}

// Map 到 Reduce 的映射文件
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// 合并后的结果文件
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

func doMap(
	jobName string,
	mapTaskNumber int,
	inFile string,
	nReduce int,
	mapF func(file string, contents string) []KeyValue,
) {
	dat, err := ioutil.ReadFile(inFile)
	if err != nil {
		return
	}
	content := string(dat)
	kvs := mapF(inFile, content)
	interFiles := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		fName := reduceName(jobName, mapTaskNumber, i)
		interFiles[i], _ = os.Create(fName)
		defer interFiles[i].Close()
	}

	for _, kv := range kvs {
		iReduce := ihash(kv.Key) % uint32(nReduce)
		enc := json.NewEncoder(interFiles[iReduce])
		enc.Encode(&kv)
	}
}

func doReduce(
	jobName string,
	reduceTaskNumber int,
	nMap int,
	reduceF func(key string, values []string) string,
) {
	var kvs []KeyValue
	for m := 0; m < nMap; m++ {
		interFile := reduceName(jobName, m,reduceTaskNumber)
		f, err := os.Open(interFile)
		checkError("Open file: ", err)
		defer f.Close()
		br := bufio.NewReader(f)
		for {
			line, _, next := br.ReadLine()
			var tmp KeyValue
			json.Unmarshal(line, &tmp)
			if next == io.EOF {
				break
			}
			kvs = append(kvs, tmp)
		}
	}

	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	mergeName := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := os.Create(mergeName)
	checkError("Merge File: ", err)
	defer mergeFile.Close()
	enc := json.NewEncoder(mergeFile)
	if len(kvs) <= 0 {
		return
	}
	key := kvs[0].Key
	var values []string
	for _, kv := range kvs {
		if kv.Key != key {
			enc.Encode(KeyValue{key, reduceF(key, values)})
			key = kv.Key
			values = nil
		}
		values = append(values, kv.Value)
	}
	enc.Encode(KeyValue{key, reduceF(key, values)})
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (mr *Master) schedule(phase jobPhase) {

}
