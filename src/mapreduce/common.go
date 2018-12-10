package mapreduce

import (
	"fmt"
	"hash/fnv"
	"log"
	"strconv"
)

// 是否开启 debug
const debugEnabled = false

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

}

func doReduce(
	jobName string,
	reduceTaskNumber int,
	nMap int,
	reduceF func(key string, values []string) string,
) {

}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (mr *Master) schedule(phase jobPhase) {

}
