package main

import (
	"fmt"
	"mapreduce"
	"os"
	"strconv"
	"strings"
	"unicode"
)

func mapF(document string, value string) (res []mapreduce.KeyValue) {
	words := strings.FieldsFunc(value, func(c rune) bool {
		return !unicode.IsLetter(c)
	})
	var kvs []mapreduce.KeyValue
	for _, word := range words {
		kvs = append(kvs, mapreduce.KeyValue{Key: word, Value: "1"})
	}
	return kvs
}

func reduceF(key string, values []string) string {
	res := 0
	for _, value := range values {
		 value, _ := strconv.Atoi(value)
		 res += value
	}
	return strconv.Itoa(res)
}


func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcseq", os.Args[3:], 3, mapF, reduceF)
			mr.CleanupFiles()
		} else {
			mr = mapreduce.Distributed("wcdis", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}

}
