package main

import (
	"fmt"
	"mapreduce"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

func mapF(document string, value string) (res []mapreduce.KeyValue) {
	words := strings.FieldsFunc(value, func(c rune) bool {
		return !unicode.IsLetter(c)
	})
	for _, word := range words {
		res = append(res, mapreduce.KeyValue{word, document})
	}
	return res
}

func reduceF(key string, values []string) string {
	set := make(map[string]bool)
	for _, value := range values {
		set[value] = true
	}
	var docs []string
	for k := range set {
		docs = append(docs, k)
	}
	sort.Strings(docs)
	return strconv.Itoa(len(docs)) + " " + strings.Join(docs, ",")
}

// run in 3 ways
// 1) Sequential (e.g., go run wc.go master sequential x1.txt ...)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt ...)
// 3) Worker (e.g., go run wc.go worker localhost:7777 x1.txt ...)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iidis", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}