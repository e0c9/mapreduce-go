package mapreduce

import (
	"fmt"
	"net"
	"sync"
)

type Master struct {
	sync.Mutex

	address         string
	registerChannel chan string
	doneChannel     chan bool
	workers         []string 	// protected by the mutex

	// Per-task information
	jobName  string
	files    []string
	nReduce  int
	shutdown chan struct{}
	l        net.Listener
	stats    []int
}

// 用于worker进行注册
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	debug("Register: worker %s\n", args.Worker)
	mr.workers = append(mr.workers, args.Worker)
	go func() {
		mr.registerChannel <- args.Worker	// 可以进行调用的worker
	}()
	return nil
}

func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.shutdown = make(chan struct{})
	mr.registerChannel = make(chan string)
	mr.doneChannel = make(chan bool)
	return
}

func Sequential(jobName string, files []string, nReduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) (mr *Master) {
	mr = newMaster("master")
	go mr.run(jobName, files, nReduce, func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, f := range mr.files {
				doMap(mr.jobName, i, f, mr.nReduce, mapF)
			}
		case reducePhase:
			for i := 0; i < mr.nReduce; i++ {
				doReduce(mr.jobName, i, len(mr.files), reduceF)
			}
		}
	}, func() {
		mr.stats = []int{len(files) + nReduce}

	})
	return
}

func Distributed(jobName string, files []string, nReduce int, master string) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()
	go mr.run(jobName, files, nReduce, mr.schedule, func() {
		mr.stats = mr.killWorkers()
		mr.stopRPCServer()
	})
	return
}

func (mr *Master) run(jobName string, files []string, nReduce int,
	schedule func(phase jobPhase),
	finish func(),
) {
	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nReduce

	debug("%s: Starting Map/Reduce task %s\n", mr.address, mr.jobName)
	schedule(mapPhase)
	schedule(reducePhase)
	finish()
	mr.merge()
	debug("%s: Map/Reduce task completed\n", mr.address)

	mr.doneChannel <- true
}

func (mr *Master) Wait() {
	<-mr.doneChannel
}

func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	ntasks := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		debug("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := call(w, "Worker.ShutDown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			ntasks = append(ntasks, reply.Ntasks)
		}
	}
	return ntasks
}
