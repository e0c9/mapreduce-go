package mapreduce

import (
	"fmt"
	"net/rpc"
)

type DoTaskArgs struct {
	JobName       string
	File          string	// input file, only used in map tasks
	Phase         jobPhase
	TaskNumber    int		// this task's index in the current phase
	NumOtherPhase int		// mappers need this to compute the number of output bins, and reducers needs this to know how many input files to collect.
}

type ShutdownReply struct {
	Ntasks int	// worker 已经处理过的任务数量
}

type RegisterArgs struct {
	Worker string
}

// 使用该方法来发送所有的rpc请求
func call(srv, rpcname string,
	args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		return false
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
