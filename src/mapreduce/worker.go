package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
)

// Worker 持有一个服务等待 DoTask 和 Shutdown 的 rpc 请求
type Worker struct {
	sync.Mutex
	name   string
	Map    func(string, string) []KeyValue
	Reduce func(string, []string) string
	nRPC   int	// protected by mutex
	nTasks int	// protected by mutex
	l      net.Listener
}

// 当一个新的任务被调度给该 worker，DoTask 被 master 调用
func (wk *Worker) DoTask(arg *DoTaskArgs, _ *struct{}) error {
	debug("%s: given %v task #%d on file %s (nios: %d)\n",
		wk.name, arg.Phase, arg.TaskNumber, arg.File, arg.NumOtherPhase)

	switch arg.Phase {
	case mapPhase:
		doMap(arg.JobName, arg.TaskNumber, arg.File, arg.NumOtherPhase, wk.Map)
	case reducePhase:
		doReduce(arg.JobName, arg.TaskNumber, arg.NumOtherPhase, wk.Reduce)
	}

	debug("%s: %v task #%d done\n", wk.name, arg.Phase, arg.TaskNumber)
	return nil
}

func (wk *Worker) ShutDown(_ *struct{}, res *ShutdownReply) error {
	debug("Shutdown %s\n", wk.name)
	wk.Lock()
	defer wk.Unlock()
	res.Ntasks = wk.nTasks
	wk.nRPC = 1
	wk.nTasks--
	return nil
}

func (wk *Worker) register(master string) {
	args := new(RegisterArgs)
	args.Worker = wk.name
	ok := call(master, "Master.Register", args, new(struct{}))
	if ok == false {
		fmt.Printf("Register: RPC %s register error\n", master)
	}
}

// RunWorker 和master建立一个连接，注册它的地址，并等待任务被调度
func RunWorker(MasterAddress string, me string,
	MapFunc func(string, string) []KeyValue,
	ReduceFunc func(string, []string) string,
	nRPC int) {
	debug("RunWorker %s\n", me)
	wk := new(Worker)
	wk.name = me
	wk.Map = MapFunc
	wk.Reduce = ReduceFunc
	wk.nRPC = nRPC
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	os.Remove(me)
	l, e := net.Listen("unix", me)
	if e != nil {
		log.Fatal("RunWorker: worker ", me, " error ", e)
	}
	wk.l = l
	wk.register(MasterAddress)

	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break
		}
		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
			wk.Lock()
			wk.nTasks++
			wk.Unlock()
		} else {
			break
		}
	}
	wk.l.Close()
	debug("RunWorker %s exit\n", me)

}
