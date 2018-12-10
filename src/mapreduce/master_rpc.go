package mapreduce

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
)

// 关闭master rpc 服务器的rpc方法。
func (mr *Master) Shutdown(_, _ *struct{}) error {
	debug("Shutdown: registration server\n")
	close(mr.shutdown)
	mr.l.Close()
	return nil
}

func (mr *Master) startRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(mr)
	os.Remove(mr.address) // only need for unix
	l, e := net.Listen("unix", mr.address)
	if e != nil {
		log.Fatal("RegistrationServer", mr.address, " error ", e)
	}
	mr.l = l
	go func() {
	loop:
		for {
			select {
			case <-mr.shutdown:
				break loop
			default:
			}
			conn, err := mr.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				debug("RegistrationServer: accept error: %v\n", err)
				break
			}
		}
		debug("RegistrationServer: done\n")
	}()

}

func (mr *Master) stopRPCServer() {
	var reply ShutdownReply
	ok := call(mr.address, "Master.Shutdown", new(struct{}), &reply)
	if ok == false {
		fmt.Printf("Cleanup: RPC %s error\n", mr.address)
	}
	debug("cleanupRegistration: done\n")
}
