package node

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

type OutServer struct {
	node* Node
	serverAddr string
	netClient * http.Client
}

func (node* Node) createOutServer(serviceAddr string) {
	outServer := new(OutServer)
	outServer.serverAddr = serviceAddr
	outServer.netClient = &http.Client{
		Timeout:time.Second * 30,
	}
	node.outServer = outServer
	outServer.node = node
}

func (node* Node) registOutputChannel(taskKey string,output chan[]byte){
	node.OutputChannels.Store(taskKey,output)
}

func(server* OutServer) asyncWordCount(w http.ResponseWriter,r*http.Request) {
	resourceInput,err := ioutil.ReadAll(r.Body)
	fmt.Println("Receive Request From Outside")
	if err == nil {
		if server.node.isLeader() {
			var recvChan = make(chan []byte)
			res := server.node.receiveNewTask(resourceInput, recvChan,true)
			// result
			w.Write(res)
			return
		} else {
			if leader_hash,ok := server.node.decidedValues.Load(LEADER);!ok {
				// leader not decided yet
				w.Write([]byte("SERVICE_ERROR"))
				return
			} else {
				peer,ok := server.node.peers.LoadPeer(leader_hash.(string))
				if !ok {
					w.Write([]byte("SERVICE_ERROR"))
					return
				} else {

					reader := bytes.NewReader(resourceInput)
					resp,err := server.netClient.Post("http://" + peer.HttpServiceAddr + "/asyncWordCount","text/plain",reader)
					if err != nil {
						fmt.Println("SERVICE_ERROR")
						fmt.Println(err)
						w.Write([]byte("SERVICE_ERROR"))
						return
					}
					defer resp.Body.Close()
					res,err := ioutil.ReadAll(resp.Body)
					if err != nil {
						fmt.Println("SERVICE_ERROR")
						fmt.Println(err)
						w.Write([]byte("SERVICE_ERROR"))
						return
					}
					w.Write(res)
				}
			}
		}
	}
}
func(server* OutServer) transformNodeResBytesToClientByte(nodeBytes[] byte) ([]byte,error){
	res := new(MainTaskResult)
	err := json.Unmarshal(nodeBytes,res)
	if err != nil {
		return nil,err
	}
	result,err:= json.Marshal(res.Entries)
	if err != nil {
		return nil,err
	}
	return result,nil
}
func(server* OutServer) outputTskResult(w http.ResponseWriter,r*http.Request) {
 	tskIds := strings.Split(r.URL.RawPath,"/")
	tskId := tskIds[len(tskIds) - 1]
	result:= server.node.serviceTaskResult(tskId)
	if result == nil {
		w.WriteHeader(400)
		w.Write([]byte("SERVICE_ERROR"))
		return
	} else {
		res,err := json.Marshal(result.Entries)
		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte("SERVICE_ERROR"))
			return
		}
		w.Write(res)
	}
}

func (server* OutServer)inputResource(w http.ResponseWriter, r *http.Request) {
	resourceInput,err := ioutil.ReadAll(r.Body)
	fmt.Println("Receive Request From Outside")

	if err == nil {
		if server.node.isLeader() {
			var recvChan = make(chan []byte)
			res := server.node.receiveNewTask(resourceInput, recvChan,false)
			w.Write(res)
			return
		} else {
			if leader_hash,ok := server.node.decidedValues.Load(LEADER);!ok {
				// leader not decided yet
				w.Write([]byte("SERVICE_ERROR"))
				return
			} else {
				peer,ok := server.node.peers.LoadPeer(leader_hash.(string))
				if !ok {
					w.Write([]byte("SERVICE_ERROR"))
					return
				} else {

					reader := bytes.NewReader(resourceInput)
					resp,err := server.netClient.Post("http://" + peer.HttpServiceAddr + "/mapReduce","text/plain",reader)
					if err != nil {
						fmt.Println("SERVICE_ERROR")
						fmt.Println(err)
						w.Write([]byte("SERVICE_ERROR"))
						return
					}
					defer resp.Body.Close()
					res,err := ioutil.ReadAll(resp.Body)
					if err != nil {
						fmt.Println("SERVICE_ERROR")
						fmt.Println(err)
						w.Write([]byte("SERVICE_ERROR"))
						return
					}
					w.Write(res)
				}
			}
		}
	}
}

func(server *OutServer) service(){
	fmt.Println("Out Service Start")
	http.HandleFunc("/mapReduce",server.inputResource)
	http.ListenAndServe(":" + strings.Split(server.serverAddr,":")[1],nil)
}


