package node

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"time"
)
var LEADER = "LEADER"
var PAYLOAD_HEARTBEAT_REQUEST uint8 = 4
var PAYLOAD_HEARTBEAT_RESPONSE uint8 = 5
type HeartBeatRequest struct {
	Node_hash string
}
type HeartBeatResponse struct{
	Status string
}
var ElectionReceivedFlag = false
var RequestingChangeLeader = false
func (msg *HeartBeatRequest) Package() (payload *PayLoad,err error) {
	payload = new(PayLoad)
	payload.PayLoadType = PAYLOAD_HEARTBEAT_REQUEST
	payload.Payload,err = json.Marshal(msg)
	return
}

func leaderConfirmedCallback(node* Node,key string,val string) {
	fmt.Println("Leader Confirmed , It is " + val)
	// Create Heart beat connection to the leader
	fmt.Println("Create Heart beat connection to the leader")
	// sleep for a while
	time.Sleep(time.Duration(5) * time.Second)

	fmt.Println("Start Sync Records on Nodes")
	go node.startSyncRecord(KEY_UNDONE_TASK)
}


func (msg *HeartBeatResponse) Package() (payload *PayLoad,err error) {
	payload = new(PayLoad)
	payload.PayLoadType = PAYLOAD_HEARTBEAT_RESPONSE
	payload.Payload,err = json.Marshal(msg)
	return
}

func (node* Node) isLeader() bool {
	if leaderHash,ok := node.decidedValues.Load(LEADER);ok{
		return node.node_hash == leaderHash
	}
	return false
}

func (node* Node) onLeaderUnresponse() {
	if RequestingChangeLeader {
		// already voting leader , ignore
	} else {
		RequestingChangeLeader = true
		node.proposer.prepareOp(OpProposal{Op:OP_TYPE_CHANGE_LEADER},"true","")
	}
}
func(node* Node) TestHeartBeatOnAddr(addr string) bool {
	conn,err := net.Dial("tcp", addr)
	if err != nil {
		return false
	}
	defer conn.Close()
	hr := new(HeartBeatRequest)
	hr.Node_hash = node.node_hash
	payload,err := hr.Package()
	err = node.writeToConn(conn,payload)
	if err == nil {
		pl,err := node.readPayloadFromConn(conn)
		if err != nil {
			fmt.Println("No response ")
			return false
		}
		msg,err := unpackagePayLoad(pl)
		if err != nil {
			fmt.Println("Bad Heartbeat Response")
			return false
		} else {
			fmt.Println("Receive PayLoad " + (interface{})(msg).(*HeartBeatResponse).Status)
		}
	} else {
		fmt.Println("Connection Close while writing")
		return false
	}
	return true
}
func(node* Node)isNodeAlive(node_hash string) bool {
	nodeAddr,ok:= node.peers.LoadPeer(node_hash)
	if !ok {
		return false
	}
	return node.TestHeartBeatOnAddr(nodeAddr.HeartbeatAddr)
}
func (node* Node) isLeaderAlive() bool {
	if node.isLeader() {
		return true
	}
	if leaderHash, ok := node.decidedValues.Load(LEADER); ok {
		return node.isNodeAlive(leaderHash.(string))
	}
	return true
}

func (node* Node) LeaderHealthCheck()  {
	missingTurns := 0
	memberMissingTurns := make(map[string]uint)
	fmt.Println("Heart beat service start")
	for {
		time.Sleep(time.Duration(5) * time.Second)
		if missingTurns >= 2 {
			println("Unresponse Leader Heart Beat ")
			go node.onLeaderUnresponse()
		}
		if !node.isLeaderAlive() {
			missingTurns ++
		} else {
			missingTurns = 0
		}
		if node.isLeader(){
			_,peerInfo := node.peers.LoadPeers()
			for _,v:=range(peerInfo) {
				if !node.isNodeAlive(v.Hash) {
					if _,ok := memberMissingTurns[v.Hash];ok {
						memberMissingTurns[v.Hash] += 1
						if memberMissingTurns[v.Hash] >= 2 {
							fmt.Println("Node " + v.Hash +" Dead, Start Kick off ")
							node.kickOff(v.Hash)
						}
					} else {
						memberMissingTurns[v.Hash] = 1
					}
				} else {
					memberMissingTurns[v.Hash] = 0
				}
			}
		}
	}
}
func electProposalTimeoutCallback(node* Node) {
	if _,ok := node.decidedValues.Load(LEADER); ok {
		// already got a leader
		return
	}
	ElectionReceivedFlag = false
	go node.beginElectLeader()
}


func (node* Node) beginElectLeader() error {
	// sleep for a random while to avoid all nodes come to elect
	fmt.Println("Start Electing Leader")
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	if _,ok := node.decidedValues.Load(LEADER); ok {
		// already got a leader
		return nil
	}
	if ElectionReceivedFlag {
		// someone is electing now give way to him
		return nil
	}
	if node.peers.Count() < 3 {
		// not enough peers wait for sometime
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		go node.beginElectLeader()
		return nil
	}
	node.proposer.prepareOp(OpProposal{Op:OP_TYPE_SET_CONST,Key:LEADER},node.node_hash,"")
	return nil
}

func (node* Node) pendingMsgRemover() error {
	for {
		time.Sleep(time.Duration(2) * time.Second)
		node.proposer.pendingMessageCheck()
	}
}

func consensusService(node* Node) {
	l, err := net.Listen("tcp", node.addr)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
	}
	defer l.Close()
	fmt.Println("Consensus Service Listen on " + node.addr)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
		}
		// Handle connections in a new goroutine.
		go node.handleCommunication(conn)
	}
}

func (node* Node) initializeConsensus() error {
	node.registBasicConsensusHandlers()
	node.RegistDataTransferHandlers()
	// establish node listener_comm_1 and node listener_heartbeat_2
	if node.peers.Count() < 3 {
		for {
			// not enough peers wait for sometime。。。
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			if node.peers.Count() >= 3 {
				break
			}
		}
	}
	go consensusService(node)
	go node.beginElectLeader()
	go node.LeaderHealthCheck()
	go node.proposer.pendingMessageRemoveService()
	go node.dataTransferService()
	return nil
}