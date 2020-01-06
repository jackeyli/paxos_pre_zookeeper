package node

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"net"
	"strconv"
	"sync"
	"time"
)


type NodeConfig struct {
	Addr string
	HeartBeatAddr string
	StarterPeerAddr string
	DataTransferAddr string
	HttpServiceAddr string
	NodeId string
	DataSetPath string
}

type IntroductionMessage struct {
	Node_hash string
	Node_addr string
	Heartbeat_addr string
	DataTransfer_addr string
	Sender_hash string
	HttpServiceAddr string
}
var PAYLOAD_HELLO_REQUEST uint8 = 6


var LastTimeGotNewFriend = time.Now()
func (msg *IntroductionMessage) Package() (payload *PayLoad,err error) {
	payload = new(PayLoad)
	payload.PayLoadType = PAYLOAD_HELLO_REQUEST
	payload.Payload,err = json.Marshal(*msg)
	return
}

func(node* Node) onNewFriend(from_hash string,msg* IntroductionMessage) bool {
	if msg.Node_hash == node.node_hash {
		// myself
		return true
	}
	if _,ok := node.peers.LoadPeer(msg.Node_hash);!ok {
		info := new(PeerInfo)
		info.Hash = msg.Node_hash
		info.Addr = msg.Node_addr
		info.HeartbeatAddr = msg.Heartbeat_addr
		info.DataTransferAddr = msg.DataTransfer_addr
		info.HttpServiceAddr = msg.HttpServiceAddr
		fmt.Println("New Peer finded hash: " + msg.Node_hash + " addr:" + msg.Node_addr)
		node.peers.StorePeer(msg.Node_hash,info)
		msg.Sender_hash = node.node_hash
		hash,peers := node.peers.LoadPeers()
		for idx,v := range(peers) {
			// Intro new friend to others
			if hash[idx] != from_hash {
				node.IntrotoFriend(v.HeartbeatAddr,msg)
			}
		}
		LastTimeGotNewFriend = time.Now()
		// introduce itself
		msgout := new (IntroductionMessage)
		msgout.Heartbeat_addr = node.heartbeat_addr
		msgout.Node_addr = node.addr
		msgout.Node_hash = node.node_hash
		msgout.Sender_hash = node.node_hash
		msgout.DataTransfer_addr = node.data_addr
		msgout.HttpServiceAddr = node.http_addr
		fmt.Print("intro myself to " + msg.Heartbeat_addr)
		go node.IntrotoFriend(msg.Heartbeat_addr,msgout)
	}
	return true
}

func(node* Node) meetFriendsRoutine() {
	for {
		time.Sleep(time.Duration(200) * time.Millisecond)
		if time.Now().Sub(LastTimeGotNewFriend) > time.Duration(5) * time.Second {
			// got a while that no one say hello to me
			// start Initialize Consensus
			break
		}
	}
	fmt.Println("initializing Consensus channel " + node.addr)
	node.initializeConsensus()
}

func(node* Node) IntrotoFriend(addr string,msg* IntroductionMessage) error{
	payload,err := msg.Package()
	if err != nil {
		return err
	}
	return node.send(addr,payload)
}
func StartNode(node* Node,guideAddr string) {
	go healthCheckResponseService(node)
	if len(guideAddr) > 0 {
		msg := new(IntroductionMessage)
		msg.Heartbeat_addr = node.heartbeat_addr
		msg.Node_addr = node.addr
		msg.Node_hash = node.node_hash
		msg.DataTransfer_addr = node.data_addr
		msg.Sender_hash = node.node_hash
		msg.HttpServiceAddr = node.http_addr
		node.IntrotoFriend(guideAddr, msg)
	}
	fmt.Print("Node Start hash:" + node.node_hash + " addr:" + node.addr + " heartbeat addr:" + node.heartbeat_addr +
		"data transfer addr:" + node.data_addr)
	go node.meetFriendsRoutine()
}

func CreateNode(config *NodeConfig)(node* Node) {
	node = new(Node)
	pool := new(PeerPool)
	pool.Peers = make(map[PeerHash]*PeerInfo)
	pool.Lock = new(sync.RWMutex)
	node.peers = pool
	node.createProposer()
	node.createAcceptor()
	node.createWorker()
	node.createOutServer(config.HttpServiceAddr)
	node.decidedValues = new(sync.Map)
	node.epoch = 0
	node.heartbeat_addr = config.HeartBeatAddr
	node.addr = config.Addr
	node.data_addr = config.DataTransferAddr
	node.http_addr = config.HttpServiceAddr
	node.OutputChannels = new(sync.Map)
	if len(config.NodeId) == 0 {
		hasher := sha256.New()
		hasher.Write([]byte(time.Now().String()))
		hashbytes := hasher.Sum(nil)
		node.node_hash = hex.EncodeToString(hashbytes)
		node.node_id = strconv.FormatUint(uint64(binary.BigEndian.Uint16(hashbytes[:2])),10)
	} else {
		node.node_id = config.NodeId
		hasher := sha256.New()
		hasher.Write([]byte(node.node_id))
		hashbytes := hasher.Sum(nil)
		node.node_hash = hex.EncodeToString(hashbytes)
	}
	info := new(PeerInfo)
	info.Hash = node.node_hash
	info.Addr = node.addr
	info.DataTransferAddr = node.data_addr
	info.HeartbeatAddr = node.heartbeat_addr
	node.peers.StorePeer(node.node_hash,info)
	dataInst := new (DataSetInstance)
	db,err := leveldb.OpenFile(config.DataSetPath,nil)
	if err != nil {
		fmt.Println("Fail to Open DB on " + config.DataSetPath)
		return nil
	}
	dataInst.db = db
	node.dataInst = dataInst
	node.worker.Initialize()
	node.RegisteOpPrepareRecvHandlers = make (map[string]OpPrepareReceived)
	node.RegistedOpPromiseRecvHandlers = make (map[string]OpPromiseReceived)
	node.RegistedOpProposalRecvHandlers = make (map[string]OpProposalReceived)
	node.RegistedOpAcceptRecvHandlers = make (map[string]OpAcceptReceived)
	node.RegistedOpConsensusReachHandlers = make (map[string]OpConsensusReached)
	node.RegistedOpProposalTimeout = make (map[string]OpProposalTimeout)
	return
}

func(node* Node)readPayloadFromConn(conn net.Conn) (payload* PayLoad,err error) {
	contentLenBytes := make([]byte,8)
	len,err :=conn.Read(contentLenBytes)
	contentLen := binary.BigEndian.Uint64(contentLenBytes)
	if len < 8 || err != nil{
		return nil,errors.New("Not enough value")
	} else {
		contentBytes := make([]byte,contentLen)
		len,err = conn.Read(contentBytes)
		if (uint64)(len) < contentLen || err != nil{
			return nil,errors.New("Not enough data")
		}
		payload = new(PayLoad)
		payload.PayLoadType = contentBytes[0]
		payload.Payload = contentBytes[1:]
		return payload,nil
	}
}

func (node* Node) writeToConn(conn net.Conn,payload * PayLoad) error {
	buff := make([]byte,0)
	buff = append(buff,payload.PayLoadType)
	buff = append(buff,payload.Payload...)
	buffLen := make([]byte,8)
	binary.BigEndian.PutUint64(buffLen,(uint64)(len(buff)))
	buff = append(buffLen,buff...)
	_,err := conn.Write(buff)
	return err
}

func(node* Node)handlePayLoad(conn net.Conn,payLoad* PayLoad) error {
	msg,err := unpackagePayLoad(payLoad)
	if err != nil {
		return err
	}
	if payLoad.PayLoadType == PAYLOAD_HEARTBEAT_REQUEST{
		fmt.Println("Receive Health Check ")
		hres := new(HeartBeatResponse)
		hres.Status = "GOOD"
		p,err := hres.Package()
		if err != nil {
			return err
		}
		node.writeToConn(conn,p)
	}
	if payLoad.PayLoadType == PAYLOAD_HELLO_REQUEST {
		node.onNewFriend(interface{}(msg).(*IntroductionMessage).Sender_hash,interface{}(msg).(*IntroductionMessage))
	}
	return nil
}

func (node* Node) healthCheckHandler(conn net.Conn) error {
	defer conn.Close()
	payload,err := node.readPayloadFromConn(conn)
	if err != nil {
		return err
	}
	node.handlePayLoad(conn,payload)
	return nil
}

func healthCheckResponseService(node* Node) {
	l, err := net.Listen("tcp", node.heartbeat_addr)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
	}
	fmt.Println("Health Check Side Service Listen on " + node.heartbeat_addr)
	defer l.Close()
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
		}
		// Handle connections in a new goroutine.
		go node.healthCheckHandler(conn)
	}
}

