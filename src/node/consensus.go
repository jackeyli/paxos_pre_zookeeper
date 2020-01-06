package node


import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)
type PeerAddr = string
type PeerHash = string
var PAYLOAD_PREPARE uint8 = 0
var PAYLOAD_PROPOSAL uint8 = 1
var PAYLOAD_PROMISE uint8= 2
var PAYLOAD_ACCEPT uint8= 3
var MAX_PENDING_TURN = 4
var ON_PREPARE_RECEIVED = "onPrepareReceived"
var PROPOSAL_ACCEPTED = "PROPOSAL_ACCEPTED"
var OP_PREFIX = "OP"
var OP_TYPE_SET_CONST = "SET_CONST"
var OP_TYPE_CHANGE_LEADER = "CHANGE_LEADER"
var OP_TYPE_KICK_OFF = "KICK_OFF"
type (
	EpocOutDateHandler = func(node* Node,receivedPromise* Promise) error
	AcceptedReceive = func(node* Node,accept * Accept)error
	OpPrepareReceived = func(node* Node,key OpProposal,prepare* PrepareMsg) error
	OpPromiseReceived = func(node* Node,key OpProposal,receivedPromise* Promise) error
	OpProposalReceived = func(node* Node,key OpProposal,proposal* Proposal) error
	OpAcceptReceived = func(node* Node,key OpProposal,accept * Accept)error
	OpConsensusReached = func(node* Node,key OpProposal,val string) error
	OpProposalTimeout = func(node* Node,key OpProposal,val string) error
)
type PeerInfo struct {
	Hash string
	Addr string
	DataTransferAddr string
	HeartbeatAddr string
	HttpServiceAddr string
}

type PeerPool struct {
	Peers map[PeerHash]*PeerInfo
	Lock *sync.RWMutex
}

type Node struct{
	epoch uint32
	node_hash string
	node_id string // node_hash[:4]
	heartbeat_addr string
	addr string
	data_addr string
	http_addr string
	proposer *Proposer
	acceptor *Acceptor
	outServer* OutServer
	worker* Worker
	dataInst *DataSetInstance
	majorThrehold uint32
	decidedValues *sync.Map  // the decided Value now store the actual Var Name-Value Pair not the key send in paxos
	peers *PeerPool
	epocOutDateHandler EpocOutDateHandler
	acceptReceiveHandler AcceptedReceive
	RegisteOpPrepareRecvHandlers map[string]OpPrepareReceived
	RegistedOpPromiseRecvHandlers map[string]OpPromiseReceived
	RegistedOpProposalRecvHandlers map[string]OpProposalReceived
	RegistedOpAcceptRecvHandlers map[string]OpAcceptReceived
	RegistedOpConsensusReachHandlers map[string]OpConsensusReached
	RegistedOpProposalTimeout map[string]OpProposalTimeout
	OutputChannels *sync.Map
}
type Message interface {
	Package() (*PayLoad,error)
}
type OpProposal struct {
	Op string
	Key string
	Version string //Optional
}
type Proposal struct {
	Id string
	Epoch uint32
	Val string
	Key string
	ProposerHash string
}
type Proposer struct {
	c_round sync.Map  `map[string]uint64`
	node *Node
	lock *int32
	pendingPrepares sync.Map
	//map[string]map[string]*PendingMessage
	pendingAccepts sync.Map
	//map[string]map[string]*PendingMessage
}

type PayLoad struct {
	PayLoadType uint8
	Payload []byte
}
type Max_ACC_Message struct {
	max_accept_id string
	max_accept_val string
}
type PendingMessage struct {
	message interface{}
	pendingTurns uint8
	passNodes uint32
	value string
	max_acc_info *Max_ACC_Message
	isLocked int32
	//acks map[string]*Message
}
type PrepareMsg struct {
	ProposerHash string
	Epoch uint32
	Id string
	Key string
}

type Acceptor struct {
	max_id *sync.Map  `map[string] *big.Float`
	accepted_val *sync.Map `map[string]string`
	accepted_id *sync.Map `map[string]string`
	lock *int32
	node *Node
}

type Promise struct {
	Approved bool
	Epoch uint32
	Key string
	ConsensusReached bool
	Proposal_id string
	Accepted_id string
	Accepted_val string
}

type Accept struct {
	AcceptorHash string
	Epoch uint32
	Key string
	Val string
	Proposal_id string
}

func OpProposalKeyToString(key OpProposal) string {
	return OP_PREFIX + "@" + key.Op + "@" + key.Key + "@" + key.Version
}

func isOpProposalKey(key string) bool {
	return strings.Split(key,"@")[0] == OP_PREFIX
}

func StringToOpProposal(str string) OpProposal {
	res := OpProposal{}
	strs := strings.Split(str,"@")
	res.Op = strs[1]
	res.Key = strs[2]
	if len(strs) > 3 {
		res.Version = strs[3]
	}
	return res
}

func (msg *PrepareMsg) Package() (payload *PayLoad,err error) {
	payload = new(PayLoad)
	payload.PayLoadType = PAYLOAD_PREPARE
	payload.Payload,err = json.Marshal(msg)
	return
}

func (msg *Proposal) Package() (payload *PayLoad,err error) {
	payload = new(PayLoad)
	payload.PayLoadType = PAYLOAD_PROPOSAL
	payload.Payload,err = json.Marshal(msg)
	return
}

func (msg *Accept) Package() (payload *PayLoad,err error) {
	payload = new(PayLoad)
	payload.PayLoadType = PAYLOAD_ACCEPT
	payload.Payload,err = json.Marshal(msg)
	return
}

func(msg *Promise) Package() (payload *PayLoad,err error) {
	payload = new(PayLoad)
	payload.PayLoadType = PAYLOAD_PROMISE
	payload.Payload,err = json.Marshal(msg)
	return
}

func(peers* PeerPool) LoadPeers() (key[] string,info []*PeerInfo){
	key = make([]string,0)
	info = make([]*PeerInfo,0)
	peers.Lock.RLock()
	defer peers.Lock.RUnlock()
	for k,v := range(peers.Peers) {
		key = append(key,k)
		info = append(info,v)
	}
	return
}

func (peers* PeerPool)LoadPeer(key string) (info *PeerInfo,ok bool) {
	peers.Lock.RLock()
	defer peers.Lock.RUnlock()
	info,ok = peers.Peers[key]
	return
}

func (peers* PeerPool)Range(f func(key string,peerInfo* PeerInfo)){
	peers.Lock.RLock()
	defer peers.Lock.RUnlock()
	for v,k := range(peers.Peers) {
		f(v,k)
	}
	return
}

func (peers* PeerPool)StorePeer(key string,peerinfo * PeerInfo) {
	peers.Lock.Lock()
	defer peers.Lock.Unlock()
	peers.Peers[key] = peerinfo
	return
}

func(peers* PeerPool) DeletePeer(key string) {
	peers.Lock.Lock()
	defer peers.Lock.Unlock()
	delete(peers.Peers,key)
	return
}

func (peers* PeerPool)Count() (int) {
	peers.Lock.RLock()
	defer peers.Lock.RUnlock()
	return len(peers.Peers)
}

func(node* Node)createProposer() {
	node.proposer = new (Proposer)
	node.proposer.node = node
	node.proposer.pendingAccepts = sync.Map{}
	node.proposer.pendingPrepares = sync.Map{}
	node.proposer.c_round = sync.Map{}
	node.proposer.lock = new(int32)
	*node.proposer.lock = 0
}

func(node* Node)createAcceptor(){
	node.acceptor = new (Acceptor)
	node.acceptor.node = node
	node.acceptor.accepted_id = new(sync.Map)
	node.acceptor.accepted_val = new(sync.Map)
	node.acceptor.max_id = new(sync.Map)
	node.acceptor.lock = new(int32)
	*node.acceptor.lock = 0
}

func (node* Node) onEpocOutDated(receivedPromise * Promise) {
	if node.epocOutDateHandler != nil {
		node.epocOutDateHandler(node,receivedPromise)
	}
}

func (node* Node) onAcceptReceived(accept* Accept) {
	if node.acceptReceiveHandler != nil {
		node.acceptReceiveHandler(node,accept)
	}
}
func unpackagePayLoad(payload* PayLoad) (content interface{}, err error) {
	if payload.PayLoadType == PAYLOAD_PREPARE {
		msg := new(PrepareMsg)
		err = json.Unmarshal(payload.Payload,msg)
		if err != nil {
			return nil,err
		}
		return msg,nil
	}
	if payload.PayLoadType == PAYLOAD_PROMISE {
		msg := new(Promise)
		err = json.Unmarshal(payload.Payload,msg)
		if err != nil {
			return nil,err
		}
		return msg,nil
	}
	if payload.PayLoadType == PAYLOAD_ACCEPT {
		msg := new(Accept)
		err = json.Unmarshal(payload.Payload,msg)
		if err != nil {
			return nil,err
		}
		return msg,nil
	}
	if payload.PayLoadType == PAYLOAD_PROPOSAL {
		msg := new(Proposal)
		err = json.Unmarshal(payload.Payload,msg)
		if err != nil {
			return nil,err
		}
		return msg,nil
	}
	if payload.PayLoadType == PAYLOAD_HEARTBEAT_REQUEST {
		msg := new(HeartBeatRequest)
		err = json.Unmarshal(payload.Payload,msg)
		if err != nil {
			return nil,err
		}
		return msg,nil
	}
	if payload.PayLoadType == PAYLOAD_HEARTBEAT_RESPONSE {
		msg := new(HeartBeatResponse)
		err = json.Unmarshal(payload.Payload,msg)
		if err != nil {
			return nil,err
		}
		return msg,nil
	}
	if payload.PayLoadType == PAYLOAD_HELLO_REQUEST {
		msg := new(IntroductionMessage)
		err = json.Unmarshal(payload.Payload,msg)
		if err != nil {
			return nil,err
		}
		return msg,nil
	}
	if payload.PayLoadType == PAYLOAD_DATATRANSFER_REQUEST {
		msg := new(DataTransferRequest)
		err = json.Unmarshal(payload.Payload,msg)
		if err != nil {
			return nil,err
		}
		return msg,nil
	}
	if payload.PayLoadType == PAYLOAD_RAW {
		msg := new(RawMessage)
		msg.content = payload.Payload
		return msg,nil
	}
	return nil,errors.New("Unknown payload type")
}

func (node* Node) handleCommunication(conn net.Conn) error {
	defer conn.Close()
	payLoad,err := node.readPayloadFromConn(conn)
	if err != nil {
		return err
	}
	msg,err := unpackagePayLoad(payLoad)
	if err != nil {
		return err
	}
	if payLoad.PayLoadType == PAYLOAD_PREPARE{
		node.acceptor.prepareReceived(msg.(*PrepareMsg))
	}
	if payLoad.PayLoadType == PAYLOAD_PROPOSAL {
		node.acceptor.proposalReceived(msg.(*Proposal))
	}
	if payLoad.PayLoadType == PAYLOAD_PROMISE {
		node.proposer.promiseReceived(msg.(*Promise))
	}
	if payLoad.PayLoadType == PAYLOAD_ACCEPT {
		node.proposer.acceptReceived(msg.(*Accept))
	}
	return nil
}


func (node* Node) send(address string,payload* PayLoad) error{
	conn,err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()
	node.writeToConn(conn,payload)
	return nil
}

func (node* Node) sendMsg(address string,msg Message) error{
	payload,err := msg.Package()
	if err != nil {
		return err
	}
	conn,err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer conn.Close()
	node.writeToConn(conn,payload)
	return nil
}

func (node* Node) broadCast(message Message) error {
	payload,err := message.Package()
	if err != nil {
		return err
	}
	_,peers := node.peers.LoadPeers()
	for _,v := range(peers) {
		if v.Hash == node.node_hash {
			// don't broad cast to myself
			continue
		}
		node.send(v.Addr,payload)
	}
	return nil
}


func NewPendingMessage(msg Message,value... string) (result *PendingMessage) {
	result = new(PendingMessage)
	result.message = interface{}(msg)
	result.passNodes = 0
	result.pendingTurns = 0
	result.isLocked = 0
	if len(value) > 0 {
		result.value = value[0]
	}
	//result.acks = make(map[string]*Message)
	return
}

func getRoundFromId(id string) (uint64,error){
	int_part := strings.Split(id,".")[0]
	return strconv.ParseUint(int_part,10,64)
}

// 明早再写，我真菜
func (proposer* Proposer) prepare(key string,val string,node_hash string) error{
	var rc uint64
	if v,ok := proposer.node.acceptor.max_id.Load(key);!ok{
		rc = uint64(0)
	} else {
		flt := v.(*big.Float)
		int_part,_ := flt.Uint64()
		rc = int_part
	}
	int_id := rc + 1
	_id := strconv.FormatUint(uint64(int_id),10) + "." + proposer.node.node_id
	msg := new(PrepareMsg)
	msg.Id = _id
	msg.Epoch = proposer.node.epoch
	msg.ProposerHash = proposer.node.node_hash
	msg.Key = key
	pendingMsgs,exist :=proposer.pendingPrepares.Load(key)
	if !exist {
		pendingMsgs = new(sync.Map)
		proposer.pendingPrepares.Store(key,pendingMsgs)
	}
	pendingMsgs.(*sync.Map).Store(_id,NewPendingMessage(msg,val))
	fmt.Println("Prepare Key: " + msg.Key + " ID: " + msg.Id)
	if len(node_hash) > 0 {
		// prepare to specific node
		if node_hash == proposer.node.node_hash {
			proposer.node.acceptor.prepareReceived(msg)
		} else {
			node_Addr,ok := proposer.node.peers.LoadPeer(node_hash)
			if !ok {
				fmt.Println("Prepare Error No Such Spec Node, Key: " + msg.Key + " ID: " + msg.Id)
			}
			proposer.node.sendMsg(node_Addr.Addr,msg)
		}
	} else {
		proposer.node.acceptor.prepareReceived(msg)
		err := proposer.node.broadCast(msg)
		if err != nil {
			return err
		}
	}
	return nil
}


func (proposer* Proposer) prepareOp(key OpProposal,val string,node_hash string) error {
	// I will not check this Op the upper layer take this reponsibility
	return proposer.prepare(OpProposalKeyToString(key),val,node_hash)
}


func (proposer* Proposer) promiseReceived(promise * Promise) error {
	if !promise.Approved {
		if promise.Epoch > proposer.node.epoch {
			theMap,ok := proposer.pendingPrepares.Load(promise.Key)
			if ok {
				theMap.(*sync.Map).Delete(promise.Proposal_id)
			}
			proposer.node.onEpocOutDated(promise)
		}
	} else {

		if isOpProposalKey(promise.Key) {
			opKey := StringToOpProposal(promise.Key)
			if handler,ok := proposer.node.RegistedOpPromiseRecvHandlers[opKey.Op];ok {
				return handler(proposer.node,opKey,promise)
			} else {
				return errors.New("Unknown Op")
			}
		}
	}
	return nil
}

func (proposer* Proposer) propose(id string,key string,val string) error{
	//send Propose request
	msg := new(Proposal)
	msg.Id = id
	msg.Epoch = proposer.node.epoch
	msg.ProposerHash = proposer.node.node_hash
	msg.Key = key
	msg.Val = val
	// the broad Cast will not broad cast to myself, however when calculate the accept,the proposer will be considered as accepted
	// so first propose to Myself
	proposer.node.acceptor.proposalReceived(msg)
	return proposer.node.broadCast(msg)
}

func (proposer* Proposer)updatePendingAcc(accept* Accept) *PendingMessage{
	GetLock(proposer.lock)
	defer ReleaseLock(proposer.lock)
	pendingAccMap,exist := proposer.pendingAccepts.Load(accept.Key)
	if !exist {
		pendingAccMap = new(sync.Map)
		proposer.pendingAccepts.Store(accept.Key,pendingAccMap)
	}
	// can be two proposer so still need to check
	pendingAcc,exist := pendingAccMap.(*sync.Map).Load(accept.Proposal_id)
	if !exist {
		pendingAcc = NewPendingMessage(accept,accept.Val)
		pendingAccMap.(*sync.Map).Store(accept.Proposal_id,pendingAcc)
	}
	pendingAcc.(*PendingMessage).passNodes ++
	pendingAcc.(*PendingMessage).pendingTurns = 0
	return pendingAcc.(*PendingMessage)
}

func (proposer* Proposer) acceptReceived(accept *Accept) error {
	if accept.Epoch < proposer.node.epoch {
		// old message
		fmt.Println("Old Epoc Accept")
		return nil
	}
	// this is good for each operation
	pendingAcc := proposer.updatePendingAcc(accept)
	fmt.Println("Accept Received On Key: " + accept.Key + " Value: " + accept.Val + " ID :" + accept.Proposal_id +
		"passed Nodes:" + strconv.FormatUint(uint64(pendingAcc.passNodes),10) + " hash: " + accept.AcceptorHash)
	if isOpProposalKey(accept.Key) {
		opKey := StringToOpProposal(accept.Key)
		if handler,ok := proposer.node.RegistedOpAcceptRecvHandlers[opKey.Op];ok {
			handler(proposer.node,opKey,accept)
		}
	}
	// the proposal node will also receive accept message so no need to add 1
	if int(pendingAcc.passNodes) > proposer.node.peers.Count()/2 {
		// consensus reached
		// for every epoch the value of key cannot be changed once it is decided so delete the pendingAccepts and pending Prepares of that key
		success := atomic.CompareAndSwapInt32(&(pendingAcc.isLocked),0,1)
		if success {
			proposer.pendingAccepts.Delete(accept.Key)
			proposer.pendingPrepares.Delete(accept.Key)
			if isOpProposalKey(accept.Key) {
				opKey := StringToOpProposal(accept.Key)
				if handler, ok := proposer.node.RegistedOpConsensusReachHandlers[opKey.Op]; ok {
					handler(proposer.node, opKey, accept.Val)
				}
			}
		}
	}
	return nil
}



func (acceptor* Acceptor) prepareReceived(prepare *PrepareMsg) error{
	proposalAddr,ok := acceptor.node.peers.LoadPeer(prepare.ProposerHash)
	if !ok && prepare.ProposerHash != acceptor.node.node_hash{
		return errors.New("Unknown Host " + prepare.ProposerHash)
	}
	if prepare.Epoch < acceptor.node.epoch{
		// reply with new epoch
		promise := new (Promise)
		promise.Approved = false
		promise.ConsensusReached = false
		promise.Epoch = acceptor.node.epoch
		payload,err := promise.Package()
		if err != nil {
			return err
		}
		acceptor.node.send(proposalAddr.Addr,payload)
	}
	// if is Op proposal using registed handler
	if isOpProposalKey(prepare.Key) {
		opKey := StringToOpProposal(prepare.Key)
		if handler,ok := acceptor.node.RegisteOpPrepareRecvHandlers[opKey.Op];ok {
			return handler(acceptor.node,opKey,	)
		} else {
			return errors.New("Unknown Operation")
		}
	}
	return nil
}

func (acceptor* Acceptor) proposalReceived(proposal* Proposal) error {
	id := big.NewFloat(0)
	id,_ = id.SetString(proposal.Id)
	fmt.Println("Proposal Received on Key: " + proposal.Key + " Value :" + proposal.Val +
		" ID: " + proposal.Id)
	if max_id,ok := acceptor.max_id.Load(proposal.Key);ok && id.Cmp(max_id.(*big.Float)) == 0 {
		if isOpProposalKey(proposal.Key) {
			opKey := StringToOpProposal(proposal.Key)
			if handler,ok := acceptor.node.RegistedOpProposalRecvHandlers[opKey.Op];ok {
				return handler(acceptor.node,opKey,proposal)
			} else {
				return errors.New("Unknown Operation")
			}
		}
	} else if(!ok) {
		// ne
	}
	return nil
}

func(proposer* Proposer) pendingMessageRemoveService(){
	fmt.Println("Pending Message Removing Service on")
	for {
		time.Sleep(time.Duration(5) * time.Second)
		proposer.pendingMessageCheck()
	}
}

func GetLock(lock* int32) {
	tryCount:= 0
	for;!atomic.CompareAndSwapInt32(lock,0,1);{
		tryCount ++
		if tryCount > 20 {
			time.Sleep(time.Duration(50) * time.Millisecond)
			tryCount = 0
		}
	}
}

func ReleaseLock(lock* int32) {
	atomic.CompareAndSwapInt32(lock,1,0)
}


func SetConstPrepareReceived(node* Node,key OpProposal,prepare* PrepareMsg) (error) {
	acceptor := node.acceptor
	proposalAddr,ok := node.peers.LoadPeer(prepare.ProposerHash)
	if !ok && prepare.ProposerHash != node.node_hash{
		// Unknown host
		return nil
	}
	// receive the already reach concensus value return that value
	if value,ok := node.decidedValues.Load(key.Key);ok {
		// already reach concensus on this value, send the value to that node
		promise := new (Promise)
		promise.Approved = true
		promise.ConsensusReached = true
		promise.Accepted_val = value.(string)
		promise.Key = prepare.Key
		payload,err := promise.Package()
		if err != nil {
			return err
		}
		node.send(proposalAddr.Addr,payload)
		return nil
	}
	id := big.NewFloat(0)
	id, _ = id.SetString(prepare.Id)
	if max_id,ok := acceptor.max_id.Load(prepare.Key);!ok || id.Cmp(max_id.(*big.Float)) > 0{
		if key.Key == LEADER {
			ElectionReceivedFlag = true
		}
		acceptor.max_id.Store(prepare.Key,id)
		acc_val,exists := acceptor.accepted_val.Load(prepare.Key)
		promise := new(Promise)
		promise.Approved = true
		promise.ConsensusReached = false
		if exists {
			// already accept a val
			promise.Key = prepare.Key
			promise.Proposal_id = prepare.Id
			acc_id,_ := acceptor.accepted_id.Load(prepare.Key)
			promise.Accepted_id = acc_id.(string)
			promise.Accepted_val = acc_val.(string)

		} else {
			promise.Key = prepare.Key
			promise.Proposal_id = prepare.Id
		}
		payload,err := promise.Package()
		if err != nil {
			return err
		}
		fmt.Println("Promise Const Value Key: " + promise.Key + " Value: " + promise.Accepted_val + " ID: " + promise.Proposal_id)

		acceptor.node.send(proposalAddr.Addr,payload)
	}
	return nil
}

func ChangeLeaderPrepareReceived(node* Node,key OpProposal,prepare* PrepareMsg) (error) {
	acceptor := node.acceptor
	fmt.Println("Prepare Change Leader Received On Key: " + prepare.Key + " ID: " + prepare.Id + " proposer: " + prepare.ProposerHash)
	proposalAddr,ok := node.peers.LoadPeer(prepare.ProposerHash)
	if !ok{
		// Unknown host
		return nil
	}
	id := big.NewFloat(0)
	id, _ = id.SetString(prepare.Id)
	if max_id,ok := acceptor.max_id.Load(prepare.Key);!ok || id.Cmp(max_id.(*big.Float)) > 0{
		acceptor.max_id.Store(prepare.Key,id)
		promise := new(Promise)
		promise.Approved = true
		promise.ConsensusReached = false
		promise.Key = prepare.Key
		promise.Proposal_id = prepare.Id
		payload,err := promise.Package()
		if err != nil {
			return err
		}
		fmt.Println("Promise Change Leader Key: " + promise.Key + " Value: " + promise.Accepted_val + " ID: " + promise.Proposal_id)
		acceptor.node.send(proposalAddr.Addr,payload)
	}
	return nil
}

func SetConstPromiseReceived(node* Node,key OpProposal,promise* Promise) (error) {
	// This for Set var Op will be Merged
	proposer := node.proposer
	if promise.ConsensusReached {
		// already reach consensus
		fmt.Println("Set Const Consensus Reached: " + promise.Key + " Value: " + promise.Accepted_val + " ID: " + promise.Proposal_id)
		proposer.node.decidedValues.Store(key.Key,promise.Accepted_val)
		proposer.pendingPrepares.Delete(key.Key)
		//
		if handler,ok := node.RegistedOpConsensusReachHandlers[OP_TYPE_SET_CONST];ok{
			handler(node,key,promise.Accepted_val)
		}
		return nil
	}
	fmt.Println("Set Const Promise Received: " + promise.Key + " Value: " + promise.Accepted_val + " ID: " + promise.Proposal_id)
	theMap,exist := proposer.pendingPrepares.Load(promise.Key)
	if exist {
		p,ok := theMap.(*sync.Map).Load(promise.Proposal_id)
		if !ok {
			// too late already timeout
			return nil
		}
		pendingPrepare := p.(*PendingMessage)
		pendingPrepare.pendingTurns = 0
		if len(promise.Accepted_id) > 0 {
			max_acc := new(Max_ACC_Message)
			max_acc.max_accept_id = promise.Accepted_id
			max_acc.max_accept_val = promise.Accepted_val
			if pendingPrepare.max_acc_info == nil {
				pendingPrepare.max_acc_info = max_acc
			} else {
				id := big.NewFloat(0)
				id, _ = id.SetString(promise.Accepted_id)
				id_old := big.NewFloat(0)
				id_old, _ = id_old.SetString(pendingPrepare.max_acc_info.max_accept_id)
				if id.Cmp(id_old) > 0 {
					pendingPrepare.max_acc_info = max_acc
				}
			}
		}
		pendingPrepare.passNodes ++
		if int(pendingPrepare.passNodes) > proposer.node.peers.Count()/2 {
			// pending Prepare should be cleared
			// 真他妈烦
			max_acc := pendingPrepare.max_acc_info
			success := atomic.CompareAndSwapInt32(&(pendingPrepare.isLocked),0,1)
			if success {
				if max_acc != nil {
					// send Proposal accepted proposal value
					fmt.Println("Propose Const Value Key : " + promise.Key + " Value :" + max_acc.max_accept_val + " ID: " + promise.Proposal_id)
					// first accept myself
					proposer.propose(promise.Proposal_id, promise.Key, max_acc.max_accept_val)
				} else {
					fmt.Println("Propose Const Value Key : " + promise.Key + " Value :" + pendingPrepare.value + " ID: " + promise.Proposal_id)
					proposer.propose(promise.Proposal_id, promise.Key, pendingPrepare.value)
				}
			}
		}
	} else {
		// already Timeout,wait for next round
	}
	return nil
}

func ChangeLeaderPromiseReceived(node* Node,key OpProposal,promise* Promise) (error) {
	proposer := node.proposer
	theMap,exist := proposer.pendingPrepares.Load(promise.Key)
	fmt.Println("Change Leader Promise Received: " + promise.Key + " Value: " + promise.Accepted_val + " ID: " + promise.Proposal_id)
	if exist {
		p,_ := theMap.(*sync.Map).Load(promise.Proposal_id)
		if p == nil {
			// already timeout
			return nil
		}
		pendingPrepare := p.(*PendingMessage)
		pendingPrepare.pendingTurns = 0
		pendingPrepare.passNodes ++
		// include the proposal node itself so add 1
		if int(pendingPrepare.passNodes) > proposer.node.peers.Count()/2 {
			success := atomic.CompareAndSwapInt32(&(pendingPrepare.isLocked),0,1)
			if success {
				fmt.Println("Propose Const Value Key: " + promise.Key + " Value: " + promise.Accepted_val + " ID: " + promise.Proposal_id)
				proposer.propose(promise.Proposal_id, promise.Key, "")
			}
		}
	}
	return nil
}


// no need to add this
func SetConstConsensusReached (node* Node,key OpProposal,val string)(error) {
	proposer := node.proposer
	proposer.node.decidedValues.Store(key.Key,val)
	// If is Leader
	if key.Key == LEADER {
		leaderConfirmedCallback(node,key.Key,val)
	}
	return nil
}


func ChangeLeaderConsensusReached (node* Node,key OpProposal,val string)(error) {
	fmt.Println("consensus reached change leader")
	if _,ok := node.decidedValues.Load(LEADER);!ok{
		// leader not decided yet
		return nil
	}
	node.newEpoc()
	ElectionReceivedFlag = false
	// wait for next round of change leader
	RequestingChangeLeader = false
	go node.beginElectLeader()
	return nil
}

func SetConstProposalTimeout (node* Node,key OpProposal,val string) (error) {
	if key.Key == LEADER {
		// some leader vote proposal dead
		electProposalTimeoutCallback(node)
	}
	return nil
}

func ChangeLeaderProposalTimeout (node* Node,key OpProposal,val string) (error) {
	RequestingChangeLeader = false
	return nil
}

func updateAcceptedVal(acceptor* Acceptor,key string,val string,id string) bool{
	GetLock(acceptor.lock)
	defer ReleaseLock(acceptor.lock)
	if _,exists := acceptor.accepted_id.Load(key);!exists {
		acceptor.accepted_val.Store(key,val)
		acceptor.accepted_id.Store(key,id)
		return true
	} else {
		// the current round number should be larget than the old one
		old_id,_ :=acceptor.accepted_id.Load(key)
		old_rc,_:= getRoundFromId(old_id.(string))
		new_rc,_:= getRoundFromId(id)
		if old_rc < new_rc {
			acceptor.accepted_val.Store(key,val)
			acceptor.accepted_id.Store(key,id)
			return true
		}
	}
	return false
}

func SetConstProposalReceived (node* Node,key OpProposal,proposal* Proposal) (error) {
	acceptor := node.acceptor
	success := updateAcceptedVal(acceptor,proposal.Key,proposal.Val,proposal.Id)
	if(!success){
		// already accept a message the value must be the same
		old_val,_ := acceptor.accepted_val.Load(key)
		if old_val != proposal.Val {
			return nil
		}
	}
	acc := new(Accept)
	acc.Key = proposal.Key
	acc.Val = proposal.Val
	acc.Proposal_id = proposal.Id
	acc.Epoch = acceptor.node.epoch
	acc.AcceptorHash = acceptor.node.node_hash
	// receive By Myself
	acceptor.node.proposer.acceptReceived(acc)
	fmt.Println("Broadcast Accept Const Value Key: " + acc.Key + " Value: " + acc.Val + " ID: " + acc.Proposal_id)
	acceptor.node.broadCast(acc)

	return nil
}

func SetConstAcceptReceived(node* Node,key OpProposal,accept* Accept)(error){
	fmt.Println("Set Const Accept Received Key: " + accept.Key + " Value: " + accept.Val +
		" ID: " + accept.Proposal_id)
	return nil
}

func ChangeLeaderProposalReceived (node* Node,key OpProposal,proposal* Proposal) (error) {
	// no need to Check Value just see if the leader is alive
	if !node.isLeaderAlive() {
		acc := new(Accept)
		acc.Key = proposal.Key
		acc.Val = proposal.Val
		acc.Proposal_id = proposal.Id
		acc.Epoch = node.epoch
		acc.AcceptorHash = node.node_hash
		// receive By Myself
		node.proposer.acceptReceived(acc)
		fmt.Println("Broadcast Accept Change Leader Key: " + acc.Key + " Value: " + acc.Val + " ID: " + acc.Proposal_id)
		node.broadCast(acc)
	}
	return nil
}



func (node* Node) newEpoc() {
	node.epoch ++
	node.decidedValues = new(sync.Map)
	node.acceptor.accepted_val = new(sync.Map)
	node.acceptor.accepted_id = new(sync.Map)
}

func epocOutDateHandler (node* Node,promise* Promise) error{
	node.epoch = promise.Epoch
	return nil
}

func (proposer * Proposer) pendingMessageCheck() {
	timeOutProposals := make([]map[string]string,0)
	proposer.pendingPrepares.Range(func(key interface{},val interface{}) bool{
		pendingMsgs := val.(*sync.Map)
		pendingMsgs.Range(func(key interface{},val interface{}) bool {
			pendingMessage := val.(*PendingMessage)
			if int(pendingMessage.pendingTurns) > MAX_PENDING_TURN {
				prepare := interface{}(pendingMessage.message).(*PrepareMsg)
				params := make(map[string]string)
				params["id"] = prepare.Id
				params["key"] = prepare.Key
				params["value"] = pendingMessage.value
				timeOutProposals = append(timeOutProposals,params)
				pendingMsgs.Delete(key)
			} else {
				pendingMessage.pendingTurns ++
			}
			return true
		})
		return true
	})
	proposer.pendingAccepts.Range(func(key interface{},val interface{}) bool{
		pendingAccMap := val.(*sync.Map)
		pendingAccMap.Range(func(key interface{},val interface{}) bool {
			pendingAcc := val.(*PendingMessage)
			if int(pendingAcc.pendingTurns) > MAX_PENDING_TURN {
				accept := interface{}(pendingAcc.message).(*Accept)
				params := make(map[string]string)
				params["id"] = accept.Key
				params["key"] = accept.Val
				params["value"] = accept.Val
				timeOutProposals = append(timeOutProposals,params)
				proposer.pendingAccepts.Delete(key)
			} else {
				pendingAcc.pendingTurns ++
			}
			return true
		})
		return true
	})

	// move handler outside because the pendingAccepts.Range is Sync func
	for _,params := range(timeOutProposals){
		if isOpProposalKey(params["key"]) {
			opKey := StringToOpProposal(params["key"])
			if handler,ok := proposer.node.RegistedOpProposalTimeout[opKey.Op];ok {
				handler(proposer.node,opKey,params["value"])
			}
		}
	}

	// remove Message Keys That do not have proposal
	// 这个还不如写个GetLock好用。。我靠连len都没有
	proposer.pendingPrepares.Range(func(key interface{},val interface{}) bool{
		pendingMsgs := val.(*sync.Map)
		var msgCount = 0
		pendingMsgs.Range(func(key interface{},val interface{}) bool {
			msgCount ++
			return false
		})
		if msgCount == 0 {
			proposer.pendingPrepares.Delete(key)
		}
		return true
	})

	proposer.pendingAccepts.Range(func(key interface{},val interface{}) bool{
		pendingAccMap := val.(*sync.Map)
		var msgCount = 0
		pendingAccMap.Range(func(key interface{},val interface{}) bool {
			msgCount ++
			return false
		})
		if msgCount == 0 {
			proposer.pendingPrepares.Delete(key)
		}
		return true
	})

}

func (node* Node)kickOff(key string) {
	opKey := OpProposal{}
	opKey.Op = OP_TYPE_KICK_OFF
	opKey.Key = key
	if node.isLeader() {
		node.proposer.prepareOp(opKey,"","")
	} else {
		opKey.Version = "SENDER:" + node.node_hash
		if leaderhash,ok := node.decidedValues.Load(LEADER);!ok {
			fmt.Println("Leader not decided Cannot Kick off Member on Key: " + key)
		} else {
			node.proposer.prepareOp(opKey,"",leaderhash.(string))
		}
	}
}


func KickOffPrepareReceived(node* Node,key OpProposal,prepare* PrepareMsg) error {
	acceptor := node.acceptor
	isLeader := node.isLeader()
	if leader_hash,ok := node.decidedValues.Load(LEADER);!ok && !isLeader{
		// no leader ignore it
		return nil
	} else {
		if prepare.ProposerHash != leader_hash.(string) && !isLeader{
			// not a leader ignore it
			return nil
		}
	}
	if isLeader && prepare.ProposerHash != node.node_hash{
		// 不是Leader的请求，是node的请求
		// 唉，这个其实不用这样，但要重用代码就重新发一个prepare
		// Leader 重新发Prepare 请求,原来的请求作废
		if !node.isNodeAlive(key.Key) {
			// 确实连接不上了
			fmt.Println("Receive Kick off Request From " + prepare.ProposerHash + " Resend New Kick off prepare to All")
			key_newKey := OpProposal{}
			key_newKey.Key = key.Key
			key_newKey.Op = key.Op
			key_newKeyStr := OpProposalKeyToString(key_newKey)
			if _,ok := node.proposer.pendingPrepares.Load(key_newKeyStr);ok {
				// some kick off request is going on this key, ignore this
				return nil
			}
			node.kickOff(key.Key)
		}
		return nil
	}
	// 自己的请求
	proposer,ok := node.peers.LoadPeer(prepare.ProposerHash)
	if !ok {
		// dead leader
		return nil
	}
	id := big.NewFloat(0)
	id, _ = id.SetString(prepare.Id)
	if max_id,ok := acceptor.max_id.Load(prepare.Key);!ok || id.Cmp(max_id.(*big.Float)) > 0 {

		acceptor.max_id.Store(prepare.Key,id)
		promise := new(Promise)
		promise.Approved = true
		promise.ConsensusReached = false
		promise.Accepted_val = key.Key
		promise.Proposal_id = prepare.Id
		promise.Key = prepare.Key
		payload, err := promise.Package()
		if err != nil {
			return err
		}
		fmt.Println("Promise Kick Off Value Key: " + promise.Key + " Value: " + promise.Accepted_val + " ID: " + promise.Proposal_id)
		acceptor.node.send(proposer.Addr, payload)
	}
	return nil
}


func KickOffPromiseReceived(node* Node,key OpProposal,promise* Promise) (error) {
	proposer := node.proposer
	fmt.Println("Kick Off Promise Received: " + promise.Key + " Value: " + promise.Accepted_val + " ID: " + promise.Proposal_id)
	theMap,exist := proposer.pendingPrepares.Load(promise.Key)
	if exist {
		p,ok := theMap.(*sync.Map).Load(promise.Proposal_id)
		if !ok {
			// too late already timeout
			return nil
		}
		pendingPrepare := p.(*PendingMessage)
		pendingPrepare.pendingTurns = 0
		if len(promise.Accepted_val) == 0 {
			// wrong message
			fmt.Println("No Kick off Node Value")
			return nil
		}
		pendingPrepare.passNodes ++
		if int(pendingPrepare.passNodes) > proposer.node.peers.Count()/2 {
			// pending Prepare should be cleared
			// 真烦
			// Kick off 就kick off le
			proposer.propose(promise.Proposal_id, promise.Key,promise.Accepted_val)

		}
	} else {
		// already Timeout,wait for next round
	}
	return nil
}

func KickOffProposalReceived (node* Node,key OpProposal,proposal* Proposal) (error) {
	acceptor := node.acceptor
	// 暂时就这样吧，虽然浪费资源但不用改啊
	if leader_hash,ok := node.decidedValues.Load(LEADER);!ok {
		return nil
	} else {
		if proposal.ProposerHash == leader_hash {
			// leader 说要踢了它，双手赞成
			acc := new(Accept)
			acc.Key = proposal.Key
			acc.Val = proposal.Val
			acc.Proposal_id = proposal.Id
			acc.Epoch = acceptor.node.epoch
			acc.AcceptorHash = acceptor.node.node_hash
			acceptor.node.proposer.acceptReceived(acc)
			fmt.Println("Broadcast Accept Kick Off Key: " + acc.Key + " Value: " + acc.Val + " ID: " + acc.Proposal_id)
			acceptor.node.broadCast(acc)
		}
	}
	return nil
}

//for log
func KickOffAcceptReceived(node* Node,key OpProposal,accept* Accept)(error){
	fmt.Println("Kick Off Accept Received Key: " + accept.Key + " Value: " + accept.Val +
		" ID: " + accept.Proposal_id)
	return nil
}

func KickOffConsensusReached (node* Node,key OpProposal,val string)(error) {
	proposer := node.proposer
	proposer.node.decidedValues.Store(key.Key,val)
	fmt.Println("Consensus Reached on Kick Off on Key: " + key.Key)
	// remove that peer
	node.peers.DeletePeer(val)
	if node.worker.superviseServiceOn {
		node.worker.newCycle()
	}
	return nil
}

func (node* Node) registBasicConsensusHandlers() {
	node.RegisteOpPrepareRecvHandlers[OP_TYPE_SET_CONST] = SetConstPrepareReceived
	node.RegistedOpProposalRecvHandlers[OP_TYPE_SET_CONST] = SetConstProposalReceived
	node.RegistedOpPromiseRecvHandlers[OP_TYPE_SET_CONST] = SetConstPromiseReceived
	node.RegistedOpProposalTimeout[OP_TYPE_SET_CONST] = SetConstProposalTimeout
	node.RegistedOpConsensusReachHandlers[OP_TYPE_SET_CONST] = SetConstConsensusReached
	node.RegisteOpPrepareRecvHandlers[OP_TYPE_CHANGE_LEADER] = ChangeLeaderPrepareReceived
	node.RegistedOpProposalRecvHandlers[OP_TYPE_CHANGE_LEADER] = ChangeLeaderProposalReceived
	node.RegistedOpPromiseRecvHandlers[OP_TYPE_CHANGE_LEADER] = ChangeLeaderPromiseReceived
	node.RegistedOpProposalTimeout[OP_TYPE_CHANGE_LEADER] = ChangeLeaderProposalTimeout
	node.RegistedOpConsensusReachHandlers[OP_TYPE_CHANGE_LEADER] = ChangeLeaderConsensusReached
	node.RegisteOpPrepareRecvHandlers[OP_TYPE_KICK_OFF] = KickOffPrepareReceived
	node.RegistedOpPromiseRecvHandlers[OP_TYPE_KICK_OFF] = KickOffPromiseReceived
	node.RegistedOpProposalRecvHandlers[OP_TYPE_KICK_OFF] = KickOffProposalReceived
	node.RegistedOpAcceptRecvHandlers[OP_TYPE_KICK_OFF] = KickOffAcceptReceived
	node.RegistedOpConsensusReachHandlers[OP_TYPE_KICK_OFF] = KickOffConsensusReached
	node.epocOutDateHandler = epocOutDateHandler
}



