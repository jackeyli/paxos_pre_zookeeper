package node

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"math/big"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
)

type DataSetInstance struct {
	db* leveldb.DB
}

type Record struct {
	Key string
	Version uint32
	Content[] byte
}

type DataSyncPreResponse struct{
	HasData bool
	Version uint32
	MyHash string `json:"NodeHash"`
}

type DataTransferRequest struct {
	Key string
}

type RawMessage struct {
	content[]byte
}

var OP_TYPE_DATA_SYNC = "DATA_SYNC"
var PAYLOAD_DATATRANSFER_REQUEST uint8 = 7
var PAYLOAD_RAW uint8 = 8
// I am a stupid guy
// Only the leader could propose Data Sync,other node should first talk to leader when need sync
// I am not going to handle multiple proposers damn

// some function of getMyRecord
func (node* Node) getMyRecord(key string) (*Record, bool) {
	val,err := node.dataInst.db.Get([]byte(key),nil)
	if err != nil {
		return nil,false
	}
	version := val[0:4]
	content := val[4:]
	record := new(Record)
	record.Key = key
	record.Content = content
	record.Version = binary.BigEndian.Uint32(version)
	return record,true
}

func recordToByte(record* Record) []byte{
	version := make([]byte,4)
	binary.BigEndian.PutUint32(version,record.Version)
	return append(version,record.Content...)
}

// some function of update My Record
func (node* Node) updateMyRecord(key string,val []byte,updateVersion bool) bool {
	if updateVersion {
		vInt := binary.BigEndian.Uint32(val[0:4])
		version := make([]byte,4)
		binary.BigEndian.PutUint32(version,vInt + 1)
		contentBytes := append(version,val[4:]...)
		err := node.dataInst.db.Put([]byte(key),contentBytes,nil)
		if err != nil {
			fmt.Println("Update data instance Fail Key: " + key )
			return false
		}
	} else {
		err := node.dataInst.db.Put([]byte(key),val,nil)
		if err != nil {
			fmt.Println("Update data instance Fail Key: " + key )
			return false
		}
	}
	return true
}

func (node* Node) deleteMyRecord(key string) bool{
	node.dataInst.db.Delete([]byte(key),nil)
	return true
}

func (msg *DataTransferRequest) Package() (payload *PayLoad,err error) {
	payload = new(PayLoad)
	payload.PayLoadType = PAYLOAD_DATATRANSFER_REQUEST
	payload.Payload,err = json.Marshal(msg)
	return
}
func (msg *RawMessage) Package() (payload *PayLoad,err error) {
	payload = new(PayLoad)
	payload.PayLoadType = PAYLOAD_RAW
	payload.Payload = msg.content
	return
}

func DataSyncPrepareReceived(node* Node,key OpProposal,prepare* PrepareMsg) error {
	acceptor := node.acceptor
	isLeader := node.isLeader()
	if leader_hash,ok := node.decidedValues.Load(LEADER);!ok && !isLeader{
		fmt.Println("No Leader,ignore Data Sync Request " + key.Key)
		// no leader ignore it
		return nil
	} else {
		if prepare.ProposerHash != leader_hash.(string) && !isLeader{
			// not a leader ignore it
			fmt.Println("I am not leader,ignore Data Sync Request " + key.Key)
			return nil
		}
	}
	if isLeader && prepare.ProposerHash != node.node_hash{
		// 不是Leader的请求，是node的请求
		// 唉，这个其实不用这样，但要重用代码就重新发一个prepare
		// Leader 重新发Prepare 请求,原来的请求作废

		fmt.Println("Receive Data Sync Request From " + prepare.ProposerHash + " Resend New Prepare to All")
		key_newKey := OpProposal{}
		key_newKey.Key = key.Key
		key_newKey.Op = key.Op
		key_newKeyStr := OpProposalKeyToString(key_newKey)
		if _,ok := node.proposer.pendingPrepares.Load(key_newKeyStr);ok {
			// some sync request is going on this key, ignore this
			return nil
		}
		node.startSyncRecord(key.Key)
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
		if key.Key == KEY_UNDONE_TASK {
			fmt.Println("Start to response prepare " + prepare.Key + " ID: "+ prepare.Id)
		}
		acceptor.max_id.Store(prepare.Key,id)
		dataKey := key.Key
		record, ok := node.getMyRecord(dataKey)
		resp := new(DataSyncPreResponse)
		if !ok {
			// I don't have this data,tell him
			resp.HasData = false
			resp.Version = 0
			resp.MyHash = node.node_hash
		} else {
			// tell him my Version of data
			resp.HasData = true
			resp.Version = record.Version
			resp.MyHash = node.node_hash
		}
		promise := new(Promise)
		promise.Approved = true
		promise.ConsensusReached = false
		bytes, _ := json.Marshal(resp)
		promise.Accepted_val = string(bytes)
		promise.Proposal_id = prepare.Id
		promise.Key = prepare.Key

		payload, err := promise.Package()
		if err != nil {
			fmt.Println("Data Sync Failed to Package Promise Key: " + prepare.Key + " ID: "+ prepare.Id)
			return err
		}
		fmt.Println("Promise Const Value Key: " + promise.Key + " Value: " + promise.Accepted_val + " ID: " + promise.Proposal_id)
		acceptor.node.send(proposer.Addr, payload)
	}else {
		fmt.Println("local max id Of this Key is " + max_id.(*big.Float).String() + ", the send Proposal ID is " +
			prepare.Id + " ignore it")
	}
	return nil
}


func (node* Node)startSyncRecord(key string) {
	opKey := OpProposal{}
	opKey.Op = OP_TYPE_DATA_SYNC
	opKey.Key = key
	if node.isLeader() {
		node.proposer.prepareOp(opKey,"","")
	} else {
		opKey.Version = node.node_hash
		if leaderhash,ok := node.decidedValues.Load(LEADER);!ok {
			fmt.Println("Leader not decided Cannot Sync Record on Key: " + key)
		} else {
			node.proposer.prepareOp(opKey,"",leaderhash.(string))
		}
	}
}


func DataSyncPromiseReceived(node* Node,key OpProposal,promise* Promise) (error) {
	proposer := node.proposer
	fmt.Println("Data Sync Promise Received: " + promise.Key + " Value: " + promise.Accepted_val + " ID: " + promise.Proposal_id)
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
			fmt.Println("Bad DataSync Response No Value")
			return nil
		}
		syncResp := new(DataSyncPreResponse)
		err :=json.Unmarshal([]byte(promise.Accepted_val),syncResp)
		if err != nil {
			fmt.Println("Bad DataSync Response")
		}

		if syncResp.HasData {
			max_acc := new(Max_ACC_Message)
			max_acc.max_accept_id = promise.Accepted_id
			// 早知道就用[]byte了，哎我真菜啊,彩笔
			max_acc.max_accept_val = promise.Accepted_val

			if pendingPrepare.max_acc_info == nil {
				pendingPrepare.max_acc_info = max_acc
			} else {
				max_acc_resp := new(DataSyncPreResponse)
				err = json.Unmarshal([]byte(pendingPrepare.max_acc_info.max_accept_val),max_acc_resp)
				if max_acc_resp.Version < syncResp.Version {
					pendingPrepare.max_acc_info = max_acc
				} else {
					if max_acc_resp.Version == syncResp.Version &&
						syncResp.MyHash != node.node_hash {
						// 不是Leader , Leader不想transfer data
						pendingPrepare.max_acc_info = max_acc
					}
				}
			}
		}

		pendingPrepare.passNodes ++
		// must collect all the data from the cluster
		if int(pendingPrepare.passNodes) == proposer.node.peers.Count() {
			// pending Prepare should be cleared
			// 真烦
			max_acc := pendingPrepare.max_acc_info
			success := atomic.CompareAndSwapInt32(&(pendingPrepare.isLocked),0,1)
			if success {
				if max_acc != nil {
					// send Proposal accepted proposal value
					fmt.Println("Propose Const Value Key : " + promise.Key + " Value :" + max_acc.max_accept_val + " ID: " + promise.Proposal_id)
					// first accept myself
					proposer.propose(promise.Proposal_id, promise.Key, max_acc.max_accept_val)
				} else {
					fmt.Println("No one has this data do not propose")
					//proposer.propose(promise.Proposal_id, promise.Key, pendingPrepare.value)
				}
			}
		}
	} else {
		// already Timeout,wait for next round
	}
	return nil
}

func DataSyncProposalReceived (node* Node,key OpProposal,proposal* Proposal) (error) {
	acceptor := node.acceptor
	// 暂时就这样吧，虽然浪费资源但不用改啊
	success := updateAcceptedVal(acceptor,proposal.Key,proposal.Val,proposal.Id)
	if(!success) {
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
	fmt.Println("Broadcast Accept Data Sync Key: " + acc.Key + " Value: " + acc.Val + " ID: " + acc.Proposal_id)
	acceptor.node.broadCast(acc)

	return nil
}

// for log
func DataSyncAcceptReceived(node* Node,key OpProposal,accept* Accept)(error){
	fmt.Println("DataSync Accept Received Key: " + accept.Key + " Value: " + accept.Val +
		" ID: " + accept.Proposal_id)
	return nil
}

func DataSyncConsensusReached (node* Node,key OpProposal,val string)(error) {
	proposer := node.proposer
	proposer.node.decidedValues.Store(key.Key,val)
	fmt.Println("Consensus Reached on Data Sync on Key: " + key.Key)
	syncResp := new(DataSyncPreResponse)
	err :=json.Unmarshal([]byte(val),syncResp)
	if err != nil {
		fmt.Println("Consensus Reached But Bad DataSync Response  on Key: " + key.Key)
	}
	record,ok := node.getMyRecord(key.Key)
	if !ok || record.Version < syncResp.Version {
		// go get data from him
		go node.getDataProcess(key,*syncResp)
	} else {
		node.onLatestDataUpdated(key,false)
	}
	return nil
}

func(node* Node) onLatestDataUpdated(key OpProposal,isFetch bool) {
	if key.Key == KEY_UNDONE_TASK {
		// just get the latest undone task list from others
		// now open supervise thread
		node.worker.startService()
		if node.worker.freshNew {
			UndoneTasks,_ := node.worker.getUndonePubTask()
			for _,v:=range(UndoneTasks.UndonTsks) {
				node.startSyncRecord(v)
			}
			node.worker.freshNew = false
		}
		node.worker.newCycle()
	}
	if node.worker.superviseServiceOn {
		node.worker.newCycle()
	}
}

func DataSyncProposalTimeout (node* Node,key OpProposal,val string) (error) {
	if node.isLeader() {
		// do something
		// should retry or some thing but I just write here...
		// just Failed restart the node haha..
	} else {
		// ignore
	}
	return nil
}

func(node* Node) getDataProcess(key OpProposal,syncResp DataSyncPreResponse) {
	fmt.Println("Get Data From Key:" + key.Key + " Node: " + syncResp.MyHash +
		" Version :" + strconv.FormatUint(uint64(syncResp.Version),10))
	nodeAddr,ok:= node.peers.LoadPeer(syncResp.MyHash)
	if !ok {
		// no such peer,wait for next round
		fmt.Println("No such peer on get Data Process")
		return
	}
	fmt.Println("Connect to address " + nodeAddr.DataTransferAddr)
	conn,err := net.Dial("tcp", nodeAddr.DataTransferAddr)
	if err != nil {
		fmt.Println("Connection Error on address " + nodeAddr.DataTransferAddr)
		return
	}
	defer conn.Close()
	req := new(DataTransferRequest)
	req.Key = key.Key
	payload,err := req.Package()
	err = node.writeToConn(conn,payload)
	if err == nil {
		pl,err := node.readPayloadFromConn(conn)
		if err != nil {
			fmt.Println("No response on Get Data From Key: " + key.Key)
			return
		} else {
			content := pl.Payload
			node.updateMyRecord(key.Key,content,false)
			node.onLatestDataUpdated(key,true)
		}
	} else {
		fmt.Println("Cannot Write Data Request to node hash: " + syncResp.MyHash)
	}
}

func(node* Node)handleDataTransferPayLoad(conn net.Conn,payLoad* PayLoad) error {
	msg,err := unpackagePayLoad(payLoad)
	if err != nil {
		return err
	}
	if payLoad.PayLoadType == PAYLOAD_DATATRANSFER_REQUEST {
		fmt.Println("Receive Data Transfer Request ")
		req := msg.(*DataTransferRequest)
		record,ok := node.getMyRecord(req.Key)
		if (!ok) {
			fmt.Println("Error Don't have data Key: " + req.Key)
		}
		buff := make([]byte,0)
		version := make([]byte,4)
		binary.BigEndian.PutUint32(version,record.Version)
		buff = append(buff,version...)
		buff = append(buff,record.Content...)
		pl := new(PayLoad)
		pl.PayLoadType = PAYLOAD_RAW
		pl.Payload = buff
		err := node.writeToConn(conn,pl)
		if err != nil {
			fmt.Println("Error in transfer Data Key: " + req.Key)
		}
	}
	return nil
}



func (node* Node) dataTransferHandler(conn net.Conn) error {
	defer conn.Close()
	payload,err := node.readPayloadFromConn(conn)
	if err != nil {
		return err
	}
	node.handleDataTransferPayLoad(conn,payload)
	return nil
}


func (node* Node) RegistDataTransferHandlers() {
	node.RegisteOpPrepareRecvHandlers[OP_TYPE_DATA_SYNC] = DataSyncPrepareReceived
	node.RegistedOpProposalRecvHandlers[OP_TYPE_DATA_SYNC] = DataSyncProposalReceived
	node.RegistedOpPromiseRecvHandlers[OP_TYPE_DATA_SYNC] = DataSyncPromiseReceived
	node.RegistedOpProposalTimeout[OP_TYPE_DATA_SYNC] = DataSyncProposalTimeout
	node.RegistedOpConsensusReachHandlers[OP_TYPE_DATA_SYNC] = DataSyncConsensusReached
	node.RegistedOpAcceptRecvHandlers[OP_TYPE_DATA_SYNC] = DataSyncAcceptReceived
}



func (node* Node) dataTransferService() {
	l, err := net.Listen("tcp", node.data_addr)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
	}
	defer l.Close()
	fmt.Println("data transfer Service Listen on " + node.data_addr)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
		}
		// Handle connections in a new goroutine.
		// 不管了
		go node.dataTransferHandler(conn)
	}
}