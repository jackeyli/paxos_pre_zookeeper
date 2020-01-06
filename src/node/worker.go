package node

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"
)
var KEY_UNDONE_SUBTASK = "UNDONE_SUBTASK"
var KEY_UNDONE_TASK = "UNDONE_PUB_TASK"
var KEY_RUNNING_TASK = "RUNNINGTASK"
var KEY_MERGING_TASK = "MERGINGTASK"
var SUBTASK_FAIL_NO_TSK = 0
var SUBTASK_FAIL_BAD_TSK_DATA = 1
var SUBTASK_FAIL_NO_RESOURCE = 3
var SUBTASK_FAIL_CANNOT_GET_RESULT = 3
// why this is so strangely heavy Load,but Ok


type SubTask struct {
	ArrangeTo string // node hash
	FromByteIdx uint64
	ToByteIdx uint64
	ResourceHash string
	Status string
	ResultKey string
}

type UndoneTaskList struct {
	UndonTsks map[string]string
}

type TaskResult struct {
	TaskId string
	Result map[string]uint32
}

type MainTaskResult struct {
	TaskId string
	Entries []ResultEntry
}

type ResultEntry struct {
	Key string
	Count uint32
}

type Task struct {
	SubTaskKeys []string
	Status string
	ResourceKey string
	ResultHash string
}

type RunningTask struct {
	Status string
	TaskId string
}

type Worker struct {
	node* Node
	lock* int32
	superviseServiceOn bool
	freshNew bool
	startServiceCycle chan int32
	superVisedSubTsks *sync.Map
}


func (node* Node) createWorker() {
	worker := new(Worker)
	worker.node = node
	worker.lock = new(int32)
	*worker.lock = 0
	worker.superVisedSubTsks = new (sync.Map)
	worker.freshNew = true
	worker.superviseServiceOn = false
	worker.startServiceCycle = make(chan int32)
	node.worker = worker
}
func isWordChar(char uint8) bool {
	return (char >= 97 && char <= 122) || (char >= 65 && char <= 90)|| (char == 45)
}

func(worker* Worker) runOnFindTask(subTaskKey string) {
	fmt.Println("Worker start run on subTaskKey: " + subTaskKey)
	subTask,version := worker.getSubTask(subTaskKey)
	if subTask == nil {
		fmt.Println("Don't have the subTask")
		worker.onSubTaskFail(subTaskKey,uint8(SUBTASK_FAIL_NO_TSK))
		return
	}
	record,ok := worker.node.getMyRecord(subTask.ResourceHash)
	if !ok {
		fmt.Println("Don't have the resource")
		worker.onSubTaskFail(subTaskKey,uint8(SUBTASK_FAIL_NO_RESOURCE))
		return
	}
	resource := record.Content
	wordsCount := make(map[string]uint32)
	myPart := resource[subTask.FromByteIdx:subTask.ToByteIdx]
	tempStr := ""
	for p2 := 0;p2 < len(myPart);p2 ++{
		if isWordChar(myPart[p2]) {
			tempStr += string(myPart[p2])
		} else {
			if len(tempStr) == 0 {
				continue
			} else {
				wordsCount[tempStr] += 1
				tempStr = ""
			}
		}
	}
	if len(tempStr) > 0 {
		wordsCount[tempStr] += 1
	}
	// save Result to db
	result := new(TaskResult)
	result.TaskId = subTaskKey
	result.Result = wordsCount
	fmt.Println("Result Generated On SubTask Key: " + subTaskKey)
	resKey,err := GenerateKey(result)
	if err != nil {
		fmt.Println("Error On Worker Hash Result bytes on SubTask Key: " + subTaskKey)
		worker.onSubTaskFail(subTaskKey,uint8(SUBTASK_FAIL_CANNOT_GET_RESULT))
		return
	}
	success := worker.updateRecordObject(resKey,result,0,true)
	// 我他妈菜出屎了，写了3天还没写完，我靠，悲剧啊
	if !success {
		// 累得一毛 坑爹啊啊啊啊这个怎么这么烦
		fmt.Println("Error On save sub task result to db")
		worker.onSubTaskFail(subTaskKey,uint8(SUBTASK_FAIL_CANNOT_GET_RESULT))
	} else {
		fmt.Println("Success fully save sub task result to db on SubTask Key: " + subTaskKey)
		// update SubTask
		subTask.Status = "DONE"
		subTask.ResultKey = resKey
		success := worker.updateRecordObject(subTaskKey,subTask,version,true)
		if !success {
			fmt.Println("Error On Worker update sub task on Key: " + subTaskKey)
			worker.onSubTaskFail(subTaskKey,uint8(SUBTASK_FAIL_CANNOT_GET_RESULT))
			return
		} else {
			fmt.Println("Sub Task Done on Key: " + subTaskKey)

			// now Need to talk to the leader

			// 1. Sync The SubTask to All Nodes


			// 2. Sync The result to All Nodes
			worker.OnSubTaskDone(subTaskKey,string(resKey))
		}
	}
 }

func(worker* Worker) getRunningTask() *RunningTask{
	v,err := worker.node.dataInst.db.Get([]byte(KEY_RUNNING_TASK),nil)
	if err != nil {
		// bye
		return nil
	}
	runTask := new(RunningTask)
	err = json.Unmarshal(v,runTask)
	if err != nil {
		return nil
	}
	return runTask
}

func(worker* Worker) updateRunningTask(task* RunningTask) bool{
	updatedRunTask,err := json.Marshal(task)
	if err != nil {
		//bye
		return false
	}
	err = worker.node.dataInst.db.Put([]byte(KEY_RUNNING_TASK),updatedRunTask,nil)
	if err != nil {
		return false
	}
	return true
}

func (worker* Worker) getUndonePubTask() (*UndoneTaskList,uint32){
	rec,ok :=worker.node.getMyRecord(KEY_UNDONE_TASK)
	if !ok{
		//bye bye
		return nil,0
	}
	tskList := new(UndoneTaskList)
	err := json.Unmarshal(rec.Content,tskList)
	if err != nil {
		return nil,0
	}
	return tskList,rec.Version
}

func (worker* Worker) getUndoneSubTask() *UndoneTaskList {
	v,err := worker.node.dataInst.db.Get([]byte(KEY_UNDONE_SUBTASK),nil)
	if err != nil {
		// bye
		return nil
	}
	runTask := new(UndoneTaskList)
	err = json.Unmarshal(v,runTask)
	if err != nil {
		return nil
	}
	return runTask
}

func(worker* Worker) updateUndoneSubTask(task* UndoneTaskList) bool{
	updatedSubTask,err := json.Marshal(task)
	if err != nil {
		//bye
		return false
	}
	err = worker.node.dataInst.db.Put([]byte(KEY_UNDONE_SUBTASK),updatedSubTask,nil)
	if err != nil {
		return false
	}
	return true
}

func(worker* Worker) onSubTaskFail(subTaskKey string,code uint8) {
	// UPDATE RUNNINGTASK as Fail
	runTask := worker.getRunningTask()
	if runTask == nil {
		// bye bye
		return
	}
	runTask.Status = "FAILED"
	if !worker.updateRunningTask(runTask) {
		// bye bye
		return
	}

	if code == uint8(SUBTASK_FAIL_NO_TSK) {
		worker.node.startSyncRecord(subTaskKey)
	}
	if code == uint8(SUBTASK_FAIL_BAD_TSK_DATA) {
		// clear the task Data
		worker.node.deleteMyRecord(subTaskKey)
		worker.node.startSyncRecord(subTaskKey)
	}
	if code == uint8(SUBTASK_FAIL_NO_RESOURCE) {
		subTaskRecord,ok := worker.node.getMyRecord(subTaskKey)
		if !ok {
			// go dead
			return
		}
		task := new(SubTask)
		err := json.Unmarshal(subTaskRecord.Content,task)
		if err != nil {
			// bye bye
			return
		}
		worker.node.startSyncRecord(task.ResourceHash)
	}
	worker.newCycle()
}

func(worker* Worker) OnSubTaskDone(subTaskKey string,resultKey string) {
	// request Sync Record To Leader
	fmt.Println("Sub Task Finished on Key: " + subTaskKey + " Local Result Generated on Key:" + resultKey)
	worker.node.startSyncRecord(subTaskKey)
	worker.node.startSyncRecord(resultKey)
	runningTaskNow := worker.getRunningTask()
	runningTaskNow.Status = "DONE"
	worker.updateRunningTask(runningTaskNow)
	worker.newCycle()
	//worker.node.dataInst.db.Delete([]byte(KEY_RUNNING_TASK),nil)
}

func(worker *Worker) getMainTask(key string) (*Task,uint32){
	rec,ok :=worker.node.getMyRecord(key)
	if !ok {
		//bye bye
		return nil,0
	}
	tsk := new(Task)
	err := json.Unmarshal(rec.Content,tsk)
	if err != nil {
		return nil,0
	}
	return tsk,rec.Version
}

func(worker *Worker) updateRecordObject(key string,val interface{},oldVersion uint32,checkVersion bool) bool {
	rec,ok :=worker.node.getMyRecord(key)
	if !ok {
		rec = new(Record)
	}
	content,err := json.Marshal(val)
	if err != nil {
		return false
	}
	rec.Content = content
	if ok && rec.Version > oldVersion {
		fmt.Println("Version Conflict on Key : " + key)
		return false
	}
	return worker.node.updateMyRecord(key,recordToByte(rec),checkVersion)
}

func(worker *Worker) getSubTask(key string) (*SubTask,uint32){
	rec,ok :=worker.node.getMyRecord(key)
	if !ok {
		//bye bye
		return nil,0
	}
	tsk := new(SubTask)
	err := json.Unmarshal(rec.Content,tsk)
	if err != nil {
		return nil,0
	}
	return tsk,rec.Version
}

func(worker* Worker) getMainTaskResult(key string) *MainTaskResult {
	rec,ok :=worker.node.getMyRecord(key)
	if !ok {
		//bye bye
		return nil
	}
	tsk := new(MainTaskResult)
	err := json.Unmarshal(rec.Content,tsk)
	if err != nil {
		return nil
	}
	return tsk
}


func(worker *Worker) getTaskResult(key string) *TaskResult{
	rec,ok :=worker.node.getMyRecord(key)
	if !ok {
		//bye bye
		return nil
	}
	res := new(TaskResult)
	err := json.Unmarshal(rec.Content,res)
	if err != nil {
		return nil
	}
	return res
}
func(worker *Worker) trySetMerging() bool{
	rec,_ := worker.node.getMyRecord(KEY_MERGING_TASK)
	if rec.Content[0] == 1 {
		return false
	}
	worker.node.updateMyRecord(KEY_MERGING_TASK,[]byte{0,0,0,0,1},false)
	return true
}
func(worker* Worker) releaseMerging() bool {
	worker.node.updateMyRecord(KEY_MERGING_TASK,[]byte{0,0,0,0,0},false)
	return true
}
func(worker* Worker) leaderSupervise(){
	undonTasks,version := worker.getUndonePubTask()
	if undonTasks == nil {
		// bye
		return
	}
	var needUpdateUndonTasks = false
	for mainK,_:=range(undonTasks.UndonTsks) {
		tsk,mainVersion := worker.getMainTask(mainK)
		if tsk.Status == "DELIVERED"{
			// already send to client or send failed,wait for the client request again
			delete(undonTasks.UndonTsks,mainK)
			needUpdateUndonTasks = true
			continue
		}
		if tsk.Status == "DELIVER_FAILED" {
			continue
		}
		if tsk.Status == "DONE" {
			// will now send result to client
			fmt.Println("Main Task Done, Will Send Result to the Client")
			res := worker.getMainTaskResult(tsk.ResultHash)
			if res == nil {
				fmt.Println("Main Task Done, But Leader didn't get the result request data sync from members")
				worker.node.startSyncRecord(tsk.ResultHash)
			} else {
				bytes,err := json.Marshal(res)
				if err == nil {
					// print result
					outputChan,ok := worker.node.OutputChannels.Load(mainK)
					if !ok {
						fmt.Println("Bad output Chan")
						tsk.Status = "DELIVER_FAILED"
						worker.updateRecordObject(mainK,tsk,mainVersion,true)
						continue
					}
					outputChan.(chan []byte) <-bytes
					tsk.Status = "DELIVERED"
					succ := worker.updateRecordObject(mainK,tsk,mainVersion,true)
					delete(undonTasks.UndonTsks,mainK)
					if succ {
						worker.node.startSyncRecord(mainK)
					} else {

					}
				} else {
					fmt.Println("Bad Result...")
				}
			}
			continue
		}
		if len(tsk.SubTaskKeys) > 0 {
			//
			var allDone = true
			for _,subK := range(tsk.SubTaskKeys) {
				subTsk,_ := worker.getSubTask(subK)
				if subTsk == nil {
					//
					worker.superVisedSubTsks.Store(subK,subK)
					allDone = false
					continue
				}
				if subTsk.Status != "DONE" {
					// may be the subTsk has done but not uploaded yet
					worker.checkUndoneSubTaskStatus(subTsk,tsk,subK,mainK,mainVersion)
					allDone = false
				}
				if subTsk.Status == "DONE" {
					TskRes := worker.getTaskResult(subTsk.ResultKey)
					if TskRes == nil{
						// try get result
						worker.node.startSyncRecord(subTsk.ResultKey)
						allDone = false
					}
				}
			}
			if allDone {
				// begin Merge Results
				if worker.trySetMerging() {
					fmt.Println("All Sub Task Done on task Key: " + mainK + " Start Merge Results ")
					go worker.mergeResults(mainK, tsk, mainVersion)
				}
			}
		}
	}
	if needUpdateUndonTasks {
		worker.updateRecordObject(KEY_UNDONE_TASK,undonTasks,version,true)
		worker.node.startSyncRecord(KEY_UNDONE_TASK)
	}
}

func GenerateKey(val interface{}) (string,error){
	bytes,err := json.Marshal(val)
	if err != nil {
		return "",err
	}
	hsher := sha256.New()
	prefByte := []byte(time.Now().String())
	hsher.Write(prefByte)
	hsher.Write(bytes)
	res := hsher.Sum(nil)
	key_suffix := hex.EncodeToString(res)
	actualKey := reflect.TypeOf(val).String() + key_suffix
	return actualKey,nil
}

func(worker* Worker) mergeResults(tskKey string,task* Task,tskVersion uint32) {
	defer worker.releaseMerging()
	defer worker.newCycle()
	totalResults := make(map[string]uint32)
	for _,k := range(task.SubTaskKeys) {
		subTsk,_ := worker.getSubTask(k)
		if subTsk == nil {
			//bye bye
			return
		}
		TskRes := worker.getTaskResult(subTsk.ResultKey)
		if TskRes == nil {
			//bye bye
			return
		}
		for k,v :=range(TskRes.Result) {
			totalResults[k] += v
		}
	}
	resultsList := make([]ResultEntry,0)
	for k,v:=range(totalResults) {
		ent := ResultEntry{Key:k,Count:v}
		resultsList = append(resultsList, ent)
	}
	// 反向排序
	sort.SliceStable(resultsList, func(i, j int) bool {
		return resultsList[i].Count > resultsList[j].Count
	})
	// merge Done
	result := new(MainTaskResult)
	result.TaskId = tskKey
	result.Entries = resultsList
	resKey,err := GenerateKey(result)
	if err != nil {
		// byte
		return
	}
	success := worker.updateRecordObject(resKey,result,0,true)
	if !success {
		return
	}
	task.ResultHash = resKey
	task.Status = "DONE"
	success = worker.updateRecordObject(tskKey,task,tskVersion,true)
	if success {
		worker.onMainTaskDone(tskKey,resKey)
	}
}

func (worker* Worker) newCycle(){
	worker.startServiceCycle <- 1
}

func(worker* Worker) onMainTaskDone(tskKey string,resKey string) {
	fmt.Println("Main Task Done on task Key: " + tskKey + " Cheers! ")
	worker.node.startSyncRecord(tskKey)
	worker.node.startSyncRecord(resKey)
}


func(worker* Worker) findUndoneTask(){
	undonePubTsks,_ := worker.getUndonePubTask()
	undoneSubTsks := worker.getUndoneSubTask()
	for k,_ :=range(undonePubTsks.UndonTsks) {
		pubTsk,_ :=worker.getMainTask(k)
		if pubTsk == nil {
			// not synced
			continue
		}
		if pubTsk.Status != "DONE" {
			// arrange to me add to undoneSubTsksList
			for _,subKey := range(pubTsk.SubTaskKeys) {
				subTsk,_ := worker.getSubTask(subKey)
				if subTsk == nil {
					// not synced
					worker.node.startSyncRecord(subKey)
				} else {
					if subTsk.Status != "DONE" && subTsk.ArrangeTo == worker.node.node_hash {
						// It's my Task
						undoneSubTsks.UndonTsks[subKey] = subKey
					}
				}
			}
		}
	}
	worker.updateUndoneSubTask(undoneSubTsks)
}

func(worker* Worker) supervise() {
	runningTaskNow := worker.getRunningTask()
	if runningTaskNow == nil {
		return
	}
	var failedTaskId = ""
	if runningTaskNow.Status == "FAILED" {
		failedTaskId = runningTaskNow.TaskId
	}
	if runningTaskNow.Status == "IDOL" || runningTaskNow.Status == "DONE" || runningTaskNow.Status == "FAILED" {
		worker.findUndoneTask()
		undoneTasks := worker.getUndoneSubTask()
		if undoneTasks == nil {
			return
		}
		if runningTaskNow.Status == "DONE" {
			GetLock(worker.lock)
			defer ReleaseLock(worker.lock)
			delete(undoneTasks.UndonTsks,runningTaskNow.TaskId)
		}
		var findOneTaskToGo = false
		for k,_ := range(undoneTasks.UndonTsks) {
			tsk,ok := worker.node.getMyRecord(k)
			if !ok {
				fmt.Println("Task Information on Key:" + k + " not got")
				// request data Sync
				worker.superVisedSubTsks.Store(k,k)
			} else {
				subtsk := new(SubTask)
				err := json.Unmarshal(tsk.Content,subtsk)
				if err != nil {
					// bad subtsk info
					worker.superVisedSubTsks.Store(k,k)
					continue
				}
				if subtsk.Status == "DONE" || subtsk.ArrangeTo != worker.node.node_hash ||
					k == failedTaskId{
					// already done or not my task or it just failed and should wait for a while to get resources
					continue
				} else {
					runningTaskNow := worker.getRunningTask()
					runningTaskNow.Status = "RUNNING"
					runningTaskNow.TaskId = k
					worker.updateRunningTask(runningTaskNow)
					findOneTaskToGo = true
					// 有风险啊
					go worker.runOnFindTask(k)
				}
			}
		}
		if runningTaskNow.Status == "FAILED" && !findOneTaskToGo {
			// No other Task is doable retry failed task
			runningTaskNow.Status = "RUNNING"
			runningTaskNow.TaskId = failedTaskId
			worker.updateRunningTask(runningTaskNow)
			go worker.runOnFindTask(failedTaskId)
		}
	}
}

func (worker* Worker) workerService() {
	fmt.Println("Worker Service on")
	for ; ; {
		<-worker.startServiceCycle
		fmt.Println("New Worker Cycle Start!")
		worker.supervise()
		if worker.node.isLeader() {
			// 是leader 负责Merge所有Results,leader负担很重啊，没办法
			worker.leaderSupervise()
		}
	}
}

func(worker* Worker) startService() {
	if worker.superviseServiceOn {
		return
	}
	worker.superviseServiceOn = true
	go worker.node.outServer.service()
	go worker.workerService()
	go worker.tskResultAskLoop()
}
func(worker* Worker) Initialize() {
	// 没跑完就拜拜了
	runningTask := worker.getRunningTask()
	if runningTask == nil {
		runningTask := new(RunningTask)
		runningTask.Status = "IDOL"
		worker.updateRunningTask(runningTask)
	} else {
		runningTask.Status = "IDOL"
		runningTask.TaskId = ""
		worker.updateRunningTask(runningTask)
	}
	undoneTsks := worker.getUndoneSubTask()
	if undoneTsks == nil {
		undoneTsks = new(UndoneTaskList)
		undoneTsks.UndonTsks = make(map[string]string)
		worker.updateUndoneSubTask(undoneTsks)
	}
	undoneMainTsks,_ := worker.getUndonePubTask()
	if undoneMainTsks == nil {
		undoneMainTsks = new(UndoneTaskList)
		undoneMainTsks.UndonTsks = make(map[string]string)
		worker.updateRecordObject(KEY_UNDONE_TASK,undoneMainTsks,0,false)
	}
	_,ok := worker.node.getMyRecord(KEY_MERGING_TASK)
	if !ok {
		worker.node.updateMyRecord(KEY_MERGING_TASK,[]byte{0,0,0,0,0},false)
	}
}
//
//func(node* Node) mockReceive(){
//	f,err := os.Open("/Users/liwei/Desktop/goProject/distributeWordSplit/AliceOnTheWonderLand.txt")
//	defer f.Close()
//	if err != nil {
//		return
//	}
//	bytes,err := ioutil.ReadAll(f)
//	if err != nil {
//		return
//	}
//	node.receiveNewTask(bytes,nil)
//}
func(worker* Worker) checkUndoneSubTaskStatus(subTsk* SubTask,mainTsk* Task,subKey string,tskKey string,mainV uint32) {
	node := worker.node
	if !node.isNodeAlive(subTsk.ArrangeTo) {
		// the node is dead,arrange to a new people
		if _,ok := node.peers.LoadPeer(subTsk.ArrangeTo);ok {
			// peer not removed start kick off request
			fmt.Println("Node " + subTsk.ArrangeTo +" Dead, Wait for Kick off " + subTsk.ArrangeTo)
			return
		}
		fmt.Println("Node " + subTsk.ArrangeTo +" Dead, Start Rearrange Sub Task on subKey: " + subKey)
		_,info := node.peers.LoadPeers()
		var newNode = ""
		for _,v := range(info) {
			if(v.Hash != node.node_hash && node.isNodeAlive(v.Hash)) {
				newNode = v.Hash
				break
			}
		}
		if len(newNode) == 0 {
			newNode = node.node_hash
		}
		// if update the subtask , the member ship might also update this subtask and update its version,this will cause conflict among nodes
		// so the leader will only update the main task, cause no member will update the main task
		newSubTask := new(SubTask)
		newSubTask.ResultKey = subTsk.ResultKey
		newSubTask.Status = subTsk.Status
		newSubTask.ResourceHash = subTsk.ResourceHash
		newSubTask.ToByteIdx = subTsk.ToByteIdx
		newSubTask.FromByteIdx = subTsk.FromByteIdx
		newSubTask.ArrangeTo = newNode
		newSubKey,err := GenerateKey(newSubTask)
		if err != nil {
			fmt.Println("Leader Failed on Generating new SubKey to rearrange subtask on subKey: " + subKey)
		}
		fmt.Println("New Sub Task Generated on Key: " + newSubKey + " Old Key: " + subKey + " Arrange To: " + newNode)
		succ := worker.updateRecordObject(newSubKey,newSubTask,0,true)
		if !succ {
			fmt.Println("Leader Failed on Saving new SubTask to Local on old subKey: " + subKey)
		}
		newSubKeys := make([]string,0)
		var finded = false
		for idx,v:=range(mainTsk.SubTaskKeys) {
			if v == subKey {
				finded = true
				if idx + 1 < len(mainTsk.SubTaskKeys) {
					newSubKeys = append(mainTsk.SubTaskKeys[0:idx],mainTsk.SubTaskKeys[idx + 1:]...)
				} else {
					newSubKeys = mainTsk.SubTaskKeys[0:idx]
				}
				break
			}
		}
		if finded {
			newSubKeys = append(newSubKeys, newSubKey)
			mainTsk.SubTaskKeys = newSubKeys
		}
		succ = worker.updateRecordObject(tskKey,mainTsk,mainV,true)
		if !succ {
			fmt.Println("Leader Failed on Updating new SubTask to Main Task on mainKey: " + tskKey + " SubKey: " + subKey)
		} else {
			// 真烦啊，挂了就重启，不想管这么多
			// if the leader dead before the message could send out and new leader update this record will cause problem
			// so actually before update record the leader should also send a prepare Message with its new version
			// the other nodes then save the prepare message
			// if the promise reached the leader update its value and broadCast
			// then if the leader dead and other leader come up, it will first check on the prepare Message he received
			// when the new leader update the record,he should use the higher version on that prepare Message
			// when this one go back online he will now find other people already have the new version of this data and fetch from them
			// the consensus will continue
			// I am not going to write this right now if I have time the strategy will be used on all leader controlled shared value
			fmt.Println("Leader Already Rearrange the" + tskKey + " SubKey: " + subKey + " Publish it among nodes")
			node.startSyncRecord(tskKey)
			node.startSyncRecord(newSubKey)
		}
	} else {
		// ask for task result
		// add this key to superVisedGroup to avoid this request stuck the network
		worker.superVisedSubTsks.Store(subKey,subKey)
	}
}


func(worker* Worker) tskResultAskLoop(){
	// this is for when a leader dead, and new leader come on line,he will keep trace on the old subtasks
	for ;; {
		time.Sleep(time.Duration(2) * time.Second)
		worker.superVisedSubTsks.Range(func(key, value interface{}) bool {
			tsk, _ := worker.getSubTask(key.(string))
			if tsk == nil {
				go worker.node.startSyncRecord(key.(string))
			} else if tsk.Status == "DONE" {
				worker.superVisedSubTsks.Delete(key)
			} else {
				go worker.node.startSyncRecord(key.(string))
			}
			return true
		})
	}
}

func(node* Node) serviceTaskResult(tsKey string) *MainTaskResult {
	mainTsk,_ := node.worker.getMainTask(tsKey)
	if mainTsk == nil {
		return nil
	} else {
		if mainTsk.Status == "DONE" || mainTsk.Status == "DELIVERED" || mainTsk.Status == "DELIVER_FAILED"{
			resKey := mainTsk.ResultHash
			return node.worker.getMainTaskResult(resKey)
		}
	}
	return nil
}

func(node* Node) receiveNewTask(resource[] byte,outputChan chan []byte,isAsync bool) []byte{
	//first let's see how many peers got
	if !node.isLeader() {
		// only Leader Could arrange Task
		return nil
	}

	fmt.Println("New Task Received ")
	hsher := sha256.New()
	byte_pref := []byte(time.Now().String())
	hsher.Write(byte_pref)
	hsher.Write(resource[:1024])
	keyBytes:= hsher.Sum(nil)
	resKey := hex.EncodeToString(keyBytes)
	rec := new(Record)
	rec.Version = 0
	rec.Key = resKey
	rec.Content = resource
	success := node.updateMyRecord(resKey,recordToByte(rec),true)
	if !success {
		fmt.Println("Save Resource Fail")
		return nil
	}
	fmt.Println("New Task Resource Created On Key: " + resKey)
	tskKey,task := node.worker.createTask(resKey,resource)
	if task == nil {
		fmt.Println("Create Main Task Fail")
		return	nil
	}
	fmt.Println("New Task Created On Key: " + tskKey)
	undonMainTask,v := node.worker.getUndonePubTask()
	undonMainTask.UndonTsks[tskKey] = tskKey
	success = node.worker.updateRecordObject(KEY_UNDONE_TASK,undonMainTask,v,true)
	if !success {
		for p := 0;p < 2;p ++ {
			undonMainTask,v = node.worker.getUndonePubTask()
			undonMainTask.UndonTsks[tskKey] = tskKey
			success = node.worker.updateRecordObject(KEY_UNDONE_TASK,undonMainTask,v,true)
			if success {
				break
			}
		}
		if !success {
			fmt.Println("Compete Update Undone Task with other Thread Failed")
			return nil
		}
	}
	fmt.Println("New Task Data Stored to Local, start synchronize among nodes ")
	// syncTask Data To All Nodes
	if outputChan != nil {
		fmt.Println("OutputChan Registed")
		node.registOutputChannel(tskKey,outputChan)
	}
	node.startSyncRecord(resKey)
	for _,v :=range(task.SubTaskKeys) {
		node.startSyncRecord(v)
	}
	node.startSyncRecord(tskKey)
	node.startSyncRecord(KEY_UNDONE_TASK)
	if isAsync {
		fmt.Print("result is " + string(tskKey))
		return []byte(tskKey)
	} else {
		if outputChan == nil {
			return nil
		}
		select {
		case <-time.After(time.Duration(30) * time.Second):
			fmt.Println("OutputChan Timeout")
			close(outputChan)
			node.OutputChannels.Delete(tskKey)
			return []byte("TIME_OUT")
		case result := <-outputChan:
			return result
		}
	}
}

func(worker* Worker) createTask(resKey string,resource[] byte) (string,*Task){
	nodeCounts := worker.node.peers.Count()
	avgByte := len(resource) / nodeCounts
	subTasks := make([]*SubTask,0)
	_,info := worker.node.peers.LoadPeers()
	var idx = 0
	var p1,p2 = 0,avgByte
	for ; p2 < len(resource); p2 += avgByte {
		for ;p2 < len(resource) && !isWordChar(resource[p2]);p2 ++{}
		if idx == len(info) - 1 {
			// the last one
			p2 = len(resource) - 1
		}
		sT := new(SubTask)
		sT.FromByteIdx = uint64(p1)
		sT.ToByteIdx = uint64(p2)
		sT.ResourceHash = resKey
		sT.Status = "NEW"
		sT.ArrangeTo = info[idx].Hash
		subTasks = append(subTasks, sT)
		p1 = p2
		idx ++
	}
	if p1 < p2 && idx <= len(info) - 1{
		sT := new(SubTask)
		sT.FromByteIdx = uint64(p1)
		sT.ToByteIdx = uint64(p2)
		sT.ResourceHash = resKey
		sT.Status = "NEW"
		sT.ArrangeTo = info[idx].Hash
		subTasks = append(subTasks, sT)
	}
	task := new(Task)
	task.Status = "NEW"
	task.ResourceKey = resKey
	strSubTaskKeys := make([]string,0)
	for _,v := range(subTasks) {
		key,err:= GenerateKey(v)
		if err != nil {
			return "",nil
		}
		strSubTaskKeys = append(strSubTaskKeys, key)
	}
	task.SubTaskKeys = strSubTaskKeys
	for idx,sT:=range(subTasks) {
		if !worker.updateRecordObject(strSubTaskKeys[idx],sT,0,true) {
			// update failed
			fmt.Println("Update Str Sub Task Keys Failed")
			return "",nil
		}
	}
	tskKey,err := GenerateKey(task)
	if err != nil {
		fmt.Println("Generate Task Keys Failed")
		return "",nil
	}
	success := worker.updateRecordObject(tskKey,task,0,true)
	if !success {
		fmt.Println("Update Task Failed")
		return "",nil
	}
	return tskKey,task
}