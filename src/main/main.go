package main

import (
	"distributeWordSplit/src/node"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

func readSomeData(){
	db1,_:=leveldb.OpenFile("/Users/liwei/Desktop/goProject/distributeWordSplit/dataSet1",nil)
	//*node.Task1e3272fe7e9265a8f6480e6963d4e16e583f1ea0f44dddc4ffb562f953bae194
	val1,_:=db1.Get([]byte(node.KEY_UNDONE_TASK),nil)
	db2,_:=leveldb.OpenFile("/Users/liwei/Desktop/goProject/distributeWordSplit/dataSet2",nil)
	val2,_:=db2.Get([]byte(node.KEY_UNDONE_TASK),nil)
	db3,_:=leveldb.OpenFile("/Users/liwei/Desktop/goProject/distributeWordSplit/dataSet3",nil)
	//val3,_:=db3.Get([]byte("*node.Taskb24d8f061e9e106c024911ba9640c7aef12f3701f24d8f6d69105b119b30f487"),nil)


	string1 := string(val1[4:])
	v1 := binary.BigEndian.Uint32(val1[0:4])
	string2 := string(val2[4:])
	v2 := binary.BigEndian.Uint32(val2[0:4])
	//string3 := string(val3[4:])
	//v3 := binary.BigEndian.Uint32(val3[0:4])
	fmt.Println("Version: " + strconv.FormatUint(uint64(v1),10) + " DataSet1 TestKey: " + string1)
	fmt.Println("Version: " + strconv.FormatUint(uint64(v2),10) + " DataSet2 TestKey: " + string2)
	//fmt.Println("Version: " + strconv.FormatUint(uint64(v3),10) + " DataSet3 TestKey: " + string3)
	defer db1.Close();db2.Close();db3.Close()

}

func putSomeData(){
	byteData1 := append([]byte{0,0,0,1},[]byte("I am the dataSet1,Version1")...)
	byteData2 := append([]byte{0,0,0,2},[]byte("I am the dataSet2,Version2")...)
	byteData3 := append([]byte{0,0,0,3},[]byte("I am the dataSet3,Version3")...)
	db1,_:=leveldb.OpenFile("/Users/liwei/Desktop/goProject/distributeWordSplit/dataSet1",nil)
	db1.Put([]byte("TestKey"),byteData1,nil)
	db2,_:=leveldb.OpenFile("/Users/liwei/Desktop/goProject/distributeWordSplit/dataSet2",nil)
	db2.Put([]byte("TestKey"),byteData2,nil)
	db3,_:=leveldb.OpenFile("/Users/liwei/Desktop/goProject/distributeWordSplit/dataSet3",nil)
	db3.Put([]byte("TestKey"),byteData3,nil)
	defer db1.Close();db2.Close();db3.Close()

}
func main() {
	//putSomeData()
	//readSomeData()
	configPath := os.Args[1]
	fmt.Println("Start Node using Configuration File " + configPath)
	//configPath := "/Users/liwei/Desktop/goProject/distributeWordSplit/node3.json"
	dat, err := ioutil.ReadFile(configPath)
	if err != nil{
		fmt.Println(err)
	}
	config := new(node.NodeConfig)
	err = json.Unmarshal(dat,config)
	if err != nil {
		fmt.Println(err)
	}
	nodeInst := node.CreateNode(config)
	if nodeInst == nil {
		return
	}
	if len(config.StarterPeerAddr) > 0 {
		for ; !nodeInst.TestHeartBeatOnAddr(config.StarterPeerAddr); {
			// wait for my starter Peer come up
			fmt.Println("Wait For My Starter Peer Started")
			time.Sleep(time.Duration(2) * time.Second)
		}
	}
	node.StartNode(nodeInst,config.StarterPeerAddr)
	for {
		time.Sleep(time.Duration(300) * time.Second)
	}
}