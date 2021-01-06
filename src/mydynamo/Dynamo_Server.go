package mydynamo

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"fmt"
	"time"
	"errors"
	"sync"
)

var writtenMapMutex sync.Mutex

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int          //Number of nodes to write to on each Put
	rValue         int          //Number of nodes to read from on each Get
	preferenceList []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode   //This node's address and port info
	nodeID         string       //ID of this node
	/*----------storage---------------*/
	storage		   Storage
	isCrash        bool
	writtenMap     map[DynamoNode] bool 
}


func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	s.preferenceList = incomingList
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	if s.isCrash {
		msg := fmt.Sprintf("Server %s is crashing.....", s.nodeID)
		return errors.New(msg)
	}

	for _, v := range s.preferenceList{
		_, isExist := s.writtenMap[v]
		if v == s.selfNode || isExist {
			continue
		}
		client := NewDynamoRPCClient(v.Address+":"+v.Port)
		err := client.RpcConnect()

		if err != nil{
			log.Println("Error")
		}

		localData := s.storage.GetAllDataWithArgs()
		for _, p := range localData {
			result := client.PutInternal(p)
			if !result{
				log.Println("Server Crash or Put old Context")
			}
		}
		client.CleanConn()
	}

	writtenMapMutex.Lock()
	s.writtenMap = make(map[DynamoNode] bool)
	writtenMapMutex.Unlock()

	return nil
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	go func(d int){
		timer := time.NewTimer(time.Duration(d) * time.Second)
		s.isCrash = true
		fmt.Printf("[Channel] Server %s Crash Start\n", s.nodeID)
		<-timer.C
		s.isCrash = false
		fmt.Printf("[Channel] Server %s Crash End\n", s.nodeID)
	}(seconds)
	*success = true
	return nil
}

func (s *DynamoServer) PutInternal(value PutArgs, result *bool) error {
	if s.isCrash {
		*result = false
		msg := fmt.Sprintf("Server %s is crashing.....", s.nodeID)
		return errors.New(msg)
	}
	
	obj := ObjectEntry{Context: value.Context, Value: value.Value}
	*result = s.storage.PutReplicatedData(value.Key, obj)
	
	writtenMapMutex.Lock()
	s.writtenMap = make(map[DynamoNode] bool)
	writtenMapMutex.Unlock()

	return nil
}

// Put a file to this server and W other servers
func (s *DynamoServer) Put(value PutArgs, result *bool) error {

	if s.isCrash {
		*result = false
		msg := fmt.Sprintf("Server %s is crashing.....", s.nodeID)
		return errors.New(msg)
	}
	
	value.Context.Clock.Increment(s.nodeID)
	obj := ObjectEntry{Context: value.Context, Value: value.Value}
	//log.Println("data "+obj.Context.Clock.GetClockString()+" "+string(obj.Value))
	//log.Println("nodeID:"+s.nodeID)
	// Check if the key is in the storage or not
	if !s.storage.IsKeyExist(value.Key) {
		//log.Println("[Put] New Data")
		// Put new key into the storage with the data
		*result = s.storage.PutNewData(s.nodeID, value.Key, obj)
		
	}else{
		// Retrive the different data with key
		*result = s.storage.PutData(s.nodeID, value.Key, obj)
		if *result{
			//log.Println("[Put] Update Data finish")
		}else{
			//log.Println("[Put] Conflict or something error")
		}
	}
	
	// Wait to implement send something to other w-1 nodes
	copyArg :=  NewPutArgs(value.Key, value.Context, value.Value)
	counter := 0
	for _,v := range s.preferenceList{
		if counter == s.wValue -1 {
			break
		}
		if v == s.selfNode {
			continue
		}

		client := NewDynamoRPCClient(v.Address+":"+v.Port)
		err := client.RpcConnect()
		if err != nil{
			log.Println("Server: "+v.Address+":"+v.Port+" Fail Connection ")
			continue
		}

		otherRes := client.PutInternal(copyArg)
		if otherRes {
			counter +=1
			writtenMapMutex.Lock()
			s.writtenMap[v] = true
			writtenMapMutex.Unlock()
		}
		client.CleanConn()
	}
	if counter != s.wValue-1{
		*result = false
	}
	return nil
}
func (s *DynamoServer) GetInternal(key string, result *DynamoResult) error{
	if s.isCrash {
		msg := fmt.Sprintf("Server %s is crashing.....", s.nodeID)
		return errors.New(msg)
	}

	if !s.storage.IsKeyExist(key){
		*result = DynamoResult{}
	} else {
		data := s.storage.GetData(key)
		*result = DynamoResult{EntryList: data}
	}
	//Print(s,key)
	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	if s.isCrash {
		msg := fmt.Sprintf("Server %s is crashing.....", s.nodeID)
		return errors.New(msg)
	}

	data := DynamoResult{}

	if s.storage.IsKeyExist(key){
		for _, object := range s.storage.GetData(key) {
			data.EntryList = append(data.EntryList, object)
		}
	} 
	
	//Print(s,key)
	//Need to read the data from other node later
	counter := 0
	for _,v := range s.preferenceList{
		if counter == s.rValue -1 {
			break
		}
		if v == s.selfNode {
			continue
		}
		client := NewDynamoRPCClient(v.Address+":"+v.Port)
		err := client.RpcConnect()
		if err != nil{
			log.Println("Server: "+v.Address+":"+v.Port+" Fail Connection ")
			continue
		}
		otherRes := client.GetInternal(key)
		if otherRes == nil{
			log.Println("Server: "+v.Address+":"+v.Port+" Crash or Get Infomation fail")
			continue
		}
		l := len(data.EntryList)
		for _, o := range otherRes.EntryList{
			isConcurrent := true
			for i, d := range data.EntryList{
				if d.Context.Clock.Concurrent(o.Context.Clock) {
					continue
				} else {
					isConcurrent = false
				}
				if o.Context.Clock.LessThan(d.Context.Clock) {	
					continue
				}
				data.EntryList[i] = o
				
				if i == l {
					break
				} 
			}
			if isConcurrent {
				data.EntryList = append(data.EntryList, o)
			}
		}
		counter += 1
		
		client.CleanConn()
	}
	if len(data.EntryList) == 0 {
		*result = DynamoResult{}
	} else {
		*result = data
	}
	
	return nil
}

// [Debug Usage] Print the context clock with a given key
func Print(server *DynamoServer,key string){
	dataEntry := server.storage.GetData(key)
	fmt.Printf("---------Print Value---------\n")
	fmt.Printf("Key: %s\n", key)
	for _,d := range dataEntry {
		fmt.Printf("%s\n", d.Context.Clock.GetClockString())
	}
	fmt.Printf("---------End of Print---------\n")
}

/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
	preferenceList := make([]DynamoNode, 0)
	writtenMap := make(map[DynamoNode] bool)
	selfNodeInfo := DynamoNode{
		Address: hostAddr,
		Port:    hostPort,
	}
	return DynamoServer{
		wValue:         w,
		rValue:         r,
		preferenceList: preferenceList,
		selfNode:       selfNodeInfo,
		nodeID:         id,
		storage:		NewStorage(),
		isCrash:		false,
		writtenMap:	    writtenMap,
	}
}

func ServeDynamoServer(dynamoServer DynamoServer) error {
	rpcServer := rpc.NewServer()
	e := rpcServer.RegisterName("MyDynamo", &dynamoServer)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

	l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	log.Println(DYNAMO_SERVER, "Serving Server Now")

	return http.Serve(l, rpcServer)
}
