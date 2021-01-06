package main

import (
	"mydynamo"
	"strconv"
	"log"
	"fmt"
)

const welcomeMsg = "hello"
const errorMsg = "Server may be crashing or your data is conflict with server"
func Print(result *mydynamo.DynamoResult){
	if result == nil || len(result.EntryList) == 0{
		return
	}
	dataEntry := result.EntryList
	fmt.Printf("---------Print Value With Result---------\n")
	for _,d := range dataEntry {
		fmt.Printf("vector clock: %s value: %s\n", d.Context.Clock.GetClockString(), d.Value)
	}
	fmt.Printf("---------End of Print---------\n")
}

func convertToClock(result *mydynamo.DynamoResult) []mydynamo.VectorClock{
	clockList := make([]mydynamo.VectorClock, 0)

	for _, item := range result.EntryList{
		clockList = append(clockList, item.Context.Clock)
	}
	return clockList
}
func main() {
	//Spin up a client for some testing
	//Expects server to be started, starting at port 8080
	serverPort := 8080
	fmt.Println("Please Type The Port [defalut 8080]: ")
	fmt.Scanf("%d", &serverPort)
	//this client connects to the server on port 8080
	clientInstance := mydynamo.NewDynamoRPCClient("localhost:" + strconv.Itoa(serverPort+0))
	clientInstance.RpcConnect()
	log.Println("RPC Client start")

	isComplete := false
	var returnArgus *mydynamo.DynamoResult
	var previousKey string
	command := ""
	for {
		fmt.Println("Please Type The Command:")
		fmt.Println("[P] Put New Data To Server")
		fmt.Println("[U] Update The Previous Data To Server")
		fmt.Println("[G] Get Data To Server")
		fmt.Println("[C] Server Crash")
		fmt.Println("[O] Gossip")
		fmt.Println("[E] End of Program")
		fmt.Scanf("%s", &command)
		var key, value string
		switch command {
			case "E":
				isComplete = true
			case "P":
				fmt.Println("Please provide KEY VALUE:")
				fmt.Scanf("%s %s", &key, &value)
				argus := mydynamo.NewPutArgsWithClock(key,"", []byte(value))
				success := clientInstance.Put(argus)
				if !success{
					log.Println(errorMsg)
				}else{
					log.Println("Put Data Successfully")
				}
			case "U":
				if returnArgus == nil{
					fmt.Println("No Previous Get Data!")
					break
				}
				fmt.Println("Previous Key:")
				fmt.Println(previousKey)
				fmt.Println("Previous Data:")
				Print(returnArgus)
				fmt.Println("Your Update Value:")
				fmt.Scanf("%s",&value)
				clockList := convertToClock(returnArgus)
				newContext := mydynamo.NewContext(mydynamo.NewVectorClock())
				newContext.Clock.Combine(clockList)
				argus :=mydynamo.NewPutArgs(previousKey, newContext, []byte(value))
				success := clientInstance.Put(argus)
				if !success{
					log.Println(errorMsg)
				}
			case "G":
				fmt.Println("Please provide KEY:")
				fmt.Scanf("%s", &key)
				previousKey = key
				returnArgus = clientInstance.Get(key)
				Print(returnArgus)
			case "C":
				var sec = 0
				fmt.Println("How long do you want to crash the server? (int)")
				fmt.Scanf("%d",&sec)
				clientInstance.Crash(sec)
			case "O":
				clientInstance.Gossip()
			default:
				log.Println("Invalid Input! Please type something again")
		}
		if isComplete {
			break
		}
		command = ""
	}
	/*//Put the first element
	argus := mydynamo.NewPutArgsWithClock("c00","", bytes.NewBufferString("hello").Bytes())
	success := clientInstance.Put(argus)
	if !success{
		log.Println(errorMsg)
	}
	returnArgus := clientInstance.Get("c00")
	Print(returnArgus)
	
	//Put the second element
	argus = mydynamo.NewPutArgs("c00", returnArgus.EntryList[0].Context, []byte("okok"))
	success = clientInstance.Put(argus)
	if !success{
		log.Println(errorMsg)
	}
	returnArgus = clientInstance.Get("c00")
	Print(returnArgus)

	argus = mydynamo.NewPutArgsWithClock("c01","", bytes.NewBufferString("new Data").Bytes())
	success = clientInstance.Put(argus)
	if !success{
		log.Println(errorMsg)
	}
	returnArgus = clientInstance.Get("c01")
	Print(returnArgus)

	//clientInstance.Crash(15)*/
	clientInstance.CleanConn()
	//You can use the space below to write some operations that you want your client to do
}

