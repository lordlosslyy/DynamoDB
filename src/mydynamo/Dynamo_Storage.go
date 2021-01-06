package mydynamo
import(
	"log"
	"sync"
)

var storageMutex sync.Mutex

type Storage struct{
	dataMap		   map[string] []ObjectEntry
}
func NewStorage() Storage{
	storageMap := make(map[string] []ObjectEntry)
	s := Storage{dataMap:storageMap}
	return s
}

func (s *Storage) GetAllDataWithArgs() []PutArgs {
	storageMutex.Lock()
	argsList := make([]PutArgs, 0, len(s.dataMap))
	for k, entry := range s.dataMap{
		key := k
		for _, item := range entry{
			currArgs := NewPutArgs(key, item.Context, item.Value)
			argsList = append(argsList,currArgs)
		}
	}
	storageMutex.Unlock()
	return argsList
}
func (s *Storage) IsKeyExist(key string) bool {
	storageMutex.Lock()
	_, isExist := s.dataMap[key]
	storageMutex.Unlock()
	return isExist
}

func (s *Storage) GetData(key string) []ObjectEntry{
	storageMutex.Lock()
	re := s.dataMap[key]
	storageMutex.Unlock()
	return re
}

func (s *Storage) PutReplicatedData(key string, object ObjectEntry) bool{
	storageMutex.Lock()
	datas, _:= s.dataMap[key]
	storageMutex.Unlock()
	isUpdated := false
	for i,d := range datas{

		if d.Context.Clock.Concurrent(object.Context.Clock){
			continue
		}else if d.Context.Clock.LessThan(object.Context.Clock){
			datas[i] = object
			isUpdated = true
		}else{
			return false
		}

	}
	//if all concurrent append the data
	if !isUpdated {
		datas = append(datas, object)
	}
	storageMutex.Lock()
	s.dataMap[key] = datas
	storageMutex.Unlock()
	return true
}
/*func (s *Storage) PutData(nodeID string,key string, object ObjectEntry) bool{
	datas, _ := s.dataMap[key]
	clockList := make([]VectorClock, 0)
	if len(datas) == 1 {
		if datas[0].Context.Clock.LessThan(object.Context.Clock) && !datas[0].Context.Clock.Equals(object.Context.Clock){
			datas[0] = object
			s.dataMap[key] = datas
			log.Println(object.Context.Clock.GetClockString())
			return true
		}else{
			log.Println("update fail")
			return false
		}
	}
	for _, d := range datas {
		clockList = append(clockList, d.Context.Clock)
	}
	object.Context.Clock.Combine(clockList)
	log.Println(object.Context.Clock.GetClockString())
	s.dataMap[key] = [] ObjectEntry{object}
	return true
}*/
func (s *Storage) PutData(nodeID string,key string, object ObjectEntry) bool{
	storageMutex.Lock()
	datas, _ := s.dataMap[key]
	storageMutex.Unlock()
	newDatas := make([]ObjectEntry, 0)
	isConcurrent :=false
	for _,d := range datas {
		//log.Println(d.Context.Clock.GetClockString())
		if d.Context.Clock.Concurrent(object.Context.Clock){
			newDatas = append(newDatas, d)
			isConcurrent = true
		}else if d.Context.Clock.LessThan(object.Context.Clock){
			continue
		}else{
			newDatas = append(newDatas, d)
		}
	}
	if len(newDatas) == len(datas) && !isConcurrent{
		return false
	}
	log.Println(object.Context.Clock.GetClockString())
	newDatas = append(newDatas, object)
	storageMutex.Lock()
	s.dataMap[key] = newDatas
	storageMutex.Unlock()
	return true
}

func (s *Storage) PutNewData(nodeID string, key string, object ObjectEntry) bool{
	newObjs := make([]ObjectEntry, 1)
	newObjs[0] = object
	storageMutex.Lock()
	s.dataMap[key] = newObjs
	storageMutex.Unlock()
	return true
}