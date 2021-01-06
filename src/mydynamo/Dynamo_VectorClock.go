package mydynamo
import(
	"strings"
	"fmt"
	"strconv"
)

//VectorClock 
type VectorClock struct {
	//todo
	ClockData map[string] int
}

//NewVectorClock 
// No delete any function here
//Creates a new VectorClock
func NewVectorClock() VectorClock {
	newClock := make(map[string]int)
	return VectorClock{ClockData: newClock}
}

// isExist
func (s *VectorClock) IsExist(nodeID string) bool{
	_, ok := s.ClockData[nodeID]
	return ok
}

// isExist
func (s *VectorClock) GetClockString() string{

	var sb strings.Builder
	sb.WriteString("{")
	for k, v := range s.ClockData{
		item := fmt.Sprintf("(%s,%s)", k, strconv.Itoa(v))
		sb.WriteString(item)
	}
	sb.WriteString("}")
	result := sb.String()

	return result
}
//Returns true if the other VectorClock is causally descended from this one
func (s *VectorClock) LessThan(otherClock VectorClock) bool {
	if len(otherClock.ClockData) < len(s.ClockData) || s.Equals(otherClock) {
		return false
	}

	for k,v := range s.ClockData {
		//log.Println("Check:"+k)
		//log.Println("Version:"+strconv.Itoa(v))
		if !otherClock.IsExist(k) || v > otherClock.ClockData[k]{
			return false
		}
	}

	return true
}

//Returns true if neither VectorClock is causally descended from the other
func (s *VectorClock) Concurrent(otherClock VectorClock) bool {
	if s.Equals(otherClock){
		return false
	}

	return !(otherClock.LessThan(*s) || s.LessThan(otherClock))
	/*tempClockData := otherClock.ClockData
	for k,v := range s.ClockData {
		val, ok := tempClockData[k]
		if ok && val > v{
			return true
		}else if ok{
			delete(tempClockData, k)
		}
	}*/

	//return len(tempClockData) != 0 
}

//Given the provided ID, return the version number
func (s *VectorClock) GetVersion(nodeId string) int {
	v, ok := s.ClockData[nodeId]
	if !ok{
		return 0
	}
	return v
}

//Increments this VectorClock at the element associated with nodeId
func (s *VectorClock) Increment(nodeId string) {
	if !s.IsExist(nodeId) {
		s.ClockData[nodeId] = 1
		return 
	}
	s.ClockData[nodeId]+=1
}

//Changes this VectorClock to be causally descended from all VectorClocks in clocks
func (s *VectorClock) Combine(clocks []VectorClock) {
	if clocks == nil{
		return 
	}

	newClockData := make(map[string] int)

	for _, clock := range clocks{
		for k,v := range clock.ClockData{
			currVer, isExist := newClockData[k]
			if !isExist || v > currVer{
				newClockData[k] = v
			}
		} 
	}

	for k, v := range s.ClockData{
		currVer, isExist := newClockData[k]
		if !isExist || v > currVer {
			newClockData[k] = v
		}
	}

	s.ClockData = newClockData
	return
}

//Tests if two VectorClocks are equal
func (s *VectorClock) Equals(otherClock VectorClock) bool {
	// check whether the length is equal
	if len(s.ClockData) != len(otherClock.ClockData) {
		return false
	}
	// traverse the key one by one and compare the value with otherClock value
	for k,v := range s.ClockData{
		if !otherClock.IsExist(k) {
			return false
		}

		if otherClock.ClockData[k] != v {
			return false
		}
	}
	return true
}
