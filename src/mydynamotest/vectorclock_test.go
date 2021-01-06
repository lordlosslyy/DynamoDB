package mydynamotest

import (
	"mydynamo"
	"testing"
)

func TestBasicVectorClock(t *testing.T) {
	t.Logf("Starting TestBasicVectorClock")
	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	//Test for equality
	if !clock1.Equals(clock2) {
		t.Fail()
		t.Logf("Vector Clocks were not equal")
	}
	
}

func TestAddFirstNode(t *testing.T){
	t.Logf("Starting TestAddFirstNode")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	//Add One Element
	clock1.Increment("A")

	if clock1.GetVersion("A") != 1{
		t.Error(`clock1.GetVersion("A") != 1`)
	}
}

func TestCompareTwoNodeWithSequence(t *testing.T){
	t.Logf("Starting TestCompareTwoNodeWithSequence")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()
	//Add One Element
	clock1.Increment("A")
	clock2.Increment("A")
	if clock1.GetVersion("A") != 1{
		t.Error(`clock1.GetVersion("A") != 1`)
	}

	if clock2.GetVersion("A") !=1{
		t.Error(`clock2.GetVersion("A") != 1`)
	}

	if !clock1.Equals(clock2) {
		t.Error(`clock1.Equals(clock2) = False`)
	}

}

func TestConcurrencyWithTwoNode(t *testing.T){
	t.Logf("Starting TestCompareTwoNodeWithSequence")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()
	//Add One Element
	clock1.Increment("A")
	clock2.Increment("B")
	if clock1.GetVersion("A") != 1{
		t.Error(`clock1.GetVersion("A") != 1`)
	}

	if clock2.GetVersion("B") !=1{
		t.Error(`clock2.GetVersion("B") != 1`)
	}

	if !clock1.Concurrent(clock2) {
		t.Error(`clock1.Concurrent(clock2) = False`)
	}

}

/*func TestConcurrencyWithThreeNodeSequence(t *testing.T){
	t.Logf("Starting TestCompareTwoNodeWithSequence")

	//create three vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()
	clock3 := mydynamo.NewVectorClock()
	//Add One Element
	clock1.Increment("A")
	
	if clock1.GetVersion("A") != 1{
		t.Error(`clock1.GetVersion("A") != 1`)
	}

	clock1.Increment("A")
	if clock1.GetVersion("A") !=2{
		t.Error(`clock1.GetVersion("A") != 2`)
	}

	clock3.Increment("C")
	clockList := make([]mydynamo.VectorClock, 0)
	clockList = append(clockList, clock1)
	clock2.Combine(clockList)

	if !clock1.Equals(clock2) {
		t.Error(`clock1.Equals(clock2) != false`)
	}

	clock3.Increment("C")
	clockList[0] = clock3
	clock2.Combine(clockList)

	if clock2.GetVersion("C") != 2 {
		t.Error(`clock2.GetVersion("C") != 2 `)
	}

	if !clock2.Concurrent(clock1) {
		t.Error(`clock2.Concurrent(clock1) = false `)
	}

	if clock2.LessThan(clock1) {
		t.Error(`clock2.Concurrent(clock1) = false `)
	}

	if !clock1.LessThan(clock2) {
		t.Error(`clock2.Concurrent(clock1) = false `)
	}
}*/