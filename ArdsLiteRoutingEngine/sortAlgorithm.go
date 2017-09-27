package main

import (
	"strconv"
	"time"
)

type timeSliceReq []Request
type ByStringValue []string
type timeSlice []ConcurrencyInfo
type ByNumericValue []WeightBaseResourceInfo
type ByReqPriority []Request
type ByWaitingTime []WeightBaseResourceInfo

func (p timeSliceReq) Len() int {
	return len(p)
}
func (p timeSliceReq) Less(i, j int) bool {
	t1, _ := time.Parse(layout, p[i].ArriveTime)
	t2, _ := time.Parse(layout, p[j].ArriveTime)
	return t1.Before(t2)
}
func (p timeSliceReq) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (a ByStringValue) Len() int           { return len(a) }
func (a ByStringValue) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByStringValue) Less(i, j int) bool { return a[i] < a[j] }

func (p timeSlice) Len() int {
	return len(p)
}
func (p timeSlice) Less(i, j int) bool {
	t1, _ := time.Parse(layout, p[i].LastConnectedTime)
	t2, _ := time.Parse(layout, p[j].LastConnectedTime)
	return t1.Before(t2)
}
func (p timeSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (a ByNumericValue) Len() int      { return len(a) }
func (a ByNumericValue) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByNumericValue) Less(i, j int) bool {
	w1 := a[i].Weight
	w2 := a[j].Weight
	return w1 > w2
}

func (p ByReqPriority) Len() int {
	return len(p)
}
func (p ByReqPriority) Less(i, j int) bool {
	prio1, _ := strconv.Atoi(p[i].Priority)
	prio2, _ := strconv.Atoi(p[j].Priority)
	if prio1 > prio2 {
		return true
	}else if prio1 == prio2{
		t1, _ := time.Parse(layout, p[i].ArriveTime)
		t2, _ := time.Parse(layout, p[j].ArriveTime)
		return t1.Before(t2)
	}else {
		return false
	}
}
func (p ByReqPriority) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (a ByWaitingTime) Len() int      { return len(a) }
func (a ByWaitingTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByWaitingTime) Less(i, j int) bool {
	layout := "2006-01-02T15:04:05.000Z"
	t1,_:=time.Parse(layout, a[i].LastConnectedTime)
	t2,_:=time.Parse(layout, a[j].LastConnectedTime)

	if(a[i].Weight==a[j].Weight){
		w1 := time.Since(t1).Seconds()
		w2 := time.Since(t2).Seconds()
		return w1 > w2
	}else{
		w1 := a[i].Weight
		w2 := a[j].Weight
		return w1 > w2
	}

}
