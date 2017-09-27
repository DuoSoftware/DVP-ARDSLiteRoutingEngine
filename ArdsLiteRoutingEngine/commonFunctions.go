package main

import (
	"fmt"
	"net"
	"log"
)

func AppendIfMissingReq(dataList []Request, i Request) []Request {
	for _, ele := range dataList {
		if ele.SessionId == i.SessionId {
			return dataList
		}
	}
	return append(dataList, i)
}

func AppendIfMissingString(dataList []string, i string) []string {
	for _, ele := range dataList {
		if ele == i {
			return dataList
		}
	}
	return append(dataList, i)
}


func CheckExistingString(dataList []string, i string) bool {
	for _, ele := range dataList {
		if ele == i {
			return true
		}
	}
	return false
}

func CreateHost(_ip, _port string) string {
	testIp := net.ParseIP(_ip)
	if testIp.To4() == nil {
		return _ip
	} else {
		return fmt.Sprintf("%s:%s", _ip, _port)
	}
}

func GetSelectedResourceForRequest(records []SelectionResult, sessionId string, pickedResources []string) (resourceForRequest SelectedResource, isExisting bool) {
	for _, record := range records {
		if record.Request == sessionId {
			log.Println("AvailableResources: ", record.Resources)

			record.Resources.Priority = DiffArray(pickedResources, record.Resources.Priority)
			record.Resources.Threshold = DiffArray(pickedResources, record.Resources.Threshold)
			resourceForRequest = record.Resources
			isExisting = true
			return
		}
	}
	isExisting = false
	return
}

func DiffArray(a, b []string) []string {
	m := make(map[string]bool)
	for _, s := range a {
		m[s] = true
	}
	result := make([]string, 0)
	for _, s := range b {
		if !m[s] {
			result = append(result, s)
		}
	}
	return result
}
