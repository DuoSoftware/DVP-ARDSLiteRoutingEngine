package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
)

func SingleResourceAlgo(ardsLbIp, ardsLbPort, serverType, requestType, sessionId string, selectedResources SelectedResource, reqCompany, reqTenant int, reqBusinessUnit string) (handlingResult, handlingResource string) {
	handlingResult, handlingResource = SingleHandling(ardsLbIp, ardsLbPort, serverType, requestType, sessionId, selectedResources, reqCompany, reqTenant, reqBusinessUnit)
	return

}

func ReserveSlot(ardsLbIp, ardsLbPort string, slotInfo CSlotInfo) bool {
	url := fmt.Sprintf("http://%s/DVP/API/1.0.0.0/ARDS/resource/%s/concurrencyslot", CreateHost(ardsLbIp, ardsLbPort), slotInfo.ResourceId)
	log.Println("URL:>", url)

	slotInfoJson, _ := json.Marshal(slotInfo)
	log.Println("request Data:: ", string(slotInfoJson))
	var jsonStr = []byte(slotInfoJson)
	authToken := fmt.Sprintf("Bearer %s", accessToken)
	internalAuthToken := fmt.Sprintf("%d:%d", slotInfo.Tenant, slotInfo.Company)
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("authorization", authToken)
	req.Header.Set("companyinfo", internalAuthToken)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		//panic(err)
		return false
	}
	defer resp.Body.Close()

	log.Println("response Status:", resp.Status)
	//log.Println("response Headers:", resp.Header)
	body, _ := ioutil.ReadAll(resp.Body)
	result := string(body)
	log.Println("response Body:", result)

	var resConv updateCsReult
	json.Unmarshal(body, &resConv)

	if resConv.IsSuccess == true {
		log.Println("Return true")
		return true
	}

	log.Println("Return false")
	return false
}

func ClearSlotOnMaxRecerved(ardsLbIp, ardsLbPort, serverType, requestType, sessionId string, resObj Resource) {
	var tagArray = make([]string, 8)

	tagArray[0] = fmt.Sprintf("company_%d:", resObj.Company)
	tagArray[1] = fmt.Sprintf("tenant_%d:", resObj.Tenant)
	tagArray[4] = fmt.Sprintf("handlingType_%s:", requestType)
	tagArray[5] = fmt.Sprintf("state_%s:", "Reserved")
	tagArray[6] = fmt.Sprintf("resourceid_%s:", resObj.ResourceId)
	tagArray[7] = fmt.Sprintf("objtype_%s", "CSlotInfo")

	tags := fmt.Sprintf("tag:*%s*", strings.Join(tagArray, "*"))
	//log.Println(tags)
	reservedSlots := RedisSearchKeys(tags)

	for _, tagKey := range reservedSlots {
		strslotKey := RedisGet(tagKey)
		log.Println(strslotKey)

		strslotObj := RedisGet(strslotKey)
		//log.Println(strslotObj)

		var slotObj CSlotInfo
		json.Unmarshal([]byte(strslotObj), &slotObj)

		log.Println("Datetime Info" + slotObj.LastReservedTime)
		t, _ := time.Parse(layout, slotObj.LastReservedTime)
		t1 := int(time.Now().Sub(t).Seconds())
		t2 := slotObj.MaxReservedTime
		log.Println(fmt.Sprintf("Time Info T1: %d", t1))
		log.Println(fmt.Sprintf("Time Info T2: %d", t2))
		if t1 > t2 {
			slotObj.State = "Available"
			slotObj.OtherInfo = "ClearReserved"

			ReserveSlot(ardsLbIp, ardsLbPort, slotObj)
		}
	}
}

func GetReqMetaData(_company, _tenent int, _serverType, _requestType string) (metaObj ReqMetaData, err error) {
	key := fmt.Sprintf("ReqMETA:%d:%d:%s:%s", _company, _tenent, _serverType, _requestType)
	//log.Println(key)
	var strMetaObj string
	strMetaObj, err = RedisGet_v1(key)

	//log.Println(strMetaObj)
	json.Unmarshal([]byte(strMetaObj), &metaObj)

	return
}

func GetResourceState(_company, _tenant int, _resId string) (state string, mode string, err error) {
	key := fmt.Sprintf("ResourceState:%d:%d:%s", _company, _tenant, _resId)
	log.Println(key)
	var strResStateObj string

	strResStateObj, err = RedisGet_v1(key)

	log.Println(strResStateObj)

	var resStatus ResourceStatus
	json.Unmarshal([]byte(strResStateObj), &resStatus)
	state = resStatus.State
	mode = resStatus.Mode
	return
}

func HandlingResources(Company, Tenant int, BusinessUnit string, ResourceCount int, ArdsLbIp, ArdsLbPort, SessionId, ServerType, RequestType, HandlingAlgo, OtherInfo string, selectedResources SelectedResource) (handlingResult string, handlingResource []string) {

	handlingResult = ""
	handlingResource = make([]string, 0)

	switch HandlingAlgo {
	case "SINGLE":
		var singleHandlingResource string
		handlingResult, singleHandlingResource = SingleResourceAlgo(ArdsLbIp, ArdsLbPort, ServerType, RequestType, SessionId, selectedResources, Company, Tenant, BusinessUnit)
		handlingResource = append(handlingResource, singleHandlingResource)
	case "MULTIPLE":
		//log.Println("ReqOtherInfo:", OtherInfo)
		resCount := ResourceCount
		log.Println("GetRequestedResCount:", resCount)
		handlingResult, handlingResource = MultipleHandling(ArdsLbIp, ArdsLbPort, ServerType, RequestType, SessionId, selectedResources, resCount, Company, Tenant, BusinessUnit)
	default:
		handlingResult = ""
		handlingResource = make([]string, 0)
	}

	return
}
