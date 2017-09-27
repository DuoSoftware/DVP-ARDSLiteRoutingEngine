package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

func MultipleHandling(ardsLbIp, ardsLbPort, ServerType, RequestType, sessionId string, selectedResources SelectedResource, nuOfResRequested, reqCompany, reqTenant int) (handlingResult string, handlingResource []string) {
	return SelectMultipleHandlingResource(ardsLbIp, ardsLbPort, ServerType, RequestType, sessionId, selectedResources, nuOfResRequested, reqCompany, reqTenant)
}

func SelectMultipleHandlingResource(ardsLbIp, ardsLbPort, ServerType, RequestType, sessionId string, selectedResources SelectedResource, nuOfResRequested, reqCompany, reqTenant int) (handlingResult string, handlingResource []string) {
	selectedResList := make([]string, 0)
	selectedResKeyList := make([]string, 0)
	resourceIds := append(selectedResources.Priority, selectedResources.Threshold...)
	fmt.Println("///////////////////////////////////////selectedResources/////////////////////////////////////////////////")
	fmt.Println("Priority:: ", selectedResources.Priority)
	fmt.Println("Threshold:: ", selectedResources.Threshold)
	fmt.Println("ResourceIds:: ", resourceIds)
	for _, key := range resourceIds {
		fmt.Println(key)
		strResObj := RedisGet(key)
		fmt.Println(strResObj)

		var resObj Resource
		json.Unmarshal([]byte(strResObj), &resObj)

		conInfo, cErr := GetConcurrencyInfo(resObj.Company, resObj.Tenant, resObj.ResourceId, RequestType)
		if cErr == nil {
			metaData, mErr := GetReqMetaData(reqCompany, reqTenant, ServerType, RequestType)
			if mErr == nil {
				resState, resMode, sErr := GetResourceState(resObj.Company, resObj.Tenant, resObj.ResourceId)
				if sErr == nil {

					if resState == "Available" && resMode == "Inbound" && conInfo.RejectCount < metaData.MaxRejectCount {
						ClearSlotOnMaxRecerved(ardsLbIp, ardsLbPort, ServerType, RequestType, sessionId, resObj)

						var tagArray = make([]string, 8)

						tagArray[0] = fmt.Sprintf("company_%d", resObj.Company)
						tagArray[1] = fmt.Sprintf("tenant_%d", resObj.Tenant)
						tagArray[4] = fmt.Sprintf("handlingType_%s", RequestType)
						tagArray[5] = fmt.Sprintf("state_%s", "Available")
						tagArray[6] = fmt.Sprintf("resourceid_%s", resObj.ResourceId)
						tagArray[7] = fmt.Sprintf("objtype_%s", "CSlotInfo")

						tags := fmt.Sprintf("tag:*%s*", strings.Join(tagArray, "*"))
						fmt.Println(tags)
						availableSlots := RedisSearchKeys(tags)

						for _, tagKey := range availableSlots {
							strslotKey := RedisGet(tagKey)
							fmt.Println(strslotKey)

							strslotObj := RedisGet(strslotKey)
							fmt.Println(strslotObj)

							var slotObj CSlotInfo
							json.Unmarshal([]byte(strslotObj), &slotObj)

							slotObj.State = "Reserved"
							slotObj.SessionId = sessionId
							slotObj.OtherInfo = "Inbound"
							slotObj.MaxReservedTime = metaData.MaxReservedTime
							slotObj.MaxAfterWorkTime = metaData.MaxAfterWorkTime
							slotObj.MaxFreezeTime = metaData.MaxFreezeTime
							slotObj.TempMaxRejectCount = metaData.MaxRejectCount

							if ReserveSlot(ardsLbIp, ardsLbPort, slotObj) == true {
								fmt.Println("Return resource Data:", conInfo.RefInfo)
								selectedResList = AppendIfMissingString(selectedResList, conInfo.RefInfo)
								selectedResKeyList = AppendIfMissingString(selectedResKeyList, key)
								if len(selectedResList) == nuOfResRequested {
									selectedResListString, _ := json.Marshal(selectedResList)
									handlingResult = string(selectedResListString)
									handlingResource = selectedResKeyList
								}
							}
						}
					}
				}
			}
		}

	}
	handlingResult = "No matching resources at the moment"
	handlingResource = make([]string, 0)
	return
}
