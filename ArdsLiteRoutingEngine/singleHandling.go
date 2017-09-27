package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

func SingleHandling(ardsLbIp, ardsLbPort, serverType, requestType, sessionId string, selectedResources SelectedResource, reqCompany, reqTenant int) (handlingResult, handlingResource string) {
	return SelectHandlingResource(ardsLbIp, ardsLbPort, serverType, requestType, sessionId, selectedResources, reqCompany, reqTenant)
}

func SelectHandlingResource(ardsLbIp, ardsLbPort, serverType, requestType, sessionId string, selectedResources SelectedResource, reqCompany, reqTenant int) (handlingResult, handlingResource string) {
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

		fmt.Println("Start GetConcurrencyInfo")
		conInfo, cErr := GetConcurrencyInfo(resObj.Company, resObj.Tenant, resObj.ResourceId, requestType)
		fmt.Println("End GetConcurrencyInfo")
		fmt.Println("Start GetReqMetaData")
		metaData, mErr := GetReqMetaData(reqCompany, reqTenant, serverType, requestType)
		fmt.Println("End GetReqMetaData")
		fmt.Println("Start GetResourceState")
		resState, resMode, sErr := GetResourceState(resObj.Company, resObj.Tenant, resObj.ResourceId)
		fmt.Println("Start GetResourceState")

		fmt.Println("conInfo.RejectCount:: ", conInfo.RejectCount)
		fmt.Println("metaData.MaxRejectCount:: ", metaData.MaxRejectCount)

		if cErr == nil {

			if mErr == nil {

				if sErr == nil {

					if resState == "Available" && resMode == "Inbound" && conInfo.RejectCount < metaData.MaxRejectCount {
						fmt.Println("===========================================Start====================================================")
						ClearSlotOnMaxRecerved(ardsLbIp, ardsLbPort, serverType, requestType, sessionId, resObj)

						var tagArray = make([]string, 8)

						tagArray[0] = fmt.Sprintf("company_%d", resObj.Company)
						tagArray[1] = fmt.Sprintf("tenant_%d", resObj.Tenant)
						tagArray[4] = fmt.Sprintf("handlingType_%s", requestType)
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
								fmt.Println("Return resource Data:", resObj.OtherInfo)
								handlingResult = conInfo.RefInfo
								handlingResource = key
								return
							}
						}
					}
				}
			}
		}

	}
	handlingResult = "No matching resources at the moment"
	handlingResource = ""
	return
}
