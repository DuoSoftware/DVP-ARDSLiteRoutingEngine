package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

func SingleHandling(ardsLbIp, ardsLbPort, serverType, requestType, sessionId string, selectedResources SelectedResource, reqCompany, reqTenant int, reqBusinessUnit string) (handlingResult, handlingResource string) {
	return SelectHandlingResource(ardsLbIp, ardsLbPort, serverType, requestType, sessionId, selectedResources, reqCompany, reqTenant, reqBusinessUnit)
}

func SelectHandlingResource(ardsLbIp, ardsLbPort, serverType, requestType, sessionId string, selectedResources SelectedResource, reqCompany, reqTenant int, reqBusinessUnit string) (handlingResult, handlingResource string) {
	resourceIds := append(selectedResources.Priority, selectedResources.Threshold...)
	log.Println("///////////////////////////////////////selectedResources/////////////////////////////////////////////////")
	log.Println("Priority:: ", selectedResources.Priority)
	log.Println("Threshold:: ", selectedResources.Threshold)
	log.Println("ResourceIds:: ", resourceIds)
	for _, key := range resourceIds {
		log.Println(key)
		strResObj := RedisGet(key)
		//log.Println(strResObj)

		var resObj Resource
		json.Unmarshal([]byte(strResObj), &resObj)

		//log.Println("Start GetConcurrencyInfo")
		conInfo, cErr := GetConcurrencyInfo(resObj.Company, resObj.Tenant, resObj.ResourceId, requestType)
		//log.Println("End GetConcurrencyInfo")
		//log.Println("Start GetReqMetaData")
		metaData, mErr := GetReqMetaData(reqCompany, reqTenant, serverType, requestType)
		//log.Println("End GetReqMetaData")
		//log.Println("Start GetResourceState")
		resState, resMode, sErr := GetResourceState(resObj.Company, resObj.Tenant, resObj.ResourceId)
		//log.Println("Start GetResourceState")

		log.Println("conInfo.RejectCount:: ", conInfo.RejectCount)
		log.Println("conInfo.IsRejectCountExceeded:: ", conInfo.IsRejectCountExceeded)
		log.Println("metaData.MaxRejectCount:: ", metaData.MaxRejectCount)

		if cErr == nil {

			if mErr == nil {

				if sErr == nil {

					if resState == "Available" && resMode == "Inbound" && conInfo.RejectCount < metaData.MaxRejectCount && conInfo.IsRejectCountExceeded == false {
						log.Println("===========================================Start====================================================")
						ClearSlotOnMaxRecerved(ardsLbIp, ardsLbPort, serverType, requestType, sessionId, resObj)

						var tagArray = make([]string, 8)

						tagArray[0] = fmt.Sprintf("company_%d:", resObj.Company)
						tagArray[1] = fmt.Sprintf("tenant_%d:", resObj.Tenant)
						tagArray[4] = fmt.Sprintf("handlingType_%s:", requestType)
						tagArray[5] = fmt.Sprintf("state_%s:", "Available")
						tagArray[6] = fmt.Sprintf("resourceid_%s:", resObj.ResourceId)
						tagArray[7] = fmt.Sprintf("objtype_%s", "CSlotInfo")

						tags := fmt.Sprintf("tag:*%s*", strings.Join(tagArray, "*"))
						//log.Println(tags)
						availableSlots := RedisSearchKeys(tags)

						for _, tagKey := range availableSlots {
							strslotKey := RedisGet(tagKey)
							//log.Println(strslotKey)

							strslotObj := RedisGet(strslotKey)
							//log.Println(strslotObj)

							var slotObj CSlotInfo
							json.Unmarshal([]byte(strslotObj), &slotObj)

							slotObj.State = "Reserved"
							slotObj.SessionId = sessionId
							slotObj.OtherInfo = "Inbound"
							slotObj.MaxReservedTime = metaData.MaxReservedTime
							slotObj.MaxAfterWorkTime = metaData.MaxAfterWorkTime
							slotObj.MaxFreezeTime = metaData.MaxFreezeTime
							slotObj.TempMaxRejectCount = metaData.MaxRejectCount
							slotObj.BusinessUnit = reqBusinessUnit

							if ReserveSlot(ardsLbIp, ardsLbPort, slotObj) == true {
								log.Println("Return resource Data:", resObj.OtherInfo)
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
