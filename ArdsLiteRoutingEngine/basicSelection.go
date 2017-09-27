package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func BasicSelection(_company, _tenent int, _requests []Request) (result []SelectionResult) {
	//requestKey := fmt.Sprintf("Request:%d:%d:%s", _company, _tenent, _sessionId)
	//fmt.Println(requestKey)
	//
	//strReqObj := RedisGet(requestKey)
	//fmt.Println(strReqObj)
	//
	//var reqObj RequestSelection
	//json.Unmarshal([]byte(strReqObj), &reqObj)

	var selectedResources = make([]SelectionResult, len(_requests))

	for i, reqObj := range _requests {

		selectedResources[i].Request = reqObj.SessionId

		var resourceConcInfo = make([]ConcurrencyInfo, 0)
		var matchingResources = make([]string, 0)
		if len(reqObj.AttributeInfo) > 0 {
			var tagArray = make([]string, 3)

			tagArray[0] = fmt.Sprintf("company_%d", reqObj.Company)
			tagArray[1] = fmt.Sprintf("tenant_%d", reqObj.Tenant)
			tagArray[2] = fmt.Sprintf("objtype_%s", "Resource")

			attInfo := make([]string, 0)

			for _, value := range reqObj.AttributeInfo {
				for _, att := range value.AttributeCode {
					attInfo = AppendIfMissingString(attInfo, att)
				}
			}

			sort.Sort(ByStringValue(attInfo))
			for _, att := range attInfo {
				fmt.Println("attCode", att)
				tagArray = AppendIfMissingString(tagArray, fmt.Sprintf("attribute_%s", att))
			}

			tags := fmt.Sprintf("tag:*%s*", strings.Join(tagArray, "*"))
			fmt.Println(tags)
			val := RedisSearchKeys(tags)
			lenth := len(val)
			fmt.Println(lenth)

			for _, match := range val {
				strResKey := RedisGet(match)
				strResObj := RedisGet(strResKey)
				fmt.Println(strResObj)

				var resObj Resource
				json.Unmarshal([]byte(strResObj), &resObj)

				_attAvailable, _ := IsAttributeAvailable(reqObj.AttributeInfo, resObj.ResourceAttributeInfo, reqObj.RequestType)

				if resObj.ResourceId != "" && _attAvailable {
					concInfo, err := GetConcurrencyInfo(resObj.Company, resObj.Tenant, resObj.ResourceId, reqObj.RequestType)
					if err != nil {
						fmt.Println("Error in GetConcurrencyInfo")
					} else {
						resourceConcInfo = append(resourceConcInfo, concInfo)
					}
					//matchingResources = AppendIfMissing(matchingResources, strResKey)
					//fmt.Println(strResKey)
				}
			}

			sort.Sort(timeSlice(resourceConcInfo))

			for _, res := range resourceConcInfo {
				resKey := fmt.Sprintf("Resource:%d:%d:%s", res.Company, res.Tenant, res.ResourceId)
				matchingResources = AppendIfMissingString(matchingResources, resKey)
				fmt.Println(resKey)
			}

		}
		selectedResources[i].Resources.Priority = matchingResources
	}
	return selectedResources

}
