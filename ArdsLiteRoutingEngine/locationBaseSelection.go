package main

import (
	"encoding/json"
	"fmt"
)

func LocationBaseSelection(_company, _tenent int, _requests []Request) (result []SelectionResult) {
	fmt.Println("-----------Start Location base----------------")


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

		var matchingResources = make([]string, 0)
		if reqObj.OtherInfo != "" {

			var locationObj ReqLocationData
			json.Unmarshal([]byte(reqObj.OtherInfo), &locationObj)

			fmt.Println("reqOtherInfo:: ", locationObj)

			if locationObj != (ReqLocationData{}) {
				fmt.Println("Start Get locations")
				locationResult := RedisGeoRadius(_tenent, _company, locationObj)
				fmt.Println("locations:: ", locationResult)

				subReplys, _ := locationResult.Array()
				for _, lor := range subReplys {

					resourceLocInfo, _ := lor.List()

					if len(resourceLocInfo) > 1 {
						issMapKey := fmt.Sprintf("ResourceIssMap:%d:%d:%s", _company, _tenent, resourceLocInfo[0])
						fmt.Println("start map iss: ", issMapKey)
						resourceKey := RedisGet(issMapKey)
						fmt.Println("resourceKey: ", resourceKey)
						if resourceKey != "" {

							strResObj := RedisGet(resourceKey)
							fmt.Println(strResObj)

							var resObj Resource
							json.Unmarshal([]byte(strResObj), &resObj)

							if resObj.ResourceId != "" {
								resKey := fmt.Sprintf("Resource:%d:%d:%s", resObj.Company, resObj.Tenant, resObj.ResourceId)
								if len(reqObj.AttributeInfo) > 0 {
									_attAvailable, _ := IsAttributeAvailable(reqObj.AttributeInfo, resObj.ResourceAttributeInfo, reqObj.RequestType)
									if _attAvailable {
										matchingResources = AppendIfMissingString(matchingResources, resKey)
									}
								} else {
									matchingResources = AppendIfMissingString(matchingResources, resKey)
								}
							}
						}
					}
				}

				selectedResources[i].Resources.Priority = matchingResources
			}

		}
	}

	return selectedResources

}
