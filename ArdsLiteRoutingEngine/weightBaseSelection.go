package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func CalculateWeight(reqAttributeInfo []ReqAttributeData, resAttributeInfo []ResAttributeData) float64 {
	var calculatedWeight float64
	calculatedWeight = 0.00
	for _, reqAtt := range reqAttributeInfo {
		if len(reqAtt.AttributeCode) > 0 {
			attCode := reqAtt.AttributeCode[0]

			for _, resAtt := range resAttributeInfo {
				if attCode == resAtt.Attribute && resAtt.HandlingType == reqAtt.HandlingType {

					reqAttPrecentage, _ := strconv.ParseFloat(reqAtt.WeightPrecentage, 64)
					fmt.Println("**********reqAttPrecentage:", reqAttPrecentage)
					fmt.Println("**********resAttPrecentage:", resAtt.Percentage)
					reqWeight := reqAttPrecentage / 100.00
					resAttWeight := resAtt.Percentage / 100.00
					calculatedWeight = calculatedWeight + (reqWeight * resAttWeight)
				}
			}
		}
	}
	return calculatedWeight
}

func WeightBaseSelection(_company, _tenent int, _sessionId string) (result SelectionResult) {
	requestKey := fmt.Sprintf("Request:%d:%d:%s", _company, _tenent, _sessionId)
	fmt.Println(requestKey)

	strReqObj := RedisGet(requestKey)
	fmt.Println(strReqObj)

	var reqObj RequestSelection
	json.Unmarshal([]byte(strReqObj), &reqObj)

	var resourceWeightInfo = make([]WeightBaseResourceInfo, 0)
	var matchingResources = make([]string, 0)
	if len(reqObj.AttributeInfo) > 0 {
		var tagArray = make([]string, 3)

		tagArray[0] = fmt.Sprintf("company_%d", reqObj.Company)
		tagArray[1] = fmt.Sprintf("tenant_%d", reqObj.Tenant)
		//tagArray[2] = fmt.Sprintf("class_%s", reqObj.Class)
		//tagArray[3] = fmt.Sprintf("type_%s", reqObj.Type)
		//tagArray[4] = fmt.Sprintf("category_%s", reqObj.Category)
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

			if resObj.ResourceId != "" {
				concInfo, err := GetConcurrencyInfo(resObj.Company, resObj.Tenant, resObj.ResourceId, reqObj.RequestType)
				if err != nil {
					fmt.Println("Error in GetConcurrencyInfo")
				}
				calcWeight := CalculateWeight(reqObj.AttributeInfo, resObj.ResourceAttributeInfo)
				resKey := fmt.Sprintf("Resource:%d:%d:%s", resObj.Company, resObj.Tenant, resObj.ResourceId)
				var tempWeightInfo WeightBaseResourceInfo
				tempWeightInfo.ResourceId = resKey
				tempWeightInfo.Weight = calcWeight
				tempWeightInfo.LastConnectedTime=concInfo.LastConnectedTime

				resourceWeightInfo = append(resourceWeightInfo, tempWeightInfo)
			}

		}

		//sort.Sort(ByNumericValue(resourceWeightInfo))
		sort.Sort(ByWaitingTime(resourceWeightInfo))

		for _, res := range resourceWeightInfo {
			matchingResources = AppendIfMissingString(matchingResources, res.ResourceId)
			logWeight := fmt.Sprintf("###################################### %s --------- %f", res.ResourceId, res.Weight)
			fmt.Println(logWeight)
		}


	}
	result.Priority = matchingResources
	return
}