package main

import (
	"encoding/json"
	"fmt"
)

func IsAttributeAvailable(reqAttributeInfo []ReqAttributeData, resAttributeInfo []ResAttributeData, reqType string) (isAttrAvailable, isThreshold bool) {

	isAttrAvailable = true
	isThreshold = false

	reqAttributeAvailability := make(map[string]bool)
	reqAttributes := make(map[string]string)

	for _, reqAtt := range reqAttributeInfo {
		if len(reqAtt.AttributeCode) > 0 {
			attCode := reqAtt.AttributeCode[0]
			reqAttributeAvailability[attCode] = false
			reqAttributes[attCode] = attCode
		}
	}

	for _, resAtt := range resAttributeInfo {
		if resAtt.Attribute == reqAttributes[resAtt.Attribute] && resAtt.HandlingType == reqType && !reqAttributeAvailability[resAtt.Attribute] {
			if resAtt.Percentage > 0 {
				reqAttributeAvailability[resAtt.Attribute] = true

				if resAtt.Percentage > 0 && resAtt.Percentage <= 25 {
					isThreshold = true
				}
			}
		}
	}

	fmt.Println("Check Attribute Availability:: ", reqAttributeAvailability)

	for _, availability := range reqAttributeAvailability {
		isAttrAvailable = isAttrAvailable && availability
	}

	fmt.Println("Check Attribute Availability Return:: isAttrAvailable: ", isAttrAvailable, " isThreshold: ", isThreshold)

	return

	/*for _, reqAtt := range reqAttributeInfo {
		if len(reqAtt.AttributeCode) > 0 {
			attCode := reqAtt.AttributeCode[0]

			for _, resAtt := range resAttributeInfo {
				if attCode == resAtt.Attribute && resAtt.HandlingType == reqAtt.HandlingType {
					if resAtt.Percentage > 0 {
						isAttrAvailable = true

						if resAtt.Percentage > 0 && resAtt.Percentage <= 25 {
							isThreshold = true
						}
					} else {
						isAttrAvailable = false
					}

					return
				}
			}
		}
	}*/
}

func GetConcurrencyInfo(_company, _tenant int, _resId, _category string) (ciObj ConcurrencyInfo, err error) {
	key := fmt.Sprintf("ConcurrencyInfo:%d:%d:%s:%s", _company, _tenant, _resId, _category)
	fmt.Println(key)
	var strCiObj string
	strCiObj, err = RedisGet_v1(key)
	fmt.Println(strCiObj)

	json.Unmarshal([]byte(strCiObj), &ciObj)

	return
}

func SelectResources(_company, _tenant int, _requests []Request, _selectionAlgo string) []SelectionResult {
	var selectionResult []SelectionResult

	switch _selectionAlgo {
	case "BASIC":
		selectionResult = BasicSelection(_company, _tenant, _requests)
		break
	case "BASICTHRESHOLD":
		selectionResult = BasicThresholdSelection(_company, _tenant, _requests)
		break
	case "WEIGHTBASE":
		selectionResult = WeightBaseSelection(_company, _tenant, _requests)
		break
	case "LOCATIONBASE":
		selectionResult = LocationBaseSelection(_company, _tenant, _requests)
		break
	default:
		break
	}

	return selectionResult
}
