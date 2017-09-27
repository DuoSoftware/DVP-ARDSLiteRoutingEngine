package main

//Configurations

type Configuration struct {
	RedisIp          string
	RedisPort        string
	RedisDb          int
	LocationDb       int
	RedisPassword    string
	Port             string
	RabbitMQIp       string
	RabbitMQPort     string
	RabbitMQUser     string
	RabbitMQPassword string
	AccessToken      string
	UseMsgQueue      bool
	RoutingEngineId  string
	RedisMode        string
	RedisClusterName string
	SentinelHosts    string
	SentinelPort     string
}

type EnvConfiguration struct {
	RedisIp          string
	RedisPort        string
	RedisDb          string
	LocationDb       string
	RedisPassword    string
	Port             string
	RabbitMQIp       string
	RabbitMQPort     string
	RabbitMQUser     string
	RabbitMQPassword string
	AccessToken      string
	UseMsgQueue      string
	RoutingEngineId  string
	RedisMode        string
	RedisClusterName string
	SentinelHosts    string
	SentinelPort     string
}

//Request

type ReqLocationData struct {
	Longitude float32
	Latitude  float32
	Radius    int
	Metric    string
}

type ReqAttributeData struct {
	AttributeCode      []string
	AttributeNames     []string
	AttributeGroupName string
	HandlingType       string
	WeightPrecentage   string
}

type Request struct {
	Company          int
	Tenant           int
	ServerType       string
	RequestType      string
	SessionId        string
	AttributeInfo    []ReqAttributeData
	ArriveTime       string
	Priority         string
	QueueId          string
	ReqHandlingAlgo  string
	ReqSelectionAlgo string
	ServingAlgo      string
	HandlingAlgo     string
	SelectionAlgo    string
	RequestServerUrl string
	CallbackOption   string
	HandlingResource string
	ResourceCount    int
	OtherInfo        string
	LbIp             string
	LbPort           string
}

type RequestSelection struct {
	Company       int
	Tenant        int
	ServerType    string
	RequestType   string
	SessionId     string
	AttributeInfo []ReqAttributeData
	OtherInfo     string
}

type ReqMetaData struct {
	MaxReservedTime  int
	MaxRejectCount   int
	MaxAfterWorkTime int
	MaxFreezeTime    int
}

//Resource

type ResAttributeData struct {
	Attribute    string
	HandlingType string
	Percentage   float64
}

type Resource struct {
	Company               int
	Tenant                int
	Class                 string
	Type                  string
	Category              string
	ResourceId            string
	ResourceAttributeInfo []ResAttributeData
	OtherInfo             string
}

type ResourceStatus struct {
	State  string
	Reason string
	Mode   string
}

type CSlotInfo struct {
	Company            int
	Tenant             int
	HandlingType       string
	State              string
	HandlingRequest    string
	ResourceId         string
	SlotId             int
	ObjKey             string
	SessionId          string
	LastReservedTime   string
	MaxReservedTime    int
	MaxAfterWorkTime   int
	MaxFreezeTime      int
	TempMaxRejectCount int
	OtherInfo          string
}

type ConcurrencyInfo struct {
	Company             int
	Tenant              int
	RejectCount         int
	ResourceId          string
	LastConnectedTime   string
	LastRejectedSession string
	RefInfo             string
}

type WeightBaseResourceInfo struct {
	ResourceId string
	Weight     float64
	LastConnectedTime   string
}

type MultiResCount struct {
	ResourceCount int
}

type updateCsReult struct {
	IsSuccess bool
}

type SelectedResource struct  {
	Priority  []string
	Threshold []string
}

type SelectionResult struct {
	Request string
	Resources SelectedResource
}
