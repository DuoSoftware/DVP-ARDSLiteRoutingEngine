// ArdsRoutingEngine project main.go
package main

import (
	"fmt"
	"github.com/DuoSoftware/gorest"
	"github.com/satori/go.uuid"
	"net/http"
	"time"
)

const layout = "2006-01-02T15:04:05Z07:00"

func main() {
	fmt.Println("Starting Ards Route Engine")
	InitiateRedis()
	go InitiateService()

	if useMsgQueue {
		//-------------------Amqp Based Routing---------------------------------------
		for {
			Worker()
			fmt.Println("End Worker()")
			time.Sleep(2 * time.Second)
		}
	} else {
		//-------------------RedisDb Based Routing---------------------------------------
		for {

			pubChannelName := fmt.Sprintf("RoutingChannel:%s", routingEngineId)
			if RoutingEngineDistribution(pubChannelName) == pubChannelName {
				availablePHashes := GetAllProcessingHashes()
				if len(availablePHashes) > 0 {
					for _, h := range availablePHashes {
						u1 := uuid.NewV4()
						if AcquireProcessingHashLock(h, u1.String()) == true {
							go ExecuteRequestHash(h, u1.String())
						}
					}
				} else {
					fmt.Println("No Processing Hash Found...")
				}
			}
			time.Sleep(1 * time.Second)
		}
	}

}

func InitiateService() {
	listeningPort := fmt.Sprintf(":%s", port)
	gorest.RegisterService(new(ArdsLiteRS))
	http.Handle("/", gorest.Handle())
	http.ListenAndServe(listeningPort, nil)
}
