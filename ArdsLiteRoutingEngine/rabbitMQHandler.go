package main

import (
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"log"
)

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Println("%s: %s", msg, err)
	}
}

func amqpDial() (*amqp.Connection, error) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in amqpDial", r)
		}
	}()

	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", rabbitMQUser, rabbitMQPassword, rabbitMQIp, rabbitMQPort)
	conn, err := amqp.Dial(url)
	failOnError(err, "Failed to connect to RabbitMQ")
	return conn, err
}

func Worker() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in RabbitMQ Worker", r)
		}
	}()

	conn, err := amqpDial()
	defer conn.Close()
	if err != nil {
		return
	} else {
		ch, err := conn.Channel()
		failOnError(err, "Failed to open a channel")
		defer ch.Close()

		q, err := ch.QueueDeclare(
			"ARDS.Workers.Queue", // name
			true,                 // durable
			false,                // delete when unused
			false,                // exclusive
			false,                // no-wait
			nil,                  // arguments
		)
		failOnError(err, "Failed to declare a queue")
		err = ch.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		failOnError(err, "Failed to set QoS")

		msgs, err := ch.Consume(
			q.Name, // queue
			"",     // consumer
			false,  // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		failOnError(err, "Failed to register a consumer")

		forever := make(chan bool)
		//cont := new(bool)
		//*cont = true

		go func() {
			fmt.Printf("closing: %s", <-conn.NotifyClose(make(chan *amqp.Error)))
			//*cont = false
			forever <- true
		}()
		//go func() {
		//	fmt.Println("Start New")
		//	for *cont {
		//		fmt.Println("cont..")
		//		time.Sleep(2 * time.Second)
		//	}
		//}()

		go func() {
			fmt.Println("Start New msgs")
			for d := range msgs {
				log.Printf("Received a message: %s", d.Body)
				d.Ack(false)
				hashKey := string(d.Body)
				u1 := uuid.NewV4()
				if AcquireProcessingHashLock(hashKey, u1.String()) == true {
					go ExecuteRequestHashWithMsgQueue(hashKey, u1.String())
				}
				//dot_count := bytes.Count(d.Body, []byte("."))
				//t := time.Duration(dot_count)
				//time.Sleep(t * time.Second)
				log.Printf("Done")
			}
			fmt.Println("End msgs")
		}()

		log.Printf(" Routing Engine Waiting for requests. To exit press CTRL+C")
		<-forever
	}
}
