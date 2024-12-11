package main

import (
	"fmt"
	"os"
	"strings"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

func connLostHandler(c MQTT.Client, err error) {
	fmt.Printf("Connection lost, reason: %v\n", err)
	os.Exit(1)
}

func main() {
	id := uuid.New().String()
	var sbMqttSubClientId strings.Builder
	var sbMqttPubClientId strings.Builder
	var sbPubTopic strings.Builder
	sbMqttSubClientId.WriteString("mqtt-topic-rewrite-lns-chirpstackv4-")
	sbMqttSubClientId.WriteString(id)
	sbMqttPubClientId.WriteString("mqtt-topic-rewrite-lns-chirpstackv4-")
	sbMqttPubClientId.WriteString(id)

	mqttSubBroker := "mqtt://networkserver2.maua.br:1883"
	mqttSubClientId := sbMqttSubClientId.String()
	mqttSubUser := ""
	mqttSubPassword := ""
	mqttSubQos := 0

	mqttSubOpts := MQTT.NewClientOptions()
	mqttSubOpts.AddBroker(mqttSubBroker)
	mqttSubOpts.SetClientID(mqttSubClientId)
	mqttSubOpts.SetUsername(mqttSubUser)
	mqttSubOpts.SetPassword(mqttSubPassword)
	mqttSubOpts.SetConnectionLostHandler(connLostHandler)

	mqttSubTopics := map[string]byte{
		"application/a7d603f2-3de4-4516-82f5-3323a3a80467/device/+/event/up": byte(mqttSubQos),
	}

	mqttPubBroker := "mqtt://mqtt.maua.br:1883"
	mqttPubClientId := sbMqttPubClientId.String()
	mqttPubUser := "public"
	mqttPubPassword := "public"
	mqttPubQos := 0

	mqttPubOpts := MQTT.NewClientOptions()
	mqttPubOpts.AddBroker(mqttPubBroker)
	mqttPubOpts.SetClientID(mqttPubClientId)
	mqttPubOpts.SetUsername(mqttPubUser)
	mqttPubOpts.SetPassword(mqttPubPassword)

	c := make(chan [2]string)

	mqttSubOpts.SetDefaultPublishHandler(func(mqttSubClient MQTT.Client, msg MQTT.Message) {
		c <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	mqttSubClient := MQTT.NewClient(mqttSubOpts)
	if token := mqttSubClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", mqttSubBroker)
	}

	pClient := MQTT.NewClient(mqttPubOpts)
	if token := pClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", mqttPubBroker)
	}

	if token := mqttSubClient.SubscribeMultiple(mqttSubTopics, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	for {
		incoming := <-c
		s := strings.Split(incoming[0], "/")
		var measurement string
		switch s[1] {
		case "a7d603f2-3de4-4516-82f5-3323a3a80467":
			measurement = "WeatherStation"
		}
		deviceId := s[3]

		sbPubTopic.Reset()
		sbPubTopic.WriteString("OpenDataTelemetry/IMT/LNS/")
		sbPubTopic.WriteString(measurement)
		sbPubTopic.WriteString("/")
		sbPubTopic.WriteString(deviceId)
		sbPubTopic.WriteString("/up/chirpstackv4")
		// fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
		token := pClient.Publish(sbPubTopic.String(), byte(mqttPubQos), false, incoming[1])
		token.Wait()

	}
}
