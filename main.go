package main

import (
	"fmt"
	"os"
	"strings"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

// type LnsImtTopicRewrite struct {
// 	ApplicationName string `json:"applicationName"`
// 	NodeName        string `json:"nodeName"`
// 	DevEUI          string `json:"devEUI"`
// }

func main() {
	// var litr LnsImtTopicRewrite

	id := uuid.New().String()
	var sbMqttSubClientId strings.Builder
	var sbMqttPubClientId strings.Builder
	var sbPubTopic strings.Builder
	sbMqttSubClientId.WriteString("mqtt-topic-rewrite-lns-imt-")
	sbMqttSubClientId.WriteString(id)
	sbMqttPubClientId.WriteString("mqtt-topic-rewrite-lns-imt-")
	sbMqttPubClientId.WriteString(id)

	mqttSubBroker := "mqtt://networkserver.maua.br:1883"
	mqttSubClientId := sbMqttSubClientId.String()
	mqttSubUser := "PUBLIC"
	mqttSubPassword := "public"
	mqttSubQos := 0

	mqttSubOpts := MQTT.NewClientOptions()
	mqttSubOpts.AddBroker(mqttSubBroker)
	mqttSubOpts.SetClientID(mqttSubClientId)
	mqttSubOpts.SetUsername(mqttSubUser)
	mqttSubOpts.SetPassword(mqttSubPassword)

	mqttSubTopics := map[string]byte{
		"application/6/node/+/rx":  byte(mqttSubQos),
		"application/9/node/+/rx":  byte(mqttSubQos),
		"application/13/node/+/rx": byte(mqttSubQos),
		"application/18/node/+/rx": byte(mqttSubQos),
		"application/19/node/+/rx": byte(mqttSubQos),
		"application/20/node/+/rx": byte(mqttSubQos),
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

	// SubscribeMultiple(filters map[string]byte, callback MessageHandler) Token
	if token := mqttSubClient.SubscribeMultiple(mqttSubTopics, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	for {
		incoming := <-c
		s := strings.Split(incoming[0], "/")
		var measurement string
		switch s[1] {
		case "6":
			measurement = "SmartLight"
		case "9":
			measurement = "EnergyMeter"
		case "13":
			measurement = "WeatherStation"
		case "18":
			measurement = "WaterTankLevel"
		case "19":
			measurement = "GaugePressure"
		case "20":
			measurement = "Hydrometer"
		}
		deviceId := s[3]

		sbPubTopic.Reset()
		sbPubTopic.WriteString("OpenDataTelemetry/IMT/LNS/")
		sbPubTopic.WriteString(measurement)
		sbPubTopic.WriteString("/")
		sbPubTopic.WriteString(deviceId)
		sbPubTopic.WriteString("/up/imt")
		// fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
		token := pClient.Publish(sbPubTopic.String(), byte(mqttPubQos), false, incoming[1])
		token.Wait()

	}
}
