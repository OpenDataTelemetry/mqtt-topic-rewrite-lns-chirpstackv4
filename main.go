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
		"application/deb35cab-8a9a-42a9-b19e-0cd2ac859cc8/device/+/event/up": byte(mqttSubQos), // DET
		"application/cf909d7a-a970-4473-9ef3-2c0618e1eb63/device/+/event/up": byte(mqttSubQos), // Emma
		"application/33d3fb39-c249-4f8d-b105-2706af00bf5c/device/+/event/up": byte(mqttSubQos), // EnergyMeter
		"application/3522bea0-3ecc-43dd-a676-824d2efc9a5a/device/+/event/up": byte(mqttSubQos), // GaugePressure
		"application/c31059f6-2a9a-49c5-80e5-c912829f433a/device/+/event/up": byte(mqttSubQos), // Hydrometer
		"application/815dcf59-4f43-45c7-9b0a-e264db48232d/device/+/event/up": byte(mqttSubQos), // MauaSat
		"application/4ae0c733-e9b5-482f-8542-3d08f8e6d077/device/+/event/up": byte(mqttSubQos), // SmartLight
		"application/25e85005-adc9-48d6-89e1-b4f677cf18ef/device/+/event/up": byte(mqttSubQos), // WaterTankLevel
		"application/a7d603f2-3de4-4516-82f5-3323a3a80467/device/+/event/up": byte(mqttSubQos), // WeatherStation
		"application/e2cbf2fb-fb26-4608-aacc-66115c0521c0/device/+/event/up": byte(mqttSubQos), // 3LevelSoilDepthMoisture
		"application/5239fc35-6b28-4908-89fa-4efa9bf0636e/device/+/event/up": byte(mqttSubQos), // Sprinkler
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
		// application/APPLICATION_ID/device/DEVICE_ID/command/down
		var measurement string

		switch s[1] {
		case "deb35cab-8a9a-42a9-b19e-0cd2ac859cc8":
			measurement = "DET"

		case "cf909d7a-a970-4473-9ef3-2c0618e1eb63":
			measurement = "Emma"

		case "33d3fb39-c249-4f8d-b105-2706af00bf5c":
			measurement = "EnergyMeter"

		case "3522bea0-3ecc-43dd-a676-824d2efc9a5a":
			measurement = "GaugePressure"

		case "c31059f6-2a9a-49c5-80e5-c912829f433a":
			measurement = "Hydrometer"

		case "815dcf59-4f43-45c7-9b0a-e264db48232d":
			measurement = "MauaSat"

		case "4ae0c733-e9b5-482f-8542-3d08f8e6d077":
			measurement = "SmartLight"

		case "25e85005-adc9-48d6-89e1-b4f677cf18ef":
			measurement = "WaterTankLevel"

		case "a7d603f2-3de4-4516-82f5-3323a3a80467":
			measurement = "WeatherStation"

		case "e2cbf2fb-fb26-4608-aacc-66115c0521c0":
			measurement = "3LevelSoilDepthMoisture"

		case "5239fc35-6b28-4908-89fa-4efa9bf0636e":
			measurement = "Sprinkler"
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
