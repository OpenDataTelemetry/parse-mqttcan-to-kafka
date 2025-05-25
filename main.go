package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"

	// "log"
	"math"
	"os"
	"strconv"
	"strings"

	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

//	type CanId1 struct {
//		EngineOilTemperature float64 `json:"engineOilTemperature"`
//		EngineOilPressure    float64 `json:"engineOilPressure"`
//	}

// type KafkaTopic struct {
// 	Organization string
// 	Bucket       string
// }

// type MqttSub struct {
// 	Topic string
// 	Msg   string
// 	// Msg   InputMsg
// 	// Tags   InputTags
// 	// Fields InputFields
// }

// type InputMsg struct {
// 	Data string
// }

type Input struct {
	Tags   InputTags
	Fields InputFields
}

type InputTags struct {
	// Tags map[string]interface{} `json:"tags"`
	Organization string
	DeviceType   string
	Measurement  string
	DeviceId     string
	Direction    string
	Etc          string
}

type InputFields struct {
	Data map[string]interface{} `json:"data"` // Timestamp uint64      `json:"timestamp"` // mandatory
}

type InputMsgDataCan struct {
	CanId   string `json:"canId"`   // mandatory
	CanData string `json:"canData"` // mandatory
}

type Output struct {
	Name      string                 `json:"name"`
	Fields    map[string]interface{} `json:"fields"`
	Tags      map[string]interface{} `json:"tags"`
	Timestamp uint64                 `json:"timestamp"`
}

type CarDynamics struct {
	GroundSpeed   float64 `json:"groundSpeed"`
	GLateral      float64 `json:"gLateral"`
	GLongitudinal float64 `json:"gLongitudinal"`
	BrakePressure float64 `json:"brakePressure"`
}

type GPSLongitude struct {
	GPSLongitude float64 `json:"gpsLongitude"`
}

type CarEngine struct {
	EngineRPM     float64 `json:"engineRPM"`
	Gear          float64 `json:"gear"`
	ThrottlePos   float64 `json:"ThrottlePos"`
	SteeringAngle float64 `json:"steeringAngle"`
}

type GPSLatitude struct {
	GPSLatitude float64 `json:"gpsLatitude"`
}

type GPSOthers struct {
	GPSAltitude float64 `json:"gpsAltitude"`
	GPSHeading  float64 `json:"gpsHeading"`
	GPSSpeed    float64 `json:"gpsSpeed"`
	GPSSatsUsed float64 `json:"gpsSatsUsed"`
}

type CarCanData struct {
	EngineOilTemperature float64 `json:"engineOilTemperature"`
	EngineOilPressure    float64 `json:"engineOilPressure"`
	GroundSpeed          float64 `json:"groundSpeed"`
	GLateral             float64 `json:"gLateral"`
	GLongitudinal        float64 `json:"gLongitudinal"`
	BrakePressure        float64 `json:"brakePressure"`
	EngineRPM            float64 `json:"engineRPM"`
	Gear                 float64 `json:"gear"`
	ThrottlePos          float64 `json:"throttlePosition"`
	SteeringAngle        float64 `json:"steeringAngle"`
	GPSAltitude          float64 `json:"gpsAltitude"`
	GPSHeading           float64 `json:"gpsHeading"`
	GPSSpeed             float64 `json:"gpsSpeed"`
	GPSSatsUsed          float64 `json:"gpsSatsUsed"`
	GPSLatitude          float64 `json:"gpsLatitude"`
	GPSLongitude         float64 `json:"gpsLongitude"`
}

type ICCan struct {
	EngineOilTemperature float64 `json:"engineOilTemperature"`
	EngineOilPressure    float64 `json:"engineOilPressure"`
	Timestamp            int64   `json:"timestamp"`
}

type EVCan struct {
	SoC              string `json:"soC"`
	HighVoltageLevel string `json:"highVoltageLevel"`
	Timestamp        int64  `json:"timestamp"`
}

type H2Can struct {
	SoC       string `json:"soC"`
	H2Level   string `json:"h2Level"`
	Timestamp int64  `json:"timestamp"`
}

type CarUp struct {
	Id        string `json:"id"`
	Data      string `json:"data"`
	Timestamp int64  `json:"timestamp"`
}

type CarInertias struct {
	AccX      float64 `json:"accX"` // :"944.000000"
	AccY      float64 `json:"accY"` // :"-356.000000"
	AccZ      float64 `json:"accZ"` // :"-16960.000000"
	GyrX      float64 `json:"gyrX"` // :"-491.000000"
	GyrY      float64 `json:"gyrY"` // :"15.000000"
	GyrZ      float64 `json:"gyrZ"` // :"196.000000"
	MagX      float64 `json:"magX"` // :"196.000000"
	MagY      float64 `json:"magY"` // :"196.000000"
	MagZ      float64 `json:"magZ"` // :"196.000000"
	Timestamp int64   `json:"timestamp"`
}

type CarTracking struct {
	Latitude  string `json:"latitude"`  // :"0.000000"
	Longitude string `json:"longitude"` // :"0.000000"
	Altitude  string `json:"altitude"`  // :"0.000000"
	Timestamp int64  `json:"timestamp"`
}

func roundFloat(val float64, precision uint) float64 {
	ratio := math.Pow(10, float64(precision))
	return math.Round(val*ratio) / ratio
}

// CONVERT B64 to BYTE
func b64ToByte(b64 string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		log.Fatal(err)
	}
	return b, err
}

func protocolParserCanDataByCanId(canId string, canData []byte) string {
	var carCanData CarCanData
	i := 0

	switch canId {
	case "500":
		if len(canData) >= i+8 {
			v := int64(canData[i])<<56 |
				int64(canData[i+1])<<48 |
				int64(canData[i+2])<<40 |
				int64(canData[i+3])<<32 |
				int64(canData[i+4])<<24 |
				int64(canData[i+5])<<16 |
				int64(canData[i+6])<<8 |
				int64(canData[i+7])

			carCanData.GPSLatitude = (float64(v) - math.Pow(2, 63)) / 1e15
			i += 8
		}

	case "501":
		if len(canData) >= i+8 {
			v := int64(canData[i])<<56 |
				int64(canData[i+1])<<48 |
				int64(canData[i+2])<<40 |
				int64(canData[i+3])<<32 |
				int64(canData[i+4])<<24 |
				int64(canData[i+5])<<16 |
				int64(canData[i+6])<<8 |
				int64(canData[i+7])

			carCanData.GPSLongitude = (float64(v) - math.Pow(2, 63)) / 1e15
			i += 8
		}

	case "502":
		if len(canData) >= i+8 {
			v := uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.GPSAltitude = float64(v) - 32768
			i += 2

			v = uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.GPSHeading = (float64(v) - 32768) / 100
			i += 2

			v = uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.GPSSpeed = float64(v)
			i += 2

			v = uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.GPSSatsUsed = float64(v)
			i += 2
		}

	case "503":
		if len(canData) >= i+8 {
			v := uint64(canData[i+1])<<8 | uint64(canData[i+1])
			carCanData.GroundSpeed = float64(v) / 10
			i += 2

			v = uint64(canData[i+1])<<8 | uint64(canData[i+1])
			carCanData.GLateral = (float64(v) - 32768) / 100
			i += 2

			v = uint64(canData[i+1])<<8 | uint64(canData[i+1])
			carCanData.GLongitudinal = (float64(v) - 32768) / 100
			i += 2

			v = uint64(canData[i+1])<<8 | uint64(canData[i+1])
			carCanData.BrakePressure = float64(v) / 10
			i += 2
		}

	case "504":
		if len(canData) >= i+8 {
			v := uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.EngineRPM = float64(v)
			i += 2

			v = uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.Gear = float64(v)
			i += 2

			v = uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.ThrottlePos = float64(v) / 100
			i += 2

			v = uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.SteeringAngle = (float64(v) - 32768) / 100
			i += 2
		}

	}

	p, err := json.Marshal(carCanData)
	if err != nil {
		fmt.Println(err)
		return "Can data parsed wrongly"
	}
	return string(p[:])
}

func parseMeasurementByDeviceType(deviceType string, measurement string, inputMsgData []byte) (map[string]interface{}, map[string]interface{}) {
	// Car, CAN | Tracking, 001,1122334455667788
	// MEASUREMENTS: Tracking, Inertias, CanIC, CanEV, CanH2
	// CanIC -> 0X01, 0X02, 0X03
	// CanEV -> 0X11, 0X12, 0X13
	// CanH2 -> 0X21, 0X22, 0X23

	var sb strings.Builder
	var output Output

	switch deviceType {
	case "Car":
		switch measurement {
		case "Inertias":
			var carInertias CarInertias
			accX := carInertias.AccX
			accY := carInertias.AccY
			accZ := carInertias.AccZ
			gyrX := carInertias.GyrX
			gyrY := carInertias.GyrY
			gyrZ := carInertias.GyrZ
			magX := carInertias.MagX
			magY := carInertias.MagY
			magZ := carInertias.MagZ

			sb.WriteString(` `)
			sb.WriteString(`,accX=`)
			sb.WriteString(strconv.FormatFloat(accX, 'f', -1, 64))
			sb.WriteString(`,accY=`)
			sb.WriteString(strconv.FormatFloat(accY, 'f', -1, 64))
			sb.WriteString(`,accZ=`)
			sb.WriteString(strconv.FormatFloat(accZ, 'f', -1, 64))
			sb.WriteString(`,gyrX=`)
			sb.WriteString(strconv.FormatFloat(gyrX, 'f', -1, 64))
			sb.WriteString(`,gyrY=`)
			sb.WriteString(strconv.FormatFloat(gyrY, 'f', -1, 64))
			sb.WriteString(`,gyrZ=`)
			sb.WriteString(strconv.FormatFloat(gyrZ, 'f', -1, 64))
			sb.WriteString(`,magX`)
			sb.WriteString(strconv.FormatFloat(magX, 'f', -1, 64))
			sb.WriteString(`,magY`)
			sb.WriteString(strconv.FormatFloat(magY, 'f', -1, 64))
			sb.WriteString(`,magZ`)
			sb.WriteString(strconv.FormatFloat(magZ, 'f', -1, 64))
		case "Tracking":
			var carTracking CarTracking
			latitude := carTracking.Latitude
			longitude := carTracking.Longitude
			altitude := carTracking.Altitude

			sb.WriteString(` `)
			sb.WriteString(`latitude=`)
			sb.WriteString(latitude)
			sb.WriteString(`,longitude=`)
			sb.WriteString(longitude)
			sb.WriteString(`,altitude=`)
			sb.WriteString(altitude)

		case "Can":
			var inputMsgDataCan InputMsgDataCan
			err := json.Unmarshal(inputMsgData, &inputMsgDataCan)
			if err != nil {
				log.Fatalf("Error unmarshaling JSON: %v", err)
			}

			canId := inputMsgDataCan.CanId
			canData := inputMsgDataCan.CanData

			// B64 to Byte
			b, err := b64ToByte(canData)
			if err != nil {
				// fmt.Print(data)
				log.Panic(err)
			}

			var carCanData CarCanData
			d := protocolParserCanDataByCanId(canId, b)
			json.Unmarshal([]byte(d), &carCanData)

			// if canId == "500" {
			// 	var gpsLatitude GPSLatitude
			// 	gpsLatitude.GPSLatitude = carCanData.GPSLatitude

			// 	// tags := map[string]interface{}{
			// 	// 	"deviceType": deviceType,
			// 	// 	"canId":      canId,
			// 	// 	"message":    "CarDynamics",
			// 	// }

			// 	tags = {
			// 		"deviceType": deviceType,
			// 		"canId":      canId,
			// 		"message":    "CarDynamics",
			// 	}
			// 	fields := map[string]interface{}{
			// 		"data": canData,
			// 	}

			// 	output.Tags = tags
			// 	// output. = fields

			// 	p, err := json.Marshal(output)
			// 	if err != nil {
			// 		fmt.Println(err)
			// 		return "Can data parsed wrongly"
			// 	}
			// 	return string(p[:])
			// }
			// if canId == "501" {
			// 	var gpsLongitude GPSLongitude
			// 	gpsLongitude.GPSLongitude = carCanData.GPSLongitude
			// 	tags := map[string]interface{}{
			// 		"deviceType": deviceType,
			// 		"canId":      canId,
			// 		"message":    "CarDynamics",
			// 	}
			// 	fields := map[string]interface{}{
			// 		"data": canData,
			// 	}

			// 	// var outputMsg OutputMsg
			// 	output.Tags = tags
			// 	output.Fields = fields

			// 	p, err := json.Marshal(outputMsg)
			// 	if err != nil {
			// 		fmt.Println(err)
			// 		return "Can data parsed wrongly"
			// 	}
			// 	return string(p[:])
			// }
			// if canId == "502" {
			// 	var gpsOthers GPSOthers
			// 	gpsOthers.GPSAltitude = carCanData.GPSAltitude
			// 	gpsOthers.GPSHeading = carCanData.GPSHeading
			// 	gpsOthers.GPSSpeed = carCanData.GPSSpeed
			// 	gpsOthers.GPSSatsUsed = carCanData.GPSSatsUsed

			// 	tags := map[string]interface{}{
			// 		"deviceType": deviceType,
			// 		"canId":      canId,
			// 		"message":    "CarDynamics",
			// 	}
			// 	fields := map[string]interface{}{
			// 		"data": canData,
			// 	}

			// 	output.Tags = tags
			// 	output.Fields = fields

			// 	p, err := json.Marshal(output)
			// 	if err != nil {
			// 		fmt.Println(err)
			// 		return "Can data parsed wrongly"
			// 	}
			// 	return string(p[:])
			// }
			// if canId == "503" {
			// 	var carDynamics CarDynamics
			// 	carDynamics.BrakePressure = carCanData.BrakePressure
			// 	carDynamics.GLateral = carCanData.GLateral
			// 	carDynamics.GLongitudinal = carCanData.GLongitudinal
			// 	carDynamics.GroundSpeed = carCanData.GroundSpeed

			// 	tags := map[string]interface{}{
			// 		"deviceType": deviceType,
			// 		"canId":      canId,
			// 		"message":    "CarDynamics",
			// 	}
			// 	fields := map[string]interface{}{
			// 		"data": canData,
			// 	}

			// 	var outputMsg OutputMsg
			// 	outputMsg.Tags = tags
			// 	outputMsg.Fields = fields

			// 	p, err := json.Marshal(outputMsg)
			// 	if err != nil {
			// 		fmt.Println(err)
			// 		return "Can data parsed wrongly"
			// 	}
			// 	return string(p[:])
			// }
			if canId == "504" {
				var carEngine CarEngine
				carEngine.EngineRPM = carCanData.EngineRPM
				carEngine.Gear = carCanData.Gear
				carEngine.ThrottlePos = carCanData.ThrottlePos
				carEngine.SteeringAngle = carCanData.SteeringAngle

				tags := map[string]interface{}{
					"deviceType": deviceType,
					"canId":      canId,
					"message":    "CarDynamics",
				}
				fields := map[string]interface{}{
					"data":          canData,
					"engineRPM":     carEngine.EngineRPM,
					"gear":          carEngine.Gear,
					"throttlePos":   carEngine.ThrottlePos,
					"steeringAngle": carEngine.SteeringAngle,
				}

				output.Tags = tags
				output.Fields = fields
			}

		}
	case "RaceTrack":
		switch measurement {
		case "SkidPad":
			// var raceTrackSkidPad RaceTrackSkidPad
			// json.Unmarshal([]byte(data), &raceTrackSkidPad)
		}
	}
	return output.Tags, output.Fields
}

func mergeKeysAndValues(keys1, keys2 map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{})
	for k, v := range keys1 {
		merged[k] = v
	}
	for k, v := range keys2 {
		merged[k] = v
	}
	return merged
}

// func parseCar(input Input) OutputMsg {
func parseInputIntoOutput(input Input) Output {
	var tags, parsedTags, parsedFields map[string]interface{}
	var output Output

	// Marshal InputMsg.Data interface{} to Json string represented in bytes
	inputMsgData, _ := json.Marshal(input.Fields.Data)

	// if message == "" {
	// 	return "No message to parse"
	// }

	if input.Tags.Direction == "up" {
		tags = map[string]interface{}{
			"deviceType": input.Tags.DeviceType,
			"deviceId":   input.Tags.DeviceId,
			"direction":  input.Tags.Direction,
			"origin":     input.Tags.Etc,
		}

		parsedTags, parsedFields = parseMeasurementByDeviceType(input.Tags.DeviceType, input.Tags.Measurement, inputMsgData)

	}

	fmt.Printf("\n@@@@@@inputTags %v", input.Tags)
	fmt.Printf("\n@@@@@@inputFields: %v", input.Fields)
	fmt.Printf("\n@@@@@@inputMsgData: %v", inputMsgData)
	// fmt.Printf("\n@@@@@@outputMsg0.Timestamp: %v", outputMsg0.Timestamp)
	fmt.Printf("\n")
	fmt.Printf("\n@@@@@@parsedTags: %v", parsedTags)
	fmt.Printf("\n@@@@@@parsedFields: %v", parsedFields)

	mergedTags := mergeKeysAndValues(tags, parsedTags)

	// Timestamp_ns
	now := time.Now()      // current local time
	nsec := now.UnixNano() // number of nanoseconds since January 1, 1970 UTC

	output.Name = input.Tags.Measurement
	output.Tags = mergedTags
	output.Fields = parsedFields
	output.Timestamp = uint64(nsec)

	return output
}

func connLostHandler(c MQTT.Client, err error) {
	fmt.Printf("Connection lost, reason: %v\n", err)
	os.Exit(1)
}

func marshalToJson(msg Output) string {
	outputMsgJson, _ := json.Marshal(msg)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// }
	return string(outputMsgJson[:])
}

func marshalToInflux(msg Output) string {
	var sb strings.Builder
	sb.WriteString(msg.Name)
	sb.WriteString(",")
	sb.WriteString(mapToCommaString(msg.Tags))
	sb.WriteString(" ")
	sb.WriteString(mapToCommaString(msg.Fields))
	sb.WriteString(" ")
	sb.WriteString(strconv.FormatUint(uint64(msg.Timestamp), 10))

	return string(sb.String())
}

func mapToCommaString(m map[string]interface{}) string {
	var sb strings.Builder
	for k, v := range m {
		sb.WriteString(",")
		sb.WriteString(k)
		sb.WriteString("=")
		switch v.(type) {
		case string:
			sb.WriteString(v.(string))
		case float64:
			sb.WriteString(strconv.FormatFloat(v.(float64), 'f', -1, 64))
		case uint64:
			sb.WriteString(strconv.FormatUint(v.(uint64), 10))
		}

	}
	return strings.Replace(sb.String(), ",", "", 1)
}

func main() {

	id := uuid.New().String()
	MQTT_BROKER_SUB := os.Getenv("MQTT_BROKER_SUB")
	MQTT_BROKER_PUB := os.Getenv("MQTT_BROKER_PUB")
	KAFKA_BROKER_PROD := os.Getenv("KAFKA_BROKER")

	// MqttSubscriberClient
	var sbMqttSubClientId strings.Builder
	sbMqttSubClientId.WriteString("parse-lns-sub-")
	sbMqttSubClientId.WriteString(id)

	// MqttSubscriberTopic
	var sbMqttSubTopic strings.Builder
	sbMqttSubTopic.WriteString("OpenDataTelemetry/#")

	// DO PUB STUFFS
	var sbMqttPubClientId strings.Builder
	sbMqttPubClientId.WriteString("parse-lns-pub-")
	sbMqttPubClientId.WriteString(id)

	// KafkaProducerClient
	var sbKafkaProdClientId strings.Builder
	sbKafkaProdClientId.WriteString("parse-lns-prod-")
	sbKafkaProdClientId.WriteString(id)

	// MQTT
	mqttSubBroker := MQTT_BROKER_SUB
	mqttSubClientId := sbMqttSubClientId.String()
	mqttSubUser := "public"
	mqttSubPassword := "public"
	mqttSubQos := 0

	mqttSubOpts := MQTT.NewClientOptions()
	mqttSubOpts.AddBroker(mqttSubBroker)
	mqttSubOpts.SetClientID(mqttSubClientId)
	mqttSubOpts.SetUsername(mqttSubUser)
	mqttSubOpts.SetPassword(mqttSubPassword)
	mqttSubOpts.SetConnectionLostHandler(connLostHandler)

	c := make(chan [2]string)

	mqttSubOpts.SetDefaultPublishHandler(func(mqttClient MQTT.Client, msg MQTT.Message) {
		c <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	mqttSubClient := MQTT.NewClient(mqttSubOpts)
	if token := mqttSubClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", mqttSubBroker)
	}

	if token := mqttSubClient.Subscribe(sbMqttSubTopic.String(), byte(mqttSubQos), nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	mqttPubBroker := MQTT_BROKER_PUB
	mqttPubClientId := sbMqttPubClientId.String()
	mqttPubUser := ""
	mqttPubPassword := ""
	mqttPubQos := 0

	mqttPubOpts := MQTT.NewClientOptions()
	mqttPubOpts.AddBroker(mqttPubBroker)
	mqttPubOpts.SetClientID(mqttPubClientId)
	mqttPubOpts.SetUsername(mqttPubUser)
	mqttPubOpts.SetPassword(mqttPubPassword)

	pClient := MQTT.NewClient(mqttPubOpts)
	if token := pClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", mqttPubBroker)
	}

	// KAFKA
	// kafkaProdClient, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "my-cluster-kafka-bootstrap.test-kafka.svc.cluster.local"})
	kafkaProdClient, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": KAFKA_BROKER_PROD})
	if err != nil {
		panic(err)
	}
	defer kafkaProdClient.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range kafkaProdClient.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("\nDelivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	for {

		var input Input
		var output Output

		var outputJsonMsg, outputInfluxMsg string

		// 1. Input
		incoming := <-c

		// 2. Process
		// 2.1. Process Topic
		s := strings.Split(incoming[0], "/")
		fmt.Printf("\nTopic: %s\n", incoming[0])
		// OpenDataTelemetry/FSAELive/Car/Can/mauaracing/up/wifi
		// organization := s[1]

		input.Tags.Organization = s[1]
		input.Tags.DeviceType = s[2]
		input.Tags.Measurement = s[3]
		input.Tags.DeviceId = s[4]
		input.Tags.Direction = s[5]
		input.Tags.Etc = s[6]

		json.Unmarshal([]byte(incoming[1]), &input.Fields)

		output = parseInputIntoOutput(input)
		now := time.Now()      // current local time
		nsec := now.UnixNano() // number of nanoseconds since January 1, 1970 UTC

		output.Name = input.Tags.Measurement
		output.Timestamp = uint64(nsec)

		outputJsonMsg = marshalToJson(output)
		outputInfluxMsg = marshalToInflux(output)

		fmt.Printf("\n###### outputMsgJson: %v", outputJsonMsg)
		fmt.Printf("\n###### outputMsgInflux: %v", outputInfluxMsg)

		var sbMqttPubTopic strings.Builder
		sbMqttPubTopic.WriteString(input.Tags.Organization)
		sbMqttPubTopic.WriteString("/")
		sbMqttPubTopic.WriteString(input.Tags.DeviceType)
		sbMqttPubTopic.WriteString("/")
		sbMqttPubTopic.WriteString(input.Tags.Measurement)
		sbMqttPubTopic.WriteString("/")
		sbMqttPubTopic.WriteString(input.Tags.DeviceId)
		sbMqttPubTopic.WriteString("/")
		sbMqttPubTopic.WriteString(input.Tags.Direction)
		sbMqttPubTopic.WriteString("/")
		sbMqttPubTopic.WriteString(input.Tags.Etc)

		token := pClient.Publish(sbMqttPubTopic.String(), byte(mqttPubQos), false, outputJsonMsg)
		token.Wait()
		sbMqttPubTopic.Reset()

		// SET KAFKA
		var sbKafkaProdTopic strings.Builder
		sbKafkaProdTopic.WriteString(input.Tags.Organization)
		sbKafkaProdTopic.WriteString(".")
		sbKafkaProdTopic.WriteString("MauaRacing")
		kafkaProdTopic := sbKafkaProdTopic.String()
		// pClient.Publish(sbPubTopic.String(), byte(pQos), false, input.Msg)

		kafkaProdClient.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaProdTopic, Partition: kafka.PartitionAny},
			Value:          []byte(outputInfluxMsg),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}, nil)
		if err != nil {
			fmt.Printf("Produce failed: %v\n", err)
			os.Exit(1)
		}

		kafkaProdClient.Flush(15 * 1000)
	}
}
