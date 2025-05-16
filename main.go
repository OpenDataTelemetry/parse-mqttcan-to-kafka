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
type CarDynamics struct {
	GroundSpeed    float64 `json:"groundSpeed"`
	GLateral       float64 `json:"gLateral"`
	GLonggitudinal float64 `json:"gLongitudinal"`
	BrakePressure  float64 `json:"brakePressure"`
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
	ThrottlePosition     float64 `json:"throttlePosition"`
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

// ParseLns -> ParseLnsMeasurement (case port 100) -> Parse ProtocolPort100 -> parseLnsMeasurement according Measurement writing influx
// ParseCar -> ParseCarMeasurement -> ParseCan (case id 1) -> ParseCanId1 -> ParseCan according Measurement writing influx
// OpenDataTelemetry/FSAELive/Car/Can/mauaracing/up/wifi
// OpenDataTelemetry/FSAELive/Car/Inertias/mauaracing/up/wifi
// OpenDataTelemetry/FSAELive/Car/Tracking/mauaracing/up/wifi
// OpenDataTelemetry/FSAELive/RaceTrack/SkidPad/mauaracing/up/wifi

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

	// len := len(canData)
	i := 0

	switch canId {
	// case "503": // gndSpd - gLat - gLong - brkPressure
	// 	v := uint64(canData[i+1]) << 8
	// 	v |= uint64(canData[i+2])
	// 	f := float64(v) / 100
	// 	carCanData.EngineOilTemperature = f
	// 	i = i + 2

	// 	v = uint64(canData[i+1]) << 8
	// 	v |= uint64(canData[i+2])
	// 	f = float64(v) / 100
	// 	carCanData.EngineOilPressure = f
	// 	i = i + 2

	case "503": // GroundSpeed, G-Lateral, G-Longitudinal, Brake Pressure
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

	// case "504": // engRPM - Gear - Throttlepos - Steering Angle
	// 	v := uint64(canData[i+1]) << 8
	// 	v |= uint64(canData[i+2])
	// 	f := float64(v) / 100
	// 	carCanData.EngineOilTemperature = f
	// 	i = i + 2

	// 	v = uint64(canData[i+1]) << 8
	// 	v |= uint64(canData[i+2])
	// 	f = float64(v) / 100
	// 	carCanData.EngineOilPressure = f
	// 	i = i + 2
	// }
	case "504": // Engine RPM, Gear, Throttle Position, Steering Angle
		if len(canData) >= i+8 {
			v := uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.EngineRPM = float64(v)
			i += 2

			v = uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.Gear = float64(v)
			i += 2

			v = uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.ThrottlePosition = float64(v) / 100
			i += 2

			v = uint64(canData[i])<<8 | uint64(canData[i+1])
			carCanData.SteeringAngle = (float64(v) - 32768) / 100
			i += 2
		}

	case "502": // GPSAltitude GPSHeading GPSSpeed GPSSatsUsed
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
	case "500": //GPSLatitude
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

	case "501": //GPSLongitude
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

	}

	p, err := json.Marshal(carCanData)
	if err != nil {
		fmt.Println(err)
		return "Can data parsed wrongly"
	}
	return string(p[:])
}

func parseCarMeasurement(deviceType string, measurement string, data string) string {
	// Car, CAN | Tracking, 001,1122334455667788
	// MEASUREMENTS: Tracking, Inertias, CanIC, CanEV, CanH2
	// CanIC -> 0X01, 0X02, 0X03
	// CanEV -> 0X11, 0X12, 0X13
	// CanH2 -> 0X21, 0X22, 0X23

	var sb strings.Builder

	if data == "" {
		return "No data"
	}

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
			//Can Message
			// CAN: 001,1122334455667788
			var carCanData CarCanData

			s := strings.Split(data, ",")
			canId := s[0]
			canData := s[1]

			// B64 to Byte
			b, err := b64ToByte(canData)
			if err != nil {
				// fmt.Print(data)
				log.Panic(err)
			}

			d := protocolParserCanDataByCanId(canId, b)
			json.Unmarshal([]byte(d), &carCanData)
			// fmt.Printf("protocolParserCanDataByCanId, d: %v\n", d)

			sb.WriteString(`,canId=`)
			sb.WriteString(canId)
			if canId == "503" {
				// sb.WriteString(`,deviceType=`)
				// sb.WriteString(deviceType)
				sb.WriteString(`,message=CarDynamics`)
				sb.WriteString(` data="`)
				sb.WriteString(canData)
				sb.WriteString(`",brakePressure=`)
				sb.WriteString(strconv.FormatFloat(carCanData.BrakePressure, 'f', -1, 64))
				sb.WriteString(`,gLongitudinal=`)
				sb.WriteString(strconv.FormatFloat(carCanData.GLongitudinal, 'f', -1, 64))
				sb.WriteString(`,gLateral=`)
				sb.WriteString(strconv.FormatFloat(carCanData.GLateral, 'f', -1, 64))
				sb.WriteString(`,groundSpeed=`)
				sb.WriteString(strconv.FormatFloat(carCanData.GroundSpeed, 'f', -1, 64))
			}
			if canId == "504" {
				// sb.WriteString(`,deviceType=`)
				// sb.WriteString(deviceType)
				sb.WriteString(`,message=CarEngine`)
				sb.WriteString(` data="`)
				sb.WriteString(canData)
				sb.WriteString(`",engineRPM=`)
				sb.WriteString(strconv.FormatFloat(carCanData.EngineRPM, 'f', -1, 64))
				sb.WriteString(`,gear=`)
				sb.WriteString(strconv.FormatFloat(carCanData.Gear, 'f', -1, 64))
				sb.WriteString(`,throttlePosition=`)
				sb.WriteString(strconv.FormatFloat(carCanData.ThrottlePosition, 'f', -1, 64))
				sb.WriteString(`,steeringAngle=`)
				sb.WriteString(strconv.FormatFloat(carCanData.SteeringAngle, 'f', -1, 64))
			}
			if canId == "502" {
				// sb.WriteString(`,deviceType=`)
				// sb.WriteString(deviceType)
				sb.WriteString(`,message=GPSOthers`)
				sb.WriteString(` data="`)
				sb.WriteString(canData)
				sb.WriteString(`",GPSAltitude=`)
				sb.WriteString(strconv.FormatFloat(carCanData.GPSAltitude, 'f', -1, 64))
				sb.WriteString(`,GPSHeading=`)
				sb.WriteString(strconv.FormatFloat(carCanData.GPSHeading, 'f', -1, 64))
				sb.WriteString(`,GPSSpeed=`)
				sb.WriteString(strconv.FormatFloat(carCanData.GPSSpeed, 'f', -1, 64))
				sb.WriteString(`,GPSSatsUsed=`)
				sb.WriteString(strconv.FormatFloat(carCanData.GPSSatsUsed, 'f', -1, 64))
			}
			if canId == "500" {
				// sb.WriteString(`,deviceType=`)
				// sb.WriteString(deviceType)
				sb.WriteString(`,message=GPSLatitude`)
				sb.WriteString(` data="`)
				sb.WriteString(canData)
				sb.WriteString(`",GPSLatitude=`)
				sb.WriteString(strconv.FormatFloat(carCanData.GPSLatitude, 'f', -1, 64))
			}
			if canId == "501" {
				// sb.WriteString(`,deviceType=`)
				// sb.WriteString(deviceType)
				sb.WriteString(`,message=GPSLongitude`)
				sb.WriteString(` data="`)
				sb.WriteString(canData)
				sb.WriteString(`",GPSLongitude=`)
				sb.WriteString(strconv.FormatFloat(carCanData.GPSLongitude, 'f', -1, 64))
			}

			// sb.WriteString(`,engineOilTemperature=`)
			// sb.WriteString(strconv.FormatFloat(carCanData.EngineOilTemperature, 'f', -1, 64))
			// sb.WriteString(`,engineOilPressure=`)
			// sb.WriteString(strconv.FormatFloat(carCanData.EngineOilPressure, 'f', -1, 64))

			// sb.WriteString(` `)
			// sb.WriteString(`EngineOilTemperature=`)
			// sb.WriteString(EngineOilTemperature)
			// sb.WriteString(`,EngineOilPressure=`)
			// sb.WriteString(EngineOilPressure)
		}
	case "RaceTrack":
		switch measurement {
		case "SkidPad":
			// var raceTrackSkidPad RaceTrackSkidPad
			// json.Unmarshal([]byte(data), &raceTrackSkidPad)
		}
	}

	return sb.String()
}

func parseCar(measurement string, deviceType string, deviceId string, direction string, etc string, message string) string {
	var sb strings.Builder
	var carUp CarUp

	// fmt.Printf("parseCar, measurement: %s\n", measurement)
	// fmt.Printf("parseCar, deviceType: %s\n", deviceType)
	// fmt.Printf("parseCar, direction: %s\n", direction)
	// fmt.Printf("parseCar, etc: %s\n", etc)
	// fmt.Printf("parseCar, message: %s\n", message)

	if message == "" {
		return "No message to parse"
	}

	if direction == "up" {
		json.Unmarshal([]byte(message), &carUp)

		// t := carUp.Timestamp

		// Measurement
		sb.WriteString(measurement)

		// Tags
		sb.WriteString(`,deviceId=`)
		sb.WriteString(deviceId)
		sb.WriteString(`,deviceType=`)
		sb.WriteString(deviceType)
		sb.WriteString(`,direction=`)
		sb.WriteString(direction)
		sb.WriteString(`,origin=`)
		sb.WriteString(etc)

		// Fields
		sb.WriteString(parseCarMeasurement(deviceType, measurement, message)) // Car, CAN | Tracking, 001#1122334455667788
		// sb.WriteString(`,data=`)
		// sb.WriteString(message)

		// Timestamp_ns

		now := time.Now()      // current local time
		nsec := now.UnixNano() // number of nanoseconds since January 1, 1970 UTC

		sb.WriteString(` `)
		sb.WriteString(strconv.FormatInt(nsec, 10))
	}

	return sb.String()
}

func connLostHandler(c MQTT.Client, err error) {
	fmt.Printf("Connection lost, reason: %v\n", err)
	os.Exit(1)
}

func main() {

	// // CAN message
	// canMsg := `{
	// 	"canMsg": "016#1122334455667788"
	// 	"timestamp": 178273648364
	// }`

	id := uuid.New().String()
	// ORGANIZATION := os.Getenv("ORGANIZATION")
	// DEVICE_TYPE := os.Getenv("DEVICE_TYPE")
	BUCKET := os.Getenv("BUCKET")
	MQTT_BROKER := os.Getenv("MQTT_BROKER")
	kafkaBroker := os.Getenv("KAFKA_BROKER")

	// MqttSubscriberClient
	var sbMqttSubClientId strings.Builder
	sbMqttSubClientId.WriteString("parse-lns-sub-")
	sbMqttSubClientId.WriteString(id)

	// MqttSubscriberTopic
	var sbMqttSubTopic strings.Builder
	// sbMqttSubTopic.WriteString("debug/OpenDataTelemetry/")
	sbMqttSubTopic.WriteString("OpenDataTelemetry/")
	// sbMqttSubTopic.WriteString(ORGANIZATION)
	// sbMqttSubTopic.WriteString("/")
	// sbMqttSubTopic.WriteString(DEVICE_TYPE)
	sbMqttSubTopic.WriteString("FSAELive/Car/Can/mauaracing/up/sim7670g")
	// sbMqttSubTopic.WriteString("#")
	// sbMqttSubTopic.WriteString("/+/+/+")

	// KafkaProducerClient
	var sbKafkaProdClientId strings.Builder
	sbKafkaProdClientId.WriteString("parse-lns-prod-")
	sbKafkaProdClientId.WriteString(id)

	// MQTT
	mqttSubBroker := MQTT_BROKER
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

	// KAFKA
	// kafkaProdClient, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "my-cluster-kafka-bootstrap.test-kafka.svc.cluster.local"})
	kafkaProdClient, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
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

	// MQTT -> KAFKA
	for {
		// 1. Input
		incoming := <-c

		// 2. Process
		// 2.1. Process Topic
		s := strings.Split(incoming[0], "/")
		fmt.Printf("\nTopic: %s\n", incoming[0])
		// OpenDataTelemetry/FSAELive/Car/Can/mauaracing/up/wifi
		organization := s[1]
		deviceType := s[2]
		measurement := s[3]
		deviceId := s[4]
		direction := s[5]
		etc := s[6]

		// // DEBUG
		// measurement := s[4]
		// deviceId := s[5]
		// direction := s[6]
		// etc := s[7]

		var kafkaMessage string

		switch organization {
		case "FSAELive":
			switch deviceType {
			case "Car": // measurements: ICCan, CarInertias, CarTracking
				kafkaMessage = parseCar(measurement, deviceType, deviceId, direction, etc, incoming[1])
			case "RaceTrack": // measurements: SkidPad, Acceleration, Autocross, Endurance
				// kafkaMessage = parseRaceTrack(measurement, deviceType, deviceId, direction, etc, incoming[1])
			}

			fmt.Printf("\nMessage: %s", kafkaMessage)
		}

		// return influx line protocol
		// measurement,tags fields timestamp
		// fmt.Printf("InfluxLineProtocol: %s\n", kafkaMessage)

		// SET KAFKA
		// KafkaProducerClient
		var sbKafkaProdTopic strings.Builder
		// TODO : parse by ORGANIZATION
		// sbKafkaProdTopic.WriteString(organization)
		sbKafkaProdTopic.WriteString("FSAELive")
		sbKafkaProdTopic.WriteString(".")
		sbKafkaProdTopic.WriteString(BUCKET)
		kafkaProdTopic := sbKafkaProdTopic.String()
		// pClient.Publish(sbPubTopic.String(), byte(pQos), false, incoming[1])

		kafkaProdClient.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaProdTopic, Partition: kafka.PartitionAny},
			Value:          []byte(kafkaMessage),
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}, nil)
		if err != nil {
			fmt.Printf("Produce failed: %v\n", err)
			os.Exit(1)
		}

		kafkaProdClient.Flush(15 * 1000)
	}
}
