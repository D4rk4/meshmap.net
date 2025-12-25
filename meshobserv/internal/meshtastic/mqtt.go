package meshtastic

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/D4rk4/meshmap.net/meshobserv/internal/meshtastic/generated"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var DefaultKey = []byte{
	0xd4, 0xf1, 0xbb, 0x3a,
	0x20, 0x29, 0x07, 0x59,
	0xf0, 0xbc, 0xff, 0xab,
	0xcf, 0x4e, 0x69, 0x01,
}

func NewBlockCipher(key []byte) cipher.Block {
	c, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	return c
}

type MQTTClient struct {
	Topics         []string
	TopicRegex     *regexp.Regexp
	Accept         func(from uint32) bool
	BlockCipher    cipher.Block
	MessageHandler func(from uint32, topic string, portNum generated.PortNum, payload []byte)
	mqtt.Client
}

func (c *MQTTClient) Connect() error {
	randomId := make([]byte, 4)
	rand.Read(randomId)
	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		broker = "tcp://mthub.monteops.com:1883"
	} else if !strings.Contains(broker, "://") {
		broker = "tcp://" + broker
	}
	username := os.Getenv("MQTT_USERNAME")
	if username == "" {
		username = "meshdev"
	}
	password := os.Getenv("MQTT_PASSWORD")
	if password == "" {
		password = "large4cats"
	}
	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetClientID(fmt.Sprintf("meshobserv-%x", randomId))
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetOrderMatters(false)
	opts.SetDefaultPublishHandler(c.handleMessage)
	c.Client = mqtt.NewClient(opts)
	token := c.Client.Connect()
	<-token.Done()
	if err := token.Error(); err != nil {
		return err
	}
	log.Print("[mqtt] connected")
	topics := make(map[string]byte)
	for _, topic := range c.Topics {
		topics[topic] = 0
	}
	token = c.SubscribeMultiple(topics, nil)
	<-token.Done()
	if err := token.Error(); err != nil {
		return err
	}
	log.Print("[mqtt] subscribed")
	return nil
}

func (c *MQTTClient) Disconnect() {
	if c.IsConnected() {
		c.Client.Disconnect(1000)
	}
}

func (c *MQTTClient) handleMessage(_ mqtt.Client, msg mqtt.Message) {
	// filter topic
	topic := msg.Topic()
	if !c.TopicRegex.MatchString(topic) {
		return
	}
	// parse ServiceEnvelope
	var envelope generated.ServiceEnvelope
	if err := proto.Unmarshal(msg.Payload(), &envelope); err != nil {
		// try JSON-encoded envelope
		if err := protojson.Unmarshal(msg.Payload(), &envelope); err != nil {
			// try raw JSON message format
			if !c.handleJSONMessage(topic, msg.Payload()) {
				log.Printf("[info] ignoring non-Meshtastic payload on %s: %v", topic, err)
			}
			return
		}
	}
	// get MeshPacket
	packet := envelope.GetPacket()
	if packet == nil {
		return
	}
	// no anonymous packets
	from := packet.GetFrom()
	if from == 0 {
		return
	}
	// ignore PKI direct messages
	if packet.GetPkiEncrypted() {
		return
	}
	// check sender
	if c.Accept != nil && !c.Accept(from) {
		return
	}
	if ch := strings.TrimSpace(envelope.GetChannelId()); ch != "" {
		topic = "msh/" + ch
	}
	// get Data, try decoded first
	data := packet.GetDecoded()
	if data == nil {
		// data must be (probably) encrypted
		encrypted := packet.GetEncrypted()
		if encrypted == nil {
			return
		}
		// decrypt
		nonce := make([]byte, 16)
		binary.LittleEndian.PutUint32(nonce[0:], packet.GetId())
		binary.LittleEndian.PutUint32(nonce[8:], from)
		decrypted := make([]byte, len(encrypted))
		cipher.NewCTR(c.BlockCipher, nonce).XORKeyStream(decrypted, encrypted)
		// parse Data
		data = new(generated.Data)
		if err := proto.Unmarshal(decrypted, data); err != nil {
			// ignore, probably encrypted with other psk
			return
		}
	}
	c.MessageHandler(from, topic, data.GetPortnum(), data.GetPayload())
}

type jsonMessage struct {
	Channel string                 `json:"channel"`
	From    uint32                 `json:"from"`
	To      uint32                 `json:"to"`
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload"`
}

func (c *MQTTClient) handleJSONMessage(topic string, payload []byte) bool {
	var jm jsonMessage
	if err := json.Unmarshal(payload, &jm); err != nil {
		return false
	}
	if jm.From == 0 {
		return false
	}
	if jm.Channel != "" {
		topic = "msh/" + jm.Channel
	}

	makePosition := func() ([]byte, bool) {
		lat, okLat := asInt32(jm.Payload["latitude_i"])
		lon, okLon := asInt32(jm.Payload["longitude_i"])
		if !okLat || !okLon || (lat == 0 && lon == 0) {
			return nil, false
		}
		alt, _ := asInt32(jm.Payload["altitude"])
		prec, _ := asUint32(jm.Payload["precision_bits"])
		pos := &generated.Position{
			LatitudeI:     ptrInt32(lat),
			LongitudeI:    ptrInt32(lon),
			Altitude:      ptrInt32(alt),
			PrecisionBits: prec,
		}
		b, err := proto.Marshal(pos)
		if err != nil {
			log.Printf("[info] failed to marshal position from json on %s: %v", topic, err)
			return nil, false
		}
		return b, true
	}

	makeUser := func() ([]byte, bool) {
		longName, _ := jm.Payload["long_name"].(string)
		shortName, _ := jm.Payload["short_name"].(string)
		if longName == "" && shortName == "" {
			return nil, false
		}
		user := &generated.User{
			LongName:  longName,
			ShortName: shortName,
		}
		b, err := proto.Marshal(user)
		if err != nil {
			log.Printf("[info] failed to marshal user from json on %s: %v", topic, err)
			return nil, false
		}
		return b, true
	}

	makeMapReport := func() ([]byte, bool) {
		longName, _ := jm.Payload["long_name"].(string)
		shortName, _ := jm.Payload["short_name"].(string)
		lat, latOk := asInt32(jm.Payload["latitude_i"])
		lon, lonOk := asInt32(jm.Payload["longitude_i"])
		alt, _ := asInt32(jm.Payload["altitude"])
		prec, _ := asUint32(jm.Payload["position_precision"])
		if (latOk && lonOk) && (lat != 0 || lon != 0) {
			mr := &generated.MapReport{
				LongName:          longName,
				ShortName:         shortName,
				LatitudeI:         lat,
				LongitudeI:        lon,
				Altitude:          alt,
				PositionPrecision: prec,
			}
			b, err := proto.Marshal(mr)
			if err != nil {
				log.Printf("[info] failed to marshal mapreport from json on %s: %v", topic, err)
				return nil, false
			}
			return b, true
		}
		return nil, false
	}

	switch strings.ToLower(jm.Type) {
	case "position":
		if data, ok := makePosition(); ok {
			c.MessageHandler(jm.From, topic, generated.PortNum_POSITION_APP, data)
			return true
		}
	case "nodeinfo":
		if data, ok := makeUser(); ok {
			c.MessageHandler(jm.From, topic, generated.PortNum_NODEINFO_APP, data)
			return true
		}
	case "mapreport":
		if data, ok := makeMapReport(); ok {
			c.MessageHandler(jm.From, topic, generated.PortNum_MAP_REPORT_APP, data)
			return true
		}
	}

	// fallback: try mapreport if position data present
	if data, ok := makeMapReport(); ok {
		c.MessageHandler(jm.From, topic, generated.PortNum_MAP_REPORT_APP, data)
		return true
	}
	if data, ok := makePosition(); ok {
		c.MessageHandler(jm.From, topic, generated.PortNum_POSITION_APP, data)
		return true
	}
	if data, ok := makeUser(); ok {
		c.MessageHandler(jm.From, topic, generated.PortNum_NODEINFO_APP, data)
		return true
	}
	return false
}

func asInt32(v interface{}) (int32, bool) {
	switch n := v.(type) {
	case float64:
		return int32(n), true
	case float32:
		return int32(n), true
	case int:
		return int32(n), true
	case int32:
		return n, true
	case int64:
		return int32(n), true
	case uint32:
		return int32(n), true
	case uint64:
		return int32(n), true
	default:
		return 0, false
	}
}

func asUint32(v interface{}) (uint32, bool) {
	switch n := v.(type) {
	case float64:
		return uint32(n), true
	case float32:
		return uint32(n), true
	case int:
		return uint32(n), true
	case int32:
		return uint32(n), true
	case int64:
		return uint32(n), true
	case uint32:
		return n, true
	case uint64:
		return uint32(n), true
	default:
		return 0, false
	}
}

func ptrInt32(v int32) *int32 {
	return &v
}

func init() {
	mqtt.ERROR = log.New(os.Stderr, "[mqtt] error: ", log.Flags()|log.Lmsgprefix)
	mqtt.CRITICAL = log.New(os.Stderr, "[mqtt] crit: ", log.Flags()|log.Lmsgprefix)
}
