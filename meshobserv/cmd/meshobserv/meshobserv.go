package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/D4rk4/meshmap.net/meshobserv/internal/meshtastic"
	"github.com/D4rk4/meshmap.net/meshobserv/internal/meshtastic/generated"
	"github.com/D4rk4/meshmap.net/meshobserv/internal/storage"
	"google.golang.org/protobuf/proto"
)

const (
	PruneWriteInterval = time.Minute
	DefaultRetention   = 360 // days
)

var (
	receiving atomic.Bool
	debug     bool
)

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func messageHandler(db *storage.DB) func(from uint32, topic string, portNum generated.PortNum, payload []byte) {
	return func(from uint32, topic string, portNum generated.PortNum, payload []byte) {
		receiving.Store(true)
		if err := db.Touch(from); err != nil {
			log.Printf("[warn] touch node %d: %v", from, err)
		}
		switch portNum {
		case generated.PortNum_TEXT_MESSAGE_APP:
			log.Printf("[msg] (%v) <%v> %s", topic, from, payload)
		case generated.PortNum_POSITION_APP:
			var position generated.Position
			if err := proto.Unmarshal(payload, &position); err != nil {
				log.Printf("[warn] could not parse Position payload from %v on %v: %v", from, topic, err)
				return
			}
			if err := db.UpsertPosition(from, position.GetLatitudeI(), position.GetLongitudeI(), position.GetAltitude(), position.GetPrecisionBits(), topic); err != nil {
				log.Printf("[warn] store position for %d: %v", from, err)
			}
		case generated.PortNum_NODEINFO_APP:
			var user generated.User
			if err := proto.Unmarshal(payload, &user); err != nil {
				log.Printf("[warn] could not parse User payload from %v on %v: %v", from, topic, err)
				return
			}
			if err := db.UpsertUser(from, user.GetLongName(), user.GetShortName(), user.GetHwModel().String(), user.GetRole().String()); err != nil {
				log.Printf("[warn] store user for %d: %v", from, err)
			}
		case generated.PortNum_TELEMETRY_APP:
			var telemetry generated.Telemetry
			if err := proto.Unmarshal(payload, &telemetry); err != nil {
				log.Printf("[warn] could not parse Telemetry payload from %v on %v: %v", from, topic, err)
				return
			}
			if deviceMetrics := telemetry.GetDeviceMetrics(); deviceMetrics != nil {
				if err := db.UpsertDeviceMetrics(from,
					deviceMetrics.GetBatteryLevel(),
					deviceMetrics.GetVoltage(),
					deviceMetrics.GetChannelUtilization(),
					deviceMetrics.GetAirUtilTx(),
					deviceMetrics.GetUptimeSeconds(),
				); err != nil {
					log.Printf("[warn] store device metrics for %d: %v", from, err)
				}
			}
			if envMetrics := telemetry.GetEnvironmentMetrics(); envMetrics != nil {
				if err := db.UpsertEnvironmentMetrics(from,
					envMetrics.GetTemperature(),
					envMetrics.GetRelativeHumidity(),
					envMetrics.GetBarometricPressure(),
					envMetrics.GetLux(),
					envMetrics.GetWindSpeed(),
					envMetrics.GetWindGust(),
					envMetrics.GetRadiation(),
					envMetrics.GetRainfall_1H(),
					envMetrics.GetRainfall_24H(),
					envMetrics.GetWindDirection(),
				); err != nil {
					log.Printf("[warn] store environment metrics for %d: %v", from, err)
				}
			}
		case generated.PortNum_NEIGHBORINFO_APP:
			var neighborInfo generated.NeighborInfo
			if err := proto.Unmarshal(payload, &neighborInfo); err != nil {
				log.Printf("[warn] could not parse NeighborInfo payload from %v on %v: %v", from, topic, err)
				return
			}
			nodeNum := neighborInfo.GetNodeId()
			if nodeNum != 0 && nodeNum != from {
				log.Printf("[info] neighborinfo node mismatch from %d (%d)", from, nodeNum)
				return
			}
			for _, neighbor := range neighborInfo.GetNeighbors() {
				neighborNum := neighbor.GetNodeId()
				if neighborNum == 0 {
					continue
				}
				if err := db.UpsertNeighbor(from, neighborNum, neighbor.GetSnr()); err != nil {
					log.Printf("[warn] store neighbor for %d -> %d: %v", from, neighborNum, err)
				}
			}
		case generated.PortNum_MAP_REPORT_APP:
			var mapReport generated.MapReport
			if err := proto.Unmarshal(payload, &mapReport); err != nil {
				log.Printf("[warn] could not parse MapReport payload from %v on %v: %v", from, topic, err)
				return
			}
			if err := db.UpsertMapReport(
				from,
				mapReport.GetLongName(),
				mapReport.GetShortName(),
				mapReport.GetFirmwareVersion(),
				mapReport.GetRegion().String(),
				mapReport.GetModemPreset().String(),
				mapReport.GetHasDefaultChannel(),
				mapReport.GetNumOnlineLocalNodes(),
				mapReport.GetLatitudeI(),
				mapReport.GetLongitudeI(),
				mapReport.GetAltitude(),
				mapReport.GetPositionPrecision(),
			); err != nil {
				log.Printf("[warn] store map report for %d: %v", from, err)
			}
		}
	}
}

func loadNodes(db *storage.DB) (meshtastic.NodeDB, error) {
	rows, neighbors, err := db.LoadValidNodes()
	if err != nil {
		return nil, err
	}
	out := make(meshtastic.NodeDB, len(rows))
	for nodeID, row := range rows {
		node := &meshtastic.Node{
			LongName:               row.LongName,
			ShortName:              row.Short,
			HwModel:                row.HwModel,
			Role:                   row.Role,
			FwVersion:              row.Fw,
			Region:                 row.Region,
			ModemPreset:            row.Modem,
			HasDefaultCh:           row.HasDef,
			OnlineLocalNodes:       row.Online,
			Latitude:               row.Lat,
			Longitude:              row.Lon,
			Altitude:               row.Alt,
			Precision:              row.Prec,
			BatteryLevel:           row.Battery,
			Voltage:                row.Voltage,
			ChUtil:                 row.ChUtil,
			AirUtilTx:              row.AirUtil,
			Uptime:                 row.Uptime,
			Temperature:            row.Temp,
			RelativeHumidity:       row.Hum,
			BarometricPressure:     row.Press,
			Lux:                    row.Lux,
			WindDirection:          row.WindDir,
			WindSpeed:              row.WindSpd,
			WindGust:               row.WindGust,
			Radiation:              row.Radiat,
			Rainfall1:              row.Rain1,
			Rainfall24:             row.Rain24,
			LastMapReport:          row.LastMap.Unix(),
			LastDeviceMetrics:      row.LastDev.Unix(),
			LastEnvironmentMetrics: row.LastEnv.Unix(),
			SeenBy:                 map[string]int64{"db": row.LastSeen.Unix()},
		}
		if node.LongName == "" && node.ShortName != "" {
			node.LongName = node.ShortName
		}
		if neighborRows := neighbors[nodeID]; neighborRows != nil {
			node.Neighbors = make(map[uint32]*meshtastic.NeighborInfo, len(neighborRows))
			for nbID, nb := range neighborRows {
				node.Neighbors[nbID] = &meshtastic.NeighborInfo{
					Snr:     nb.Snr,
					Updated: nb.Updated.Unix(),
				}
			}
		}
		if node.IsValid() {
			out[nodeID] = node
		}
	}
	return out, nil
}

func main() {
	var blockedPath, httpAddr, dbPath string
	var retentionDays int
	flag.StringVar(&blockedPath, "b", "", "node blocklist `file`")
	flag.StringVar(&httpAddr, "http", envString("HTTP_ADDR", ":80"), "HTTP listen address for serving nodes.json and health")
	flag.StringVar(&dbPath, "db", envString("DB_PATH", "/data/db/meshobserv.duckdb"), "path to DuckDB database file")
	flag.IntVar(&retentionDays, "retention-days", envInt("RETENTION_DAYS", DefaultRetention), "retention in days for MQTT data")
	flag.BoolVar(&debug, "debug", false, "enable verbose logging")
	flag.Parse()

	store, err := storage.Open(dbPath, time.Duration(retentionDays)*24*time.Hour)
	if err != nil {
		log.Fatalf("[fatal] open storage: %v", err)
	}
	defer store.Close()
	log.Printf("[info] using db %s retention %d days", dbPath, retentionDays)

	// load node blocklist
	blocked := make(map[uint32]struct{})
	if len(blockedPath) > 0 {
		f, err := os.Open(blockedPath)
		if err != nil {
			log.Fatalf("[fatal] open blocklist: %v", err)
		}
		s := bufio.NewScanner(f)
		for s.Scan() {
			n, err := strconv.ParseUint(s.Text(), 10, 32)
			if err == nil {
				blocked[uint32(n)] = struct{}{}
				log.Printf("[info] node %v blocked", n)
			}
		}
		f.Close()
		if err := s.Err(); err != nil {
			log.Fatalf("[fatal] read blocklist: %v", err)
		}
	}

	client := &meshtastic.MQTTClient{
		Topics:     []string{"msh/#"},
		TopicRegex: regexp.MustCompile(`^msh/.*`),
		Accept: func(from uint32) bool {
			if _, found := blocked[from]; found {
				return false
			}
			return true
		},
		BlockCipher:    meshtastic.NewBlockCipher(meshtastic.DefaultKey),
		MessageHandler: messageHandler(store),
	}
	if err := client.Connect(); err != nil {
		log.Fatalf("[fatal] connect: %v", err)
	}

	// prune loop
	go func() {
		ticker := time.NewTicker(PruneWriteInterval)
		defer ticker.Stop()
		for range ticker.C {
			if err := store.Prune(); err != nil {
				log.Printf("[warn] prune: %v", err)
			}
			if debug && !receiving.CompareAndSwap(true, false) {
				log.Print("[warn] no messages received in the last interval")
			}
		}
	}()

	// serve nodes.json over HTTP
	mux := http.NewServeMux()
	mux.HandleFunc("/nodes.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		nodes, err := loadNodes(store)
		if err != nil {
			log.Printf("[warn] load nodes: %v", err)
			http.Error(w, "load error", http.StatusInternalServerError)
			return
		}
		if err := json.NewEncoder(w).Encode(nodes); err != nil {
			http.Error(w, "encode error", http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	go func() {
		log.Printf("[http] listening on %s", httpAddr)
		if err := http.ListenAndServe(httpAddr, mux); err != nil {
			log.Fatalf("[fatal] http listen: %v", err)
		}
	}()

	// wait until exit
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, syscall.SIGINT, syscall.SIGTERM)
	<-terminate
	log.Print("[info] exiting")
	client.Disconnect()
}
