package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

type DB struct {
	conn      *sql.DB
	retention time.Duration
}

func Open(path string, retention time.Duration) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("mkdir db dir: %w", err)
	}
	conn, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	conn.SetMaxOpenConns(1)
	conn.SetMaxIdleConns(1)
	if _, err := conn.Exec(`PRAGMA threads=2`); err != nil {
		return nil, fmt.Errorf("set pragma: %w", err)
	}
	schema := `
CREATE TABLE IF NOT EXISTS nodes (
  node_id           UINTEGER PRIMARY KEY,
  long_name         TEXT,
  short_name        TEXT,
  hw_model          TEXT,
  role              TEXT,
  fw_version        TEXT,
  region            TEXT,
  modem_preset      TEXT,
  has_default_ch    BOOLEAN,
  online_local_nodes UINTEGER,
  latitude          INTEGER,
  longitude         INTEGER,
  altitude          INTEGER,
  precision_bits    UINTEGER,
  last_position     TIMESTAMP,
  last_map_report   TIMESTAMP,
  battery_level     UINTEGER,
  voltage           DOUBLE,
  ch_util           DOUBLE,
  air_util_tx       DOUBLE,
  uptime            UINTEGER,
  last_device_metrics TIMESTAMP,
  temperature       DOUBLE,
  relative_humidity DOUBLE,
  barometric_pressure DOUBLE,
  lux               DOUBLE,
  wind_direction    UINTEGER,
  wind_speed        DOUBLE,
  wind_gust         DOUBLE,
  radiation         DOUBLE,
  rainfall1         DOUBLE,
  rainfall24        DOUBLE,
  last_environment_metrics TIMESTAMP,
  first_seen        TIMESTAMP DEFAULT now(),
  last_seen         TIMESTAMP DEFAULT now()
);
CREATE TABLE IF NOT EXISTS neighbors (
  node_id     UINTEGER,
  neighbor_id UINTEGER,
  snr         DOUBLE,
  updated     TIMESTAMP,
  PRIMARY KEY (node_id, neighbor_id)
);`
	if _, err := conn.Exec(schema); err != nil {
		conn.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &DB{conn: conn, retention: retention}, nil
}

func (db *DB) Close() error {
	return db.conn.Close()
}

func (db *DB) touch(nodeID uint32) error {
	_, err := db.conn.Exec(`INSERT INTO nodes(node_id,last_seen,first_seen) VALUES (?, now(), now())
ON CONFLICT (node_id) DO UPDATE SET last_seen=now()`, nodeID)
	return err
}

func (db *DB) Touch(nodeID uint32) error {
	return db.touch(nodeID)
}

func (db *DB) UpsertPosition(nodeID uint32, lat, lon, alt int32, prec uint32, topic string) error {
	if err := db.touch(nodeID); err != nil {
		return err
	}
	_, err := db.conn.Exec(`INSERT INTO nodes(node_id, latitude, longitude, altitude, precision_bits, last_position, last_seen)
VALUES (?, ?, ?, ?, ?, now(), now())
ON CONFLICT (node_id) DO UPDATE SET latitude=excluded.latitude, longitude=excluded.longitude, altitude=excluded.altitude,
precision_bits=excluded.precision_bits, last_position=excluded.last_position, last_seen=excluded.last_seen`, nodeID, lat, lon, alt, prec)
	return err
}

func (db *DB) UpsertUser(nodeID uint32, longName, shortName, hwModel, role string) error {
	if err := db.touch(nodeID); err != nil {
		return err
	}
	_, err := db.conn.Exec(`INSERT INTO nodes(node_id, long_name, short_name, hw_model, role, last_seen)
VALUES (?, ?, ?, ?, ?, now())
ON CONFLICT (node_id) DO UPDATE SET long_name=excluded.long_name, short_name=excluded.short_name,
hw_model=excluded.hw_model, role=excluded.role, last_seen=excluded.last_seen`, nodeID, longName, shortName, hwModel, role)
	return err
}

func (db *DB) UpsertMapReport(nodeID uint32, longName, shortName, fwVersion, region, modemPreset string, hasDefault bool, onlineLocal uint32, lat, lon, alt int32, prec uint32) error {
	if err := db.touch(nodeID); err != nil {
		return err
	}
	_, err := db.conn.Exec(`INSERT INTO nodes(node_id, long_name, short_name, fw_version, region, modem_preset, has_default_ch, online_local_nodes,
 latitude, longitude, altitude, precision_bits, last_map_report, last_seen)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now())
ON CONFLICT (node_id) DO UPDATE SET long_name=excluded.long_name, short_name=excluded.short_name, fw_version=excluded.fw_version,
region=excluded.region, modem_preset=excluded.modem_preset, has_default_ch=excluded.has_default_ch, online_local_nodes=excluded.online_local_nodes,
latitude=excluded.latitude, longitude=excluded.longitude, altitude=excluded.altitude, precision_bits=excluded.precision_bits,
last_map_report=excluded.last_map_report, last_seen=excluded.last_seen`,
		nodeID, longName, shortName, fwVersion, region, modemPreset, hasDefault, onlineLocal, lat, lon, alt, prec)
	return err
}

func (db *DB) UpsertDeviceMetrics(nodeID uint32, battery uint32, voltage, chUtil, airUtilTx float32, uptime uint32) error {
	if err := db.touch(nodeID); err != nil {
		return err
	}
	_, err := db.conn.Exec(`INSERT INTO nodes(node_id, battery_level, voltage, ch_util, air_util_tx, uptime, last_device_metrics, last_seen)
VALUES (?, ?, ?, ?, ?, ?, now(), now())
ON CONFLICT (node_id) DO UPDATE SET battery_level=excluded.battery_level, voltage=excluded.voltage, ch_util=excluded.ch_util,
air_util_tx=excluded.air_util_tx, uptime=excluded.uptime, last_device_metrics=excluded.last_device_metrics, last_seen=excluded.last_seen`,
		nodeID, battery, voltage, chUtil, airUtilTx, uptime)
	return err
}

func (db *DB) UpsertEnvironmentMetrics(nodeID uint32, temperature, humidity, pressure, lux, windSpeed, windGust, radiation, rainfall1, rainfall24 float32, windDir uint32) error {
	if err := db.touch(nodeID); err != nil {
		return err
	}
	_, err := db.conn.Exec(`INSERT INTO nodes(node_id, temperature, relative_humidity, barometric_pressure, lux, wind_speed, wind_gust, radiation, rainfall1, rainfall24, wind_direction, last_environment_metrics, last_seen)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), now())
ON CONFLICT (node_id) DO UPDATE SET temperature=excluded.temperature, relative_humidity=excluded.relative_humidity,
barometric_pressure=excluded.barometric_pressure, lux=excluded.lux, wind_speed=excluded.wind_speed, wind_gust=excluded.wind_gust,
radiation=excluded.radiation, rainfall1=excluded.rainfall1, rainfall24=excluded.rainfall24, wind_direction=excluded.wind_direction,
last_environment_metrics=excluded.last_environment_metrics, last_seen=excluded.last_seen`,
		nodeID, temperature, humidity, pressure, lux, windSpeed, windGust, radiation, rainfall1, rainfall24, windDir)
	return err
}

func (db *DB) UpsertNeighbor(nodeID, neighborID uint32, snr float32) error {
	if err := db.touch(nodeID); err != nil {
		return err
	}
	_, err := db.conn.Exec(`INSERT INTO neighbors(node_id, neighbor_id, snr, updated) VALUES (?, ?, ?, now())
ON CONFLICT (node_id, neighbor_id) DO UPDATE SET snr=excluded.snr, updated=excluded.updated`, nodeID, neighborID, snr)
	return err
}

func (db *DB) Prune() error {
	cutoff := time.Now().Add(-db.retention)
	_, err := db.conn.Exec(`DELETE FROM neighbors WHERE updated < ?;
DELETE FROM nodes WHERE last_seen < ?;`, cutoff, cutoff)
	return err
}

type NodeRow struct {
	NodeID   uint32
	LongName string
	Short    string
	HwModel  string
	Role     string
	Lat      int32
	Lon      int32
	Alt      int32
	Prec     uint32
	Fw       string
	Region   string
	Modem    string
	HasDef   bool
	Online   uint32
	Battery  uint32
	Voltage  float32
	ChUtil   float32
	AirUtil  float32
	Uptime   uint32
	Temp     float32
	Hum      float32
	Press    float32
	Lux      float32
	WindDir  uint32
	WindSpd  float32
	WindGust float32
	Radiat   float32
	Rain1    float32
	Rain24   float32
	LastMap  time.Time
	LastDev  time.Time
	LastEnv  time.Time
	LastSeen time.Time
}

func (db *DB) LoadValidNodes() (map[uint32]*NodeRow, map[uint32]map[uint32]NeighborRow, error) {
	nodes := make(map[uint32]*NodeRow)
	neighbors := make(map[uint32]map[uint32]NeighborRow)
	cutoff := time.Now().Add(-db.retention)
	rows, err := db.conn.Query(`
SELECT node_id,
  coalesce(long_name, '') AS long_name,
  coalesce(short_name, '') AS short_name,
  coalesce(hw_model, '') AS hw_model,
  coalesce(role, '') AS role,
  coalesce(latitude, 0) AS latitude,
  coalesce(longitude, 0) AS longitude,
  coalesce(altitude, 0) AS altitude,
  coalesce(precision_bits, 0) AS precision_bits,
  coalesce(fw_version, '') AS fw_version,
  coalesce(region, '') AS region,
  coalesce(modem_preset, '') AS modem_preset,
  coalesce(has_default_ch, false) AS has_default_ch,
  coalesce(online_local_nodes, 0) AS online_local_nodes,
  coalesce(battery_level, 0) AS battery_level,
  coalesce(voltage, 0) AS voltage,
  coalesce(ch_util, 0) AS ch_util,
  coalesce(air_util_tx, 0) AS air_util_tx,
  coalesce(uptime, 0) AS uptime,
  coalesce(temperature, 0) AS temperature,
  coalesce(relative_humidity, 0) AS relative_humidity,
  coalesce(barometric_pressure, 0) AS barometric_pressure,
  coalesce(lux, 0) AS lux,
  coalesce(wind_direction, 0) AS wind_direction,
  coalesce(wind_speed, 0) AS wind_speed,
  coalesce(wind_gust, 0) AS wind_gust,
  coalesce(radiation, 0) AS radiation,
  coalesce(rainfall1, 0) AS rainfall1,
  coalesce(rainfall24, 0) AS rainfall24,
  coalesce(last_map_report, TIMESTAMP 'epoch') AS last_map_report,
  coalesce(last_device_metrics, TIMESTAMP 'epoch') AS last_device_metrics,
  coalesce(last_environment_metrics, TIMESTAMP 'epoch') AS last_environment_metrics,
  coalesce(last_seen, TIMESTAMP 'epoch') AS last_seen
FROM nodes
WHERE (latitude != 0 OR longitude != 0) AND (coalesce(long_name, '') != '' OR coalesce(short_name, '') != '') AND last_seen >= ?
`, cutoff)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var r NodeRow
		if err := rows.Scan(
			&r.NodeID, &r.LongName, &r.Short, &r.HwModel, &r.Role,
			&r.Lat, &r.Lon, &r.Alt, &r.Prec,
			&r.Fw, &r.Region, &r.Modem, &r.HasDef, &r.Online,
			&r.Battery, &r.Voltage, &r.ChUtil, &r.AirUtil, &r.Uptime,
			&r.Temp, &r.Hum, &r.Press, &r.Lux, &r.WindDir, &r.WindSpd, &r.WindGust, &r.Radiat, &r.Rain1, &r.Rain24,
			&r.LastMap, &r.LastDev, &r.LastEnv, &r.LastSeen,
		); err != nil {
			return nil, nil, err
		}
		nodes[r.NodeID] = &r
	}
	nrows, err := db.conn.Query(`SELECT node_id, neighbor_id, snr, updated FROM neighbors WHERE updated >= ?`, cutoff)
	if err != nil {
		return nil, nil, err
	}
	defer nrows.Close()
	for nrows.Next() {
		var nid, nb uint32
		var snr float32
		var updated time.Time
		if err := nrows.Scan(&nid, &nb, &snr, &updated); err != nil {
			return nil, nil, err
		}
		if neighbors[nid] == nil {
			neighbors[nid] = make(map[uint32]NeighborRow)
		}
		neighbors[nid][nb] = NeighborRow{Snr: snr, Updated: updated}
	}
	return nodes, neighbors, nil
}

type NeighborRow struct {
	Snr     float32
	Updated time.Time
}
