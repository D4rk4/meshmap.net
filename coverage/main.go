package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/paulmach/go.geojson"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/planar"
	"golang.org/x/time/rate"
)

const (
	cacheExpiration      = 365 * 24 * time.Hour // 1 year
	maxConcurrentFetches = 5                    // Maximum number of concurrent API requests
	updateInterval       = 5 * time.Minute      // Interval between coverage updates
	defaultCachePath     = "/tmp/elevation_cache.duckdb"
	hexRadiusMeters      = 15.0 // Target hex radius for coverage/binning
	defaultNodesURL      = "http://meshobserv/nodes.json"
	defaultHTTPAddr      = ":80"
)

var (
	fetchSemaphore = make(chan struct{}, maxConcurrentFetches) // Semaphore channel
)

// Define rate limiters for both APIs
var (
	openElevationLimiter = rate.NewLimiter(rate.Every(500*time.Millisecond), 1)
	openTopodataLimiter  = rate.NewLimiter(rate.Every(500*time.Millisecond), 1)
)

// initialize the DuckDB cache database and ensure schema exists
func initCacheDB(path string) (*sql.DB, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	// DuckDB driver does not support multiple concurrent connections; keep pool to one.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS elevation_cache (
		lat DOUBLE,
		lon DOUBLE,
		elevation DOUBLE,
		timestamp TIMESTAMP,
		UNIQUE(lat, lon)
	)`); err != nil {
		db.Close()
		return nil, fmt.Errorf("init cache schema: %w", err)
	}

	return db, nil
}

func cacheDBPathValue() string {
	if v := strings.TrimSpace(os.Getenv("CACHE_DB_PATH")); v != "" {
		return v
	}
	return defaultCachePath
}

func ensureCachePath(path string) error {
	dir := filepath.Dir(path)
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0755)
}

// snapToHex snaps a lat/lon to the center of the nearest hex cell with the given radius (in meters).
// This reduces cache cardinality and downstream point counts.
func snapToHex(lat, lon float64) (float64, float64) {
	// Convert lat/lon to a local planar coordinate system in meters (equirectangular projection).
	latRad := lat * math.Pi / 180.0
	lonRad := lon * math.Pi / 180.0
	x := earthRadiusMeters() * lonRad * math.Cos(latRad)
	y := earthRadiusMeters() * latRad

	size := hexRadiusMeters
	q := (math.Sqrt(3)/3*x - 1.0/3*y) / size
	r := (2.0 / 3.0 * y) / size
	s := -q - r

	rq := math.Round(q)
	rr := math.Round(r)
	rs := math.Round(s)

	qDiff := math.Abs(rq - q)
	rDiff := math.Abs(rr - r)
	sDiff := math.Abs(rs - s)

	if qDiff > rDiff && qDiff > sDiff {
		rq = -rr - rs
	} else if rDiff > sDiff {
		rr = -rq - rs
	} else {
		rs = -rq - rr
	}

	// Convert hex axial coords back to x/y meters (pointy-topped layout).
	xHex := size * (math.Sqrt(3)*rq + math.Sqrt(3)/2*rr)
	yHex := size * (1.5 * rr)

	latOutRad := yHex / earthRadiusMeters()
	lonOutRad := xHex / (earthRadiusMeters() * math.Cos(latOutRad))

	return latOutRad * 180.0 / math.Pi, lonOutRad * 180.0 / math.Pi
}

func earthRadiusMeters() float64 {
	return 6371000.0
}

// Node represents a Meshtastic node with relevant data
type Node struct {
	ID        string  `json:"id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Altitude  float64 `json:"altitude"`
}

// NodeData holds the mapping from node IDs to Nodes
type NodeData map[string]Node

// OpenElevationResult represents the JSON structure returned by the Open-Elevation API
type OpenElevationResult struct {
	Results []struct {
		Elevation float64 `json:"elevation"`
		Location  struct {
			Latitude  float64 `json:"latitude"`
			Longitude float64 `json:"longitude"`
		} `json:"location"`
	} `json:"results"`
}

// OpenTopodataResult represents the JSON structure returned by the OpenTopodata API
type OpenTopodataResult struct {
	Results []struct {
		Dataset   string  `json:"dataset"`
		Elevation float64 `json:"elevation"`
		Location  struct {
			Lat float64 `json:"lat"`
			Lng float64 `json:"lng"`
		} `json:"location"`
	} `json:"results"`
	Status string `json:"status"`
}

type rawNode struct {
	Latitude  int32 `json:"latitude"`
	Longitude int32 `json:"longitude"`
	Altitude  int32 `json:"altitude"`
}

// LoadNodeData fetches node data from the meshobserv HTTP endpoint.
func LoadNodeData(ctx context.Context, url string, db *sql.DB) (NodeData, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch nodes: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch nodes: status %d", resp.StatusCode)
	}

	var raw map[string]rawNode
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode nodes: %w", err)
	}

	nodes := make(NodeData, len(raw))
	for id, n := range raw {
		if n.Latitude == 0 && n.Longitude == 0 {
			continue
		}
		lat := float64(n.Latitude) / 1e7
		lon := float64(n.Longitude) / 1e7
		alt := float64(n.Altitude)

		if alt == 0 {
			elevation, err := GetElevation(lat, lon, db)
			if err != nil {
				log.Printf("[warn] node %s altitude missing; using 0m (elevation lookup failed: %v)", id, err)
			} else {
				alt = elevation + 2.0
				log.Printf("[info] node %s altitude missing; using elevation %.2fm + 2m", id, alt)
			}
		}

		nodes[id] = Node{
			ID:        id,
			Latitude:  lat,
			Longitude: lon,
			Altitude:  alt,
		}
	}

	return nodes, nil
}

// GetElevation retrieves the elevation for given coordinates from cache.
// If not present, it fetches from the API with concurrency control and caches the result.
func GetElevation(lat, lon float64, db *sql.DB) (float64, error) {
	lat, lon = snapToHex(lat, lon)

	var elevation float64
	var ts time.Time
	err := db.QueryRow(`SELECT elevation, timestamp FROM elevation_cache WHERE lat = ? AND lon = ?`, lat, lon).
		Scan(&elevation, &ts)
	if err == nil {
		if time.Since(ts) <= cacheExpiration {
			return elevation, nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("cache lookup failed: %w", err)
	}

	// Cache miss or expired, fetch synchronously with concurrency control
	fetchSemaphore <- struct{}{}
	defer func() { <-fetchSemaphore }()

	elevation, err = GetElevationFromAPI(lat, lon)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch elevation for (%.6f, %.6f): %w", lat, lon, err)
	}

	if _, err := db.Exec(
		`INSERT OR REPLACE INTO elevation_cache (lat, lon, elevation, timestamp) VALUES (?, ?, ?, ?)`,
		lat, lon, elevation, time.Now(),
	); err != nil {
		return 0, fmt.Errorf("failed to store elevation for (%.6f, %.6f): %w", lat, lon, err)
	}

	log.Printf("[info] cached elevation for (%.6f, %.6f): %.2f", lat, lon, elevation)
	return elevation, nil
}

// GetElevationFromAPI fetches elevation data with fallback and rate limiting
func GetElevationFromAPI(lat, lon float64) (float64, error) {
	var openElevationErr error

	// Attempt to fetch from Open-Elevation API with rate limiting and retry
	elevation, err := fetchWithRetryAndRateLimit(fetchFromOpenElevation, openElevationLimiter, lat, lon)
	if err == nil {
		return elevation, nil
	}
	openElevationErr = err

	// Log the fallback attempt
	log.Printf("WARNING: Open-Elevation API failed for (%.6f, %.6f): %v. Attempting fallback to OpenTopodata API.", lat, lon, err)

	// Attempt to fetch from OpenTopodata API with rate limiting and retry
	elevation, err = fetchWithRetryAndRateLimit(fetchFromOpenTopodata, openTopodataLimiter, lat, lon)
	if err == nil {
		return elevation, nil
	}

	// Both APIs failed
	log.Printf("ERROR: Both Open-Elevation and OpenTopodata APIs failed for (%.6f, %.6f).", lat, lon)
	return 0, fmt.Errorf("both Open-Elevation and OpenTopodata APIs failed: open-elevation: %v; open-topodata: %v", openElevationErr, err)
}

// fetchWithRetryAndRateLimit handles rate limiting and retries for API calls
func fetchWithRetryAndRateLimit(apiFunc func(float64, float64) (float64, error), limiter *rate.Limiter, lat, lon float64) (float64, error) {
	var elevation float64
	var err error

	// Define retry parameters
	maxRetries := 3
	initialBackoff := 1 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Wait for rate limiter
		if err := limiter.Wait(context.Background()); err != nil {
			log.Printf("ERROR: Rate limiter wait failed: %v", err)
			return 0, err
		}

		// Call the API function
		elevation, err = apiFunc(lat, lon)
		if err == nil {
			return elevation, nil
		}

		// Check if error is due to rate limiting
		if isRateLimitError(err) {
			// Extract Retry-After duration from error message or context
			retryAfter := extractRetryAfterFromError(err)
			if retryAfter > 0 {
				log.Printf("INFO: Received Retry-After: %v. Sleeping before retrying...", retryAfter)
				time.Sleep(retryAfter)
			} else {
				// Exponential backoff
				backoff := initialBackoff * time.Duration(1<<uint(attempt-1)) // 1s, 2s, 4s
				log.Printf("INFO: Rate limited. Backing off for %v before retrying...", backoff)
				time.Sleep(backoff)
			}
		} else {
			// For non-rate limit errors, decide whether to retry or not
			log.Printf("INFO: Non-rate limit error occurred: %v. Retrying...", err)
			time.Sleep(initialBackoff * time.Duration(attempt)) // Linear backoff
		}
	}

	return 0, err
}

// isRateLimitError determines if an error is due to rate limiting
func isRateLimitError(err error) bool {
	// Corrected to use err.Error() instead of fmt.Sprintf(err)
	return strings.Contains(err.Error(), "status code 429")
}

// extractRetryAfterFromError parses the Retry-After from the error message
func extractRetryAfterFromError(err error) time.Duration {
	// Example error message: "Open-Elevation API returned status code 429; Retry-After: 10"
	msg := err.Error()
	parts := strings.Split(msg, "Retry-After: ")
	if len(parts) == 2 {
		retryStr := strings.TrimSpace(parts[1])
		// Attempt to parse explicit duration (e.g., 10s)
		if d, parseErr := time.ParseDuration(retryStr); parseErr == nil {
			return d
		}
		// Attempt to parse as seconds
		if seconds, err := strconv.Atoi(retryStr); err == nil {
			return time.Duration(seconds) * time.Second
		}
		// Attempt to parse as HTTP-date
		if retryTime, err := http.ParseTime(retryStr); err == nil {
			return time.Until(retryTime)
		}
	}
	return 0
}

// fetchFromOpenElevation fetches elevation from Open-Elevation API
func fetchFromOpenElevation(lat, lon float64) (float64, error) {
	url := fmt.Sprintf("https://api.open-elevation.com/api/v1/lookup?locations=%.6f,%.6f", lat, lon)

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		log.Printf("ERROR: Failed to send request to Open-Elevation API: %v", err)
		return 0, fmt.Errorf("failed to send request to Open-Elevation API: %v; Retry-After: 10", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		log.Printf("ERROR: Open-Elevation API rate limited with status code %d for (%.6f, %.6f). Retry after %v.", resp.StatusCode, lat, lon, retryAfter)
		return 0, fmt.Errorf("Open-Elevation API returned status code %d; Retry-After: %v", resp.StatusCode, retryAfter)
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("ERROR: Open-Elevation API returned status code %d for (%.6f, %.6f)", resp.StatusCode, lat, lon)
		return 0, fmt.Errorf("Open-Elevation API returned status code %d", resp.StatusCode)
	}

	var result OpenElevationResult
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		log.Printf("ERROR: Failed to decode Open-Elevation API response: %v", err)
		return 0, fmt.Errorf("failed to decode Open-Elevation API response: %v; Retry-After: 10", err)
	}

	if len(result.Results) == 0 {
		log.Printf("ERROR: Open-Elevation API returned empty results for (%.6f, %.6f)", lat, lon)
		return 0, fmt.Errorf("Open-Elevation API returned empty results; Retry-After: 10")
	}

	return result.Results[0].Elevation, nil
}

// fetchFromOpenTopodata fetches elevation from OpenTopodata API
func fetchFromOpenTopodata(lat, lon float64) (float64, error) {
	url := fmt.Sprintf("https://api.opentopodata.org/v1/srtm30m?locations=%.6f,%.6f", lat, lon)

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		log.Printf("ERROR: Failed to send request to OpenTopodata API: %v", err)
		return 0, fmt.Errorf("failed to send request to OpenTopodata API: %v; Retry-After: 10", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		log.Printf("ERROR: OpenTopodata API rate limited with status code %d for (%.6f, %.6f). Retry after %v.", resp.StatusCode, lat, lon, retryAfter)
		return 0, fmt.Errorf("OpenTopodata API returned status code %d; Retry-After: %v", resp.StatusCode, retryAfter)
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("ERROR: OpenTopodata API returned status code %d for (%.6f, %.6f)", resp.StatusCode, lat, lon)
		return 0, fmt.Errorf("OpenTopodata API returned status code %d", resp.StatusCode)
	}

	var result OpenTopodataResult
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		log.Printf("ERROR: Failed to decode OpenTopodata API response: %v", err)
		return 0, fmt.Errorf("failed to decode OpenTopodata API response: %v; Retry-After: 10", err)
	}

	if result.Status != "OK" {
		log.Printf("ERROR: OpenTopodata API returned status: %s for (%.6f, %.6f)", result.Status, lat, lon)
		return 0, fmt.Errorf("OpenTopodata API returned status: %s; Retry-After: 10", result.Status)
	}

	if len(result.Results) == 0 {
		log.Printf("ERROR: OpenTopodata API returned empty results for (%.6f, %.6f)", lat, lon)
		return 0, fmt.Errorf("OpenTopodata API returned empty results; Retry-After: 10")
	}

	return result.Results[0].Elevation, nil
}

// parseRetryAfter parses the Retry-After header value into a time.Duration
func parseRetryAfter(header string) time.Duration {
	if header == "" {
		return 0
	}

	// Try to parse as delta-seconds
	if seconds, err := strconv.Atoi(header); err == nil {
		return time.Duration(seconds) * time.Second
	}

	// Try to parse as HTTP-date
	if retryTime, err := http.ParseTime(header); err == nil {
		return time.Until(retryTime)
	}

	return 0
}

// ComputeCoveragePolygon computes the coverage polygon for a node using elevation data
func ComputeCoveragePolygon(node Node, db *sql.DB) ([][][]float64, error) {
	numRadials := 360      // One per degree
	maxDistance := 20000.0 // Max distance in meters (e.g., 20 km)
	stepSize := 500.0      // Distance between points along radial in meters

	// Node antenna height (altitude + 2m antenna)
	h1 := node.Altitude + 2.0

	var points [][]float64

	for angle := 0; angle < numRadials; angle++ {
		rad := float64(angle) * (math.Pi / 180)
		var lastPoint []float64

		for dist := stepSize; dist <= maxDistance; dist += stepSize {
			dx := dist * math.Cos(rad)
			dy := dist * math.Sin(rad)

			// Convert distance offsets to lat/lon
			dLat := (dy / 111320.0) // Approximate conversion
			dLon := (dx / (40075000.0 * math.Cos(node.Latitude*(math.Pi/180)) / 360))

			lat := node.Latitude + dLat
			lon := node.Longitude + dLon

			// Get terrain elevation at this point
			elevation, err := GetElevation(lat, lon, db)
			if err != nil {
				log.Printf("[warn] skipping radial at angle %d; missing elevation for (%.6f, %.6f)", angle, lat, lon)
				break
			}

			// Simple LOS check
			deltaElevation := elevation - h1
			elevationAngle := math.Atan2(deltaElevation, dist)

			if elevationAngle > 0 {
				// Obstacle found
				break
			} else {
				lastPoint = []float64{lon, lat}
			}
		}

		if lastPoint != nil {
			points = append(points, lastPoint)
		}
	}

	// Close the polygon
	if len(points) > 0 {
		points = append(points, points[0])
	}

	// Return the coordinates as [][][]float64
	coordinates := [][][]float64{points}
	return coordinates, nil
}

// samplePointsInPolygon samples random points within a polygon
func samplePointsInPolygon(polygonCoords [][][]float64, numPoints int) []orb.Point {
	if numPoints <= 0 {
		return nil
	}

	var points []orb.Point

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Convert polygon coordinates to orb.Polygon
	var rings []orb.Ring
	for _, ringCoords := range polygonCoords {
		if len(ringCoords) < 3 {
			continue
		}
		var ring orb.Ring
		for _, coord := range ringCoords {
			if len(coord) < 2 {
				continue
			}
			ring = append(ring, orb.Point{coord[0], coord[1]})
		}
		if len(ring) >= 3 {
			rings = append(rings, ring)
		}
	}
	if len(rings) == 0 {
		return nil
	}
	polygon := orb.Polygon(rings)

	// Get bounding box
	bbox := polygon.Bound()
	if bbox.IsEmpty() {
		return nil
	}
	minX, minY := bbox.Min[0], bbox.Min[1]
	maxX, maxY := bbox.Max[0], bbox.Max[1]

	maxAttempts := numPoints * 50
	for attempts := 0; len(points) < numPoints && attempts < maxAttempts; attempts++ {
		x := minX + r.Float64()*(maxX-minX)
		y := minY + r.Float64()*(maxY-minY)
		point := orb.Point{x, y}
		if planar.PolygonContains(polygon, point) {
			points = append(points, point)
		}
	}

	return points
}

// GenerateCoveragePointsGeoJSON creates point features for the coverage heatmap and returns GeoJSON bytes.
func GenerateCoveragePointsGeoJSON(nodes NodeData, db *sql.DB) ([]byte, error) {
	var features []*geojson.Feature
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(node Node) {
			defer wg.Done()
			log.Printf("[info] processing node %s at (%.6f, %.6f)", node.ID, node.Latitude, node.Longitude)
			polygonCoords, err := ComputeCoveragePolygon(node, db)
			if err != nil {
				log.Printf("[warn] coverage polygon failed for node %s: %v", node.ID, err)
				return
			}

			// Sample points within the coverage polygon
			sampledPoints := samplePointsInPolygon(polygonCoords, 7500) // Adjust number of points as needed
			if len(sampledPoints) == 0 {
				log.Printf("[info] node %s: no coverage points generated (empty or invalid polygon)", node.ID)
				return
			}

			hexCounts := make(map[[2]float64]int)
			for _, point := range sampledPoints {
				lat, lon := point[1], point[0]
				snappedLat, snappedLon := snapToHex(lat, lon)
				key := [2]float64{snappedLon, snappedLat}
				hexCounts[key]++
			}

			mu.Lock()
			for key, count := range hexCounts {
				feature := geojson.NewPointFeature([]float64{key[0], key[1]})
				feature.SetProperty("node_id", node.ID)
				feature.SetProperty("intensity", count)
				features = append(features, feature)
			}
			mu.Unlock()
			log.Printf("[info] node %s: added %d coverage points (%d sampled -> %d hexes)", node.ID, len(hexCounts), len(sampledPoints), len(hexCounts))
		}(node)
	}

	wg.Wait()

	if len(features) == 0 {
		log.Print("[warn] no coverage points generated; ensure elevation data is available")
	}

	featureCollection := geojson.NewFeatureCollection()
	featureCollection.Features = features

	data, err := featureCollection.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GeoJSON: %v", err)
	}

	log.Printf("[info] coverage points generated with %d features", len(features))
	return data, nil
}

func nodesURL() string {
	if v := strings.TrimSpace(os.Getenv("NODES_URL")); v != "" {
		return v
	}
	return defaultNodesURL
}

func httpListenAddr() string {
	if v := strings.TrimSpace(os.Getenv("COVERAGE_HTTP_ADDR")); v != "" {
		return v
	}
	return defaultHTTPAddr
}

// generateCoverage orchestrates loading node data and generating coverage points
func generateCoverage(ctx context.Context, db *sql.DB, nodesEndpoint string) ([]byte, error) {
	nodes, err := LoadNodeData(ctx, nodesEndpoint, db)
	if err != nil {
		return nil, fmt.Errorf("load nodes: %w", err)
	}

	data, err := GenerateCoveragePointsGeoJSON(nodes, db)
	if err != nil {
		return nil, fmt.Errorf("build coverage geojson: %w", err)
	}

	return data, nil
}

func main() {
	nodesEndpoint := nodesURL()
	coverageHTTPAddr := httpListenAddr()
	cachePath := cacheDBPathValue()

	log.Printf("[info] nodes_url=%s cache_db=%s listen=%s", nodesEndpoint, cachePath, coverageHTTPAddr)

	if err := ensureCachePath(cachePath); err != nil {
		log.Fatalf("[fatal] ensure cache path: %v", err)
	}

	db, err := initCacheDB(cachePath)
	if err != nil {
		log.Fatalf("[fatal] open cache database: %v", err)
	}
	defer db.Close()

	var coverageJSON atomic.Value
	coverageJSON.Store([]byte(`{"type":"FeatureCollection","features":[]}`))

	run := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		log.Print("[info] generating coverage points")
		data, err := generateCoverage(ctx, db, nodesEndpoint)
		if err != nil {
			log.Printf("[warn] coverage generation failed: %v", err)
			return
		}
		coverageJSON.Store(data)
		log.Printf("[info] coverage points refreshed (%d bytes)", len(data))
	}

	run()

	mux := http.NewServeMux()
	mux.HandleFunc("/coverage_points.geojson", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/geo+json")
		if data, ok := coverageJSON.Load().([]byte); ok && len(data) > 0 {
			_, _ = w.Write(data)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"type":"FeatureCollection","features":[]}`))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	server := &http.Server{
		Addr:              coverageHTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("[info] coverage http listening on %s", coverageHTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[fatal] http server failed: %v", err)
		}
	}()

	ticker := time.NewTicker(updateInterval)
	defer ticker.Stop()
	for range ticker.C {
		run()
	}
}
