package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/urfave/cli/v2"
)

// // BEGIN config
const (
	INSERT_STATION_DATA_BATCH_SIZE    = 250
	INSERT_STATION_DATA_LOG_INTERVALL = 100_000
)

//// END config

// Queries for clickhouse
const (
	CREATE_STATIONS = `create table if not exists stations (
		id           String,
		display_name String,
		country      String,
		latitude     FLOAT,
		longitude    FLOAT,
		timezone     String
	) engine = MergeTree ORDER BY id PRIMARY KEY(id);`
	CREATE_STATION_DATA = `create table if not exists station_data (
		station String,
		measured_at DateTime64,
		temp Nullable(Float32),
		dwpt Nullable(Float32),
		rhum Nullable(Int16),
		prcp Nullable(Float32),
		snow Nullable(Int16),
		wdir Nullable(Int16),
		wspd Nullable(Float32),
		wpgt Nullable(Float32),
		pres Nullable(Float32),
		tsun Nullable(Int16),
		coco Nullable(Int16)
	) engine = MergeTree ORDER BY measured_at PARTITION BY station`
	INSERT_STATION      = `INSERT INTO stations`
	INSERT_STATION_DATA = `INSERT INTO station_data`
)

// Structure description of station details file
type StationDetails struct {
	Id       string            `json:"id"`
	Name     map[string]string `json:"name"`
	Country  string            `json:"country"`
	Location struct {
		Latitude  float32 `json:"latitude"`
		Longitude float32 `json:"longitude"`
		Elevation int     `json:"elevation"`
	} `json:"location"`
	Timezone string `json:"timezone"`
}

var (
	ErrFailedDownload = errors.New("failed file download")
)

func downloadFile(url string, filepath string) error {
	// Create the file
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	// Get the data
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Do not write non-OK status code response to file
	if resp.StatusCode != http.StatusOK {
		return ErrFailedDownload
	}

	// Writer the body to file
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}

	return nil
}

func getEnvOrDefault(key string, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}

func parseFloat32OrNil(s string) *float32 {
	v, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return nil
	}
	v32 := float32(v)
	return &v32
}

func parseInt16OrNil(s string) *int16 {
	v, err := strconv.ParseInt(s, 10, 16)
	if err != nil {
		return nil
	}
	v16 := int16(v)
	return &v16
}

func main() {
	// Get env values
	dataFolder := getEnvOrDefault("DATA_DIR", "data")
	clickhouseAddr := getEnvOrDefault("CLICKHOUSE_ADDR", "localhost:9000")
	clickhouseDb := getEnvOrDefault("CLICKHOUSE_DB", "default")
	clickhouseUsername := getEnvOrDefault("CLICKHOUSE_USERNAME", "")
	clickhousePassword := getEnvOrDefault("CLICKHOUSE_PASSWORD", "")

	// Connect to clickhouse
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{clickhouseAddr},
		Auth: clickhouse.Auth{
			Database: clickhouseDb,
			Username: clickhouseUsername,
			Password: clickhousePassword,
		},
	})
	defer conn.Close()

	// Setup cli commands
	app := &cli.App{
		Name:  "meteostat",
		Usage: "import tool",
		Commands: []*cli.Command{
			{
				Name:  "setup",
				Usage: "setup database and environment",
				Action: func(cCtx *cli.Context) error {
					_, err := conn.Exec(CREATE_STATIONS)
					if err != nil {
						return err
					}

					_, err = conn.Exec(CREATE_STATION_DATA)
					if err != nil {
						return err
					}

					return nil
				},
			},
			{
				Name:    "import",
				Aliases: []string{"i"},
				Usage:   "import tools",
				Subcommands: []*cli.Command{
					{
						Name:  "stations",
						Usage: "import station details",
						Action: func(cCtx *cli.Context) error {
							//// Download station details folder
							filepath := path.Join(dataFolder, "full.json.gz")
							err := downloadFile("https://bulk.meteostat.net/v2/stations/full.json.gz", filepath)
							if err != nil {
								return err
							}

							//// Import into database
							// Open file
							fp, err := os.Open(filepath)
							if err != nil {
								return err
							}
							defer fp.Close()
							// Use GZIP reader
							gr, err := gzip.NewReader(fp)
							if err != nil {
								return err
							}
							// Create new decoder
							dec := json.NewDecoder(gr)
							// Read open bracket of array
							if _, err := dec.Token(); err != nil {
								return err
							}
							// While we have more items
							for dec.More() {
								// Decode item
								var sd StationDetails
								if err := dec.Decode(&sd); err != nil {
									return err
								}
								// Insert into database
								if _, err := conn.Exec(INSERT_STATION, sd.Id, sd.Name["en"], sd.Country, sd.Location.Latitude, sd.Location.Longitude, sd.Timezone); err != nil {
									return err
								}
							}
							// Read closing bracket
							if _, err := dec.Token(); err != nil {
								return err
							}

							return nil
						},
					},
					{
						Name:  "data",
						Usage: "import station details",
						Action: func(cCtx *cli.Context) error {
							stationId := strings.TrimSpace(cCtx.Args().Get(0))
							log.Printf("Importing data for station %v...", stationId)
							//// Download station details folder
							filepath := path.Join(dataFolder, fmt.Sprintf("%v.csv.gz", stationId))
							if _, err := os.Stat(filepath); errors.Is(err, os.ErrNotExist) {
								if err := downloadFile(fmt.Sprintf("https://bulk.meteostat.net/v2/hourly/%v.csv.gz", stationId), filepath); err != nil {
									return err
								}
							}

							//// Query sation details
							var stationTimezone string
							r := conn.QueryRow("SELECT timezone FROM stations WHERE id=?", stationId)
							if err := r.Scan(&stationTimezone); err != nil {
								return err
							}
							timeLocation, err := time.LoadLocation(stationTimezone)
							if err != nil {
								return err
							}

							//// Import into database
							// TODO: Better batch handling
							// Batch tx
							batch, err := conn.Begin()
							if err != nil {
								return err
							}
							// Prepare insert statement
							stmt, err := batch.Prepare(INSERT_STATION_DATA)
							if err != nil {
								return err
							}

							// Open file
							fp, err := os.Open(filepath)
							if err != nil {
								return err
							}
							defer fp.Close()
							// Use GZIP reader
							gr, err := gzip.NewReader(fp)
							if err != nil {
								return err
							}
							// Use CSV reader
							cr := csv.NewReader(gr)
							// Read all rows
							var count uint64 = 0
							for {
								// Read record
								rec, err := cr.Read()
								// Handle EOF
								if err == io.EOF {
									break
								}
								if err != nil {
									return err
								}
								// Parse columns
								date := rec[0]
								hour := rec[1]
								datetime, err := time.ParseInLocation("2006-01-02 15:00:00", fmt.Sprintf("%v %v:00:00", date, hour), timeLocation)
								if err != nil {
									return err
								}
								// NOTE: We need to parse the strings as floats/integers as the client does not support "inserting float/integer as a string" (even though this is possible as a raw query)
								temp, dwpt, rhum, prcp, snow, wdir, wspd, wpgt, pres, tsun, coco := parseFloat32OrNil(rec[2]), parseFloat32OrNil(rec[3]), parseInt16OrNil(rec[4]), parseFloat32OrNil(rec[5]), parseInt16OrNil(rec[6]), parseInt16OrNil(rec[7]), parseFloat32OrNil(rec[8]), parseFloat32OrNil(rec[9]), parseFloat32OrNil(rec[10]), parseInt16OrNil(rec[11]), parseInt16OrNil(rec[12])
								// Add to current batch
								if _, err := stmt.Exec(stationId, datetime, temp, dwpt, rhum, prcp, snow, wdir, wspd, wpgt, pres, tsun, coco); err != nil {
									return err
								}
								// Count new records amount
								count += 1
								// Check for batch submit
								if count%INSERT_STATION_DATA_BATCH_SIZE == 0 {
									// Commit current batch
									if err := batch.Commit(); err != nil {
										return err
									}
									// New batch tx
									batch, err = conn.Begin()
									if err != nil {
										return err
									}
									// Clear up old statement
									if err := stmt.Close(); err != nil {
										return err
									}
									// Prepare insert statement
									stmt, err = batch.Prepare(INSERT_STATION_DATA)
									if err != nil {
										return err
									}
								}
								// Progress status
								if count%INSERT_STATION_DATA_LOG_INTERVALL == 0 {
									log.Printf("Inserted %d records...", count)
								}
							}

							// Final batch submit
							if err := batch.Commit(); err != nil {
								return err
							}
							// Clean up preared statement
							if err := stmt.Close(); err != nil {
								return err
							}

							return nil

						},
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
