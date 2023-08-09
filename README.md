# Meteostat Clickhouse Playground

Tool to import data from meteostat into clickhouse to play around in grafana.

## Installation

```bash
# Clone project
git clone https://github.com/oltdaniel/meteostat-clickhouse-playground
cd meteostat-clickhouse-playground
# Start clickhouse and grafana server
docker compose up -d
# Setup database schema
go run main.go setup
# Import station details
go run main.go import stations
# Import data for a specific station
go run main.go import data <STATION_ID>
```

## License

_do what you'd like to do_

![GitHub](https://img.shields.io/github/license/oltdaniel/meteostat-clickhouse-playground)