# Meteostat Clickhouse Playground

Tool to import data from meteostat into clickhouse to play around in grafana.

![grafik](https://github.com/oltdaniel/meteostat-clickhouse-playground/assets/53529846/6d7a8cf5-08bc-4b3b-ba96-e201cdcc196d)
> Example Heatmap showing the coverage of the different weather stations listed from meteostat.

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

## Starting in Grafana

1. Call `https://localhost:3000`, signin with `admin/admin` and set a new password.
2. Install clickhouse source on `http://localhost:3000/connections/datasources/grafana-clickhouse-datasource`.
3. Create a clickhouse data source (button on top right on step 2).
4. Use `Server address = clickhouse` and `Server port = 9000`, Save & Test.
5. Explore the imported data on `http://localhost:3000/explore`.

## License

_do what you'd like to do_

![GitHub](https://img.shields.io/github/license/oltdaniel/meteostat-clickhouse-playground)
