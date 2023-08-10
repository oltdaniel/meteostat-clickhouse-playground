# Meteostat Clickhouse Playground

Tool to import data from meteostat into clickhouse to play around in grafana.

![grafik](https://github.com/oltdaniel/meteostat-clickhouse-playground/assets/53529846/0468d5a9-505e-45e8-98d8-8c78091b1a44)

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
go run main.go import data <STATION_ID> # <STATION_ID...>
```

## Starting in Grafana

1. Call `https://localhost:3000`, signin with `admin/admin` and set a new password.
2. Install clickhouse source on `http://localhost:3000/connections/datasources/grafana-clickhouse-datasource`.
3. Create a clickhouse data source (button on top right on step 2).
4. Use `Server address = clickhouse` and `Server port = 9000`, Save & Test.
5. Explore the imported data on `http://localhost:3000/explore`.

Here are a few (random) example stations you could import:
- `10147`: Hamburg Airport (Germany)
- `94767`: Sydney Airport (Australia)
- `01141`: Leknes Airport (Norway)
- `03772`: London Heathrow Airport (England, United Kingdom)
- `72259`: Dallas/Ft. Worth International  Airport (United States)
- `76679`: Mexico City Airport (Mexico)
- `48698`: Singapore / Changi Airport (Singapore)
- `40007`: Aleppo International Airport (Syria)

> Import all of the above stations with this command: `go run main.go import data 10147 94767 01141 03772 72259 76679 48698 40007` (about ~3 million records)

## Clickhouse Tables

```sql
-- See https://dev.meteostat.net/bulk/stations.html
create table if not exists stations (
	id           String,
	display_name String,
	country      String,
	latitude     FLOAT,
	longitude    FLOAT,
	timezone     String
) engine = MergeTree ORDER BY id PRIMARY KEY(id);

-- See https://dev.meteostat.net/bulk/hourly.html#endpoints
create table if not exists station_data (
	station String,
	measured_at DateTime64, -- this is parsed based on the reported date and hour, converted to the stations timezone
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
) engine = MergeTree ORDER BY measured_at PARTITION BY station;
```

## License

_do what you'd like to do_

![GitHub](https://img.shields.io/github/license/oltdaniel/meteostat-clickhouse-playground)
