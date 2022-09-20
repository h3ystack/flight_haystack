# Flight_haystack

This set of Python scripts allows to to easily query the [OpenSkyNetwork API](https://openskynetwork.github.io/opensky-api/rest.html), searching for flights from given aircrafts and from/to given airports, on given date periods.

It saves the flights to a PostgreSQL database you can then use to derive statistics for given aircrafts/airports.



## Getting Started

This is an example of how you may give instructions on setting up your project locally.
To get a local copy up and running follow these simple example steps.

### Prerequisites

You will need the following Python libraries.

  ```sh
time
datetime
requests
psycopg2
  ```

And you will also need an PostgreSQL database (remote or on localhost) to save the flights
- Multi-platform downloads for : [PostgreSQL](https://www.postgresql.org/download/)
- Tools to configure/monitor/query your database: PGadmin4, DBeaver and many others...


### Configuration:

The whole configuration is contained in the `/config/haystack_config.py` file

1. Once you have a PostgreSQL database running you will need to provide the database hostname,name, user and password

```
host= 'localhost'
user='postgres'
database='yourdatabase'
#password =     Add a password if you wish e.g if running a remote DB instance.
```

2. Once this is done, you can run the `create_database.py` script that will create a `data` schema containing three tables:
```sh
> flights (will be filled by the flights collected by the scripts)
> aircraft (that you can fill once with data from /data_sources/aircraftDatabase.zip -- once unzipped)
> airports (that you can fill once with data from /data_sources/airports.csv)
```





<!-- USAGE EXAMPLES -->
## Usage
### Collecting flight data:

There are two main scripts that you can use:
- `get_flights_for_aircraft.py` which will query OpenSky for flights from a given list of aircrafts on a given date period.
- `get_all_flights_for_aircraft.py` which will query OpenSky for inbound and outbound flights from a given list of airports, on a given date period.

Dates, Aircrafts and Airports are defined in the `/config/haystack_config.py` file.

```
start = "16/09/2022" # Start date in dd/mm/yyyy format
stop = "25/09/2022" # End date in dd/mm/yyyy format, scripts will automatically stop at the current date anyhow
airports = ['KJFK','LFPG'] # List of airports from their ICAO Ident code
icao24 = ['486201','4007ee'] # List of airplanes from their ICAO24 Hex codes
```

Once set, launch ay script from your IDE or python console
Any new found flight will be added to the database, duplicate flights are not inserted.

### Finding flights in the database once collected:

Open a database GUI tool and run a query in the Flights table:



### Known limitations:
- The scripts don't fully handle timeouts on the OpenSky API. They currently crash and need to be re-launched.
- At the OpenSky level, estimated departure and arrival airports are not always accurate, you might want to search fro nearby airports as well.

