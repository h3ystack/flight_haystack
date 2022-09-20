-- DROP SCHEMA "data";

CREATE SCHEMA "data" AUTHORIZATION postgres;
-- "data".aircrafts definition

-- Drop table

-- DROP TABLE "data".aircrafts;

CREATE TABLE "data".aircrafts (
	icao24 varchar NOT NULL,
	registration varchar NULL,
	manufacturericao varchar NULL,
	manufacturername varchar NULL,
	model varchar NULL,
	typecode varchar NULL,
	serialnumber varchar NULL,
	linenumber varchar NULL,
	icaoaircrafttype varchar NULL,
	"operator" varchar NULL,
	operatorcallsign varchar NULL,
	operatoricao varchar NULL,
	operatoriata varchar NULL,
	"owner" varchar NULL,
	testreg varchar NULL,
	registered varchar NULL,
	reguntil varchar NULL,
	status varchar NULL,
	built varchar NULL,
	firstflightdate varchar NULL,
	seatconfiguration varchar NULL,
	engines varchar NULL,
	modes bool NULL,
	adsb bool NULL,
	acars bool NULL,
	notes varchar NULL,
	categorydescription varchar NULL
);


-- "data".airports definition

-- Drop table

-- DROP TABLE "data".airports;

CREATE TABLE "data".airports (
	iata_code varchar NULL,
	ident varchar NULL,
	gps_code varchar NULL,
	"name" varchar NULL,
	latitude_deg float4 NULL,
	longitude_deg float4 NULL,
	elevation_ft int4 NULL,
	continent varchar NULL,
	iso_country varchar NULL,
	iso_region varchar NULL,
	municipality varchar NULL,
	"type" varchar NULL
);


-- "data".flights definition

-- Drop table

-- DROP TABLE "data".flights;

CREATE TABLE "data".flights (
	icao24 varchar NOT NULL,
	firstseen timestamp NOT NULL,
	estdepartureairport varchar NULL,
	lastseen timestamp NOT NULL,
	estarrivalairport varchar NULL,
	callsign varchar NULL,
	estdepartureairporthorizdistance float8 NULL,
	estdepartureairportvertdistance float8 NULL,
	estarrivalairporthorizdistance float8 NULL,
	estarrivalairportvertdistance float8 NULL,
	departureairportcandidatescount int4 NULL,
	arrivalairportcandidatescount int4 NULL,
	duration float8 NULL,
	trip varchar NULL GENERATED ALWAYS AS (
CASE
    WHEN estdepartureairport IS NULL THEN 'Unknown - '::text ||estarrivalairport::text
    WHEN estarrivalairport IS NULL THEN estdepartureairport::text || 'Unknown - '::text
    WHEN estarrivalairport IS NULL AND estdepartureairport IS NULL THEN 'Unknown - Unknown'::text
    ELSE (estdepartureairport::text || ' - '::text) || estarrivalairport::text
END) STORED,
	CONSTRAINT flights_un UNIQUE (icao24, firstseen, lastseen)
);
CREATE INDEX flights_trip_idx ON data.flights USING btree (trip);


-- "data".aircraft_stats source

CREATE OR REPLACE VIEW "data".aircraft_stats
AS SELECT f.icao24,
    count(DISTINCT date_trunc('month'::text, f.firstseen)) AS nb_months,
    count(f.trip) AS nb_flights
   FROM data.flights f
  GROUP BY f.icao24;

