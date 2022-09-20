#

import datetime
import time

import psycopg2.extras

from config.haystack_config import host, database, user, flights_table_name, icao24,start,stop
from functions import get_flights_aircraft, db_insert

startTime = time.time()

start = time.mktime(datetime.datetime.strptime(start, "%d/%m/%Y").timetuple())

stop = time.mktime(datetime.datetime.strptime(stop, "%d/%m/%Y").timetuple())
stop = min(stop,startTime)

flights = []

# connect to Database

conn = None
try:
    conn = psycopg2.connect(host=host, database=database, user=user)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    print('Connected')

    # close the communication with the PostgreSQL
except (Exception, psycopg2.DatabaseError) as error:
    print('Could not connect to DB', str(error))

# ready to insert data in database

# loop on Airports, Aircraft & dates
for i in icao24:
    begin = int(start)
    end = min(int(start + 30 * 24 * 60 * 60),int(stop))

    while (startTime - begin > 0):

        # get flights
        flights = get_flights_aircraft(i, begin, end)
        time.sleep(0.5)

        # Insert found flights in DB
        for flight in flights:
            if flight['firstSeen'] != None and flight['lastSeen'] != None:
                firstseen = str(datetime.datetime.fromtimestamp(flight['firstSeen']).strftime("%Y-%m-%d %H:%M"))
                lastseen = str(datetime.datetime.fromtimestamp(flight['lastSeen']).strftime("%Y-%m-%d %H:%M"))

                duration = flight['lastSeen'] - flight['firstSeen']

                db_insert(cursor, flights_table_name,
                          icao24=flight['icao24'],
                          firstseen=firstseen,
                          estdepartureairport=flight['estDepartureAirport'],
                          lastseen=lastseen,
                          estarrivalairport=flight['estArrivalAirport'],
                          callsign=flight['callsign'],
                          estdepartureairporthorizdistance=flight['estDepartureAirportHorizDistance'],
                          estdepartureairportvertdistance=flight['estDepartureAirportVertDistance'],
                          estarrivalairporthorizdistance=flight['estArrivalAirportHorizDistance'],
                          estarrivalairportvertdistance=flight['estArrivalAirportVertDistance'],
                          departureairportcandidatescount=flight['departureAirportCandidatesCount'],
                          arrivalairportcandidatescount=flight['arrivalAirportCandidatesCount'],
                          duration=duration)
        conn.commit()
        begin += 30 * 24 * 60 * 60
        end = min(end + 30 * 24 * 60 * 60,int(stop))

    # End:
    conn.commit()

conn.close()
