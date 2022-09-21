import datetime
import time

import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from config.haystack_config import host, database, user, flights_table_name

retry_strategy = Retry(
    total=5,
    backoff_factor=3,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)




def get_OpenSky_flights_aircraft(icao24, start, end):
    # Open Sky API request

    print('Getting flights for plane ICAO24 :', icao24, ' between: ',
          datetime.datetime.fromtimestamp(start), ' and : ',
          datetime.datetime.fromtimestamp(end))
    apiURL = "https://opensky-network.org/api/flights/aircraft"

    requestURL = apiURL + '?icao24=' + icao24 + '&begin=' + str(start) + '&end=' + str(end)
    r = session.get(requestURL, timeout=10)

    if r is None:
        print('Cannot reach OpenSky API - just re-run the script')
        flights = []
    else:
        flights = r.json()
        print('Found ', len(flights), ' flights for ', icao24)

    return flights


def get_OpenSky_flights_from_departure(airport, start, end):
    # Open Sky API request

    print('Getting flights from airport :', airport, ' between: ',
          datetime.datetime.fromtimestamp(start), ' and : ',
          datetime.datetime.fromtimestamp(end))

    apiURL = "https://opensky-network.org/api/flights/departure"

    requestURL = apiURL + '?airport=' + airport + '&begin=' + str(start) + '&end=' + str(end)
    r = session.get(requestURL, timeout=10)

    if r is None:
        print('Cannot reach OpenSky API - just re-run the script')
        flights = []
    else:
        flights = r.json()
        print('Found ', len(flights), ' flights from ', airport)

    return flights


def get_OpenSky_flights_to_destination(airport, start, end):
    # Open Sky API request

    print('Getting flights to airport :', airport, ' between: ',
          datetime.datetime.fromtimestamp(start), ' and : ',
          datetime.datetime.fromtimestamp(end))

    apiURL = "https://opensky-network.org/api/flights/arrival"

    requestURL = apiURL + '?airport=' + airport + '&begin=' + str(start) + '&end=' + str(end)
    r = session.get(requestURL, timeout=10)

    if r is None:
        print('Cannot reach OpenSky API - just re-run the script')
        flights = []
    else:
        flights = r.json()
        print('Found ', len(flights), ' flights to ', airport)

    return flights


def db_insert(db_cursor, table_name, **kwargs):
    sql = "INSERT INTO " + str(table_name) + ' ' + '('

    k_list = ','.join('"%s"' % x for x in kwargs.keys())
    m_list = ','.join('%s' for x in kwargs.keys())
    sql += k_list + ') VALUES(' + m_list + ') ON CONFLICT DO NOTHING;'
    values = [(v) for v in kwargs.values()]

    #print("Flight values: " + str(values))

    try:
        db_cursor.execute(sql, values)
    except Exception as error:
        print(str(error))
        print('Db_insert failure')


def collect_flights_from_airports(airports, start, stop):


    print('Start requesting flights for ',len(airports), ' airports, between: ',
          datetime.datetime.fromtimestamp(start), ' and : ',
          datetime.datetime.fromtimestamp(stop))

    # connect to Database

    conn = None
    print('Trying to connect to database')
    try:
        conn = psycopg2.connect(host=host, database=database, user=user)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        print('Connected')

        # close the communication with the PostgreSQL
    except (Exception, psycopg2.DatabaseError) as error:
        print('Could not connect to DB', str(error))

    # ready to insert data in database

    nb_flights = 0
    airport_index=1
    airport_nb = len(airports)

    for a in airports:
        begin = int(start)
        end = min(int(start + 7 * 24 * 60 * 60), int(stop))

        print(' ')
        print('*** Processing airport nb: ',airport_index,'/',airport_nb,' ***')
        print(' ')

        while (begin < stop):
            flights=[]
            # get flights

            flights = get_OpenSky_flights_from_departure(a, begin, end)
            nb_flights += len(flights)

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
            begin += 7 * 24 * 60 * 60
            end = min(end + 7 * 24 * 60 * 60,int(stop))
        conn.commit()
    conn.close()

    status='OK'
    return(nb_flights,status)


def collect_flights_for_airports(airports,start,stop):

    print('Start requesting flights for ',len(airports), ' between: ',
          datetime.datetime.fromtimestamp(start), ' and : ',
          datetime.datetime.fromtimestamp(stop))

    # connect to Database

    conn = None
    print('Trying to connect to database')
    try:
        conn = psycopg2.connect(host=host, database=database, user=user)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        print('Connected')

        # close the communication with the PostgreSQL
    except (Exception, psycopg2.DatabaseError) as error:
        print('Could not connect to DB', str(error))

    # ready to insert data in database

    nb_flights = 0
    airport_index=1
    airport_nb = len(airports)

    for a in airports:
        begin = int(start)
        end = min(int(start + 7 * 24 * 60 * 60),int(stop))

        print(' ')
        print('*** Processing airport nb: ',airport_index,'/',airport_nb,' ***')
        print(' ')

        while (begin < stop):

            # get flights from...
            flights = []
            flights = get_OpenSky_flights_to_destination(a, begin, end)
            nb_flights+=len(flights)
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

            # get flights to...
            flights = []
            flights = get_OpenSky_flights_from_departure(a, begin, end)
            nb_flights += len(flights)
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

            begin += 7 * 24 * 60 * 60
            end = min(end + 7 * 24 * 60 * 60,int(stop))

        conn.commit()
    conn.close()
    status='OK'
    return(nb_flights,status)


def collect_flights_for_aircrafts(icao24,start,stop):

    try:
        conn = psycopg2.connect(host=host, database=database, user=user)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        print('Connected')

        # close the communication with the PostgreSQL
    except (Exception, psycopg2.DatabaseError) as error:
        print('Could not connect to DB', str(error))

    # ready to insert data in database

    # loop on Airports, Aircraft & dates
    nb_flights = 0
    aircraft_index=1
    aircraft_nb = len(icao24)

    for i in icao24:
        begin = int(start)
        end = min(int(start + 30 * 24 * 60 * 60),int(stop))
        print(' ')
        print('*** Processing aircraft nb: ',aircraft_index,'/',aircraft_nb,' ***')
        print(' ')
        while (begin < stop):

            # get flights
            flights = []
            flights = get_OpenSky_flights_aircraft(i, begin, end)
            nb_flights += len(flights)

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
        aircraft_index+=1

    conn.close()

    status='OK'
    return(nb_flights,status)


def collect_flights_to_airports(airports, start, stop):
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
    nb_flights = 0
    airport_index=1
    airport_nb = len(airports)

    for a in airports:
        begin = int(start)
        end = min(int(start + 7 * 24 * 60 * 60), int(stop))

        print(' ')
        print('*** Processing airport nb: ',airport_index,'/',airport_nb,' ***')
        print(' ')

        while (begin < stop):

            # get flights
            flights = []
            flights = get_OpenSky_flights_to_destination(a, begin, end)
            nb_flights += len(flights)

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

            begin += 7 * 24 * 60 * 60
            end = min(end + 7 * 24 * 60 * 60,int(stop))
        conn.commit()
    conn.close()
    status='OK'
    return(nb_flights,status)
