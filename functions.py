import datetime

import requests


def get_flights_aircraft(icao24, start, end):
    # Open Sky API request

    print('Getting flights for plane ICAO24 :', icao24, ' between: ',
          datetime.datetime.fromtimestamp(start), ' and : ',
          datetime.datetime.fromtimestamp(end))
    apiURL = "https://opensky-network.org/api/flights/aircraft"

    requestURL = apiURL + '?icao24=' + icao24 + '&begin=' + str(start) + '&end=' + str(end)
    r = requests.get(requestURL, timeout=10)

    if r is None:
        print('Cannot reach OpenSky API - just re-run the script')
        flights = []
    else:
        flights = r.json()
        print('Found ', len(flights), ' flights for ', icao24)

    return flights


def get_flights_from_departure(airport, start, end):
    # Open Sky API request

    print('Getting flights from airport :', airport, ' between: ',
          datetime.datetime.fromtimestamp(start), ' and : ',
          datetime.datetime.fromtimestamp(end))

    apiURL = "https://opensky-network.org/api/flights/departure"

    requestURL = apiURL + '?airport=' + airport + '&begin=' + str(start) + '&end=' + str(end)
    r = requests.get(requestURL, timeout=10)

    if r is None:
        print('Cannot reach OpenSky API - just re-run the script')
        flights = []
    else:
        flights = r.json()
        print('Found ', len(flights), ' flights from ', airport)

    return flights


def get_flights_to_destination(airport, start, end):
    # Open Sky API request

    print('Getting flights to airport :', airport, ' between: ',
          datetime.datetime.fromtimestamp(start), ' and : ',
          datetime.datetime.fromtimestamp(end))

    apiURL = "https://opensky-network.org/api/flights/arrival"

    requestURL = apiURL + '?airport=' + airport + '&begin=' + str(start) + '&end=' + str(end)
    r = requests.get(requestURL, timeout=10)

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

    print("Flight values: " + str(values))

    try:
        db_cursor.execute(sql, values)
    except Exception as error:
        print(str(error))
        print('Db_insert failure')
