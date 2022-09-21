#

import datetime
import time

from config.haystack_config import icao24,start,stop
from functions import collect_flights_for_aircrafts

startTime = time.time()

start = time.mktime(datetime.datetime.strptime(start, "%d/%m/%Y").timetuple())

stop = time.mktime(datetime.datetime.strptime(stop, "%d/%m/%Y").timetuple())
stop = min(stop,startTime)


nb_flights, status = collect_flights_for_aircrafts(icao24, start, stop)
print('Number of collected flights: ',nb_flights)