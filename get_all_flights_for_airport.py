#

import datetime
import time

from config.haystack_config import start,stop, airports
from functions import collect_flights_for_airports

startTime = time.time()
start = time.mktime(datetime.datetime.strptime(start, "%d/%m/%Y").timetuple())
stop = time.mktime(datetime.datetime.strptime(stop, "%d/%m/%Y").timetuple())
stop = min(stop,startTime)



nb_flights,status = collect_flights_for_airports(airports, start, stop)
print('Number of collected flights: ',nb_flights)