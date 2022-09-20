-- Example: select Flights and append readable Airport names

select f.icao24,f.firstseen,f.lastseen,f.callsign,f.duration,
ad.name as departure,
aa.name as arrival

from data.flights f
left join data.airports ad on ad.ident = f.estdepartureairport
left join data.airports aa on aa.ident = f.estarrivalairport

order by firstseen desc


