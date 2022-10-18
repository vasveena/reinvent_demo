--Query Hudi table

SELECT * FROM default.hudi_trips_kinesis_streaming_table_ts;

--Total number of entries

select count (tripId) as numOfTrips from default.hudi_trips_kinesis_streaming_table

--Select all non-metadata fields 

select tripId, routeId, startTime, numOfFutureStops, currentStopSequence, currentStatus, currentStopId, 
nextStopArrivalTime, nextStopId, lastStopArrivalTime, lastStopId
from default.hudi_trips_kinesis_streaming_table_ts limit 10;

--Number of trips for each route

select routeId, count(tripId) as numOfTrips
from default.hudi_trips_kinesis_streaming_table_ts
group by routeId
order by routeId;

-- Number of trips started between two intervals 

select count(tripId) as numOfTrips, routeId 
from default.hudi_trips_kinesis_streaming_table_ts
where startTime BETWEEN cast('2022-10-12 00:00:00' as timestamp)  AND cast('2022-10-18 18:00:00' as timestamp)
group by routeId, startTime order by 1 desc;

-- Current stop and upcoming stop for all trips along with their status

select tripId, routeId, 
currentStopSequence, currentStatus, currentStopId, nextStopId
from default.hudi_trips_kinesis_streaming_table_ts;

-- Time until next stop for a trip

select tripId, routeId, minutesRemainingUntilNextStop 
from (select tripId, routeId, 
date_diff('minute', currentTime, nextStopArrivalTime) as minutesRemainingUntilNextStop
from default.hudi_trips_kinesis_streaming_table_ts) temp_trip_table
where minutesRemainingUntilNextStop >=0;


-- Last stop along with arrival time for all trips in each route 

select tripId, routeId, lastStopId, lastStopArrivalTime 
from default.hudi_trips_kinesis_streaming_table_ts;

-- Get future number of stops per trip and route at the moment 

select tripId, routeId, currentTime, sum(numOfFutureStops) from
(select tripId , routeId, currentTime, numOfFutureStops,
rank() over (order by currentTime) as rnk
from default.hudi_trips_kinesis_streaming_table_ts)
where rnk = 1
group by 1,2,3
order by 4 desc;
