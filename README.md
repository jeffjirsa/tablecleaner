tablecleaner
============

tablecleaner is a simple python utility that iterates over a cassandra 
(CQL) column family and deletes data based on timestamps, ttls, or both. 
It is ported from an internal tool, therefore it is lightly tested, 
likely to have errors, and should not be used in production without 
running first on a test cluster. It is offered as a convenience, but
expressly without warranty - this tool deletes data, and you need to 
test to ensure it is deleting the RIGHT data.

The tool uses the datastax driver with a fairly small ```fetch_size``` 
(10) to page across the table. This will cause a large number of fairly 
inexpensive queries, but the overall impact will be significant if you 
have a large table. Be aware that running this while your cluster
does not have spare I/O could be dangerous to your cluster health.

Example
=======

Imagine a large table of counters:

```
CREATE TABLE counters (
  counter_name text,
  counter_time_interval text,
  counter_time_partition timestamp,
  counter_time timestamp,
  counter_value counter,
  PRIMARY KEY ((counter_name, counter_time_interval, counter_time_partition), 
								counter_time)
)
```

In this case,
* counter_name describes the counter, 
* counter_time_interval denotes the resolution (year, month, day, hour, etc), 
* counter_time_partition is the first timestamp within a partition, and 
* counter_time is a clustering key to order the counters within the partition.

Given that Cassandra does not (and likely will not) support TTLs on counters, if
you wanted to remove high resolution (per-second) counters after 90 days, this 
command could be used.

In this case, the current unix timestamp is: ```1412212512```
Using Cassandra timestamps in milliseconds, the cutoff is: ```1404436512000```
If your timestamps are using microseconds, ADJUST ACCORDINGLY.

The command becomes:

```
python tablecleaner.py  --host 127.0.0.1 --table "counters" --keyspace "demo" --timestamp 1404436512000 --match_column_name counter_time_interval --match_column_value second
```


TODO 
====

- Add ability to stop and restart iteration across the table
- Add ability to run in parallel

LICENSE
=======

The MIT License (MIT)

Copyright (c) 2014, Jeff Jirsa



Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

