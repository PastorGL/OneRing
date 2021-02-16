One Ring CLI provides a rather simple monitoring capabilities primarily targeted to analyze the quality of Task's flow and data transforms.

### Raw Record Count Monitoring for Storages

The CLI is able to report the number of records read and written from and to external storage — in One Ring terms these are 'sink' and 'tee' DataStreams. Currently, only default Hadoop Adapter provides this information (technically, CLI taps into Spark's developer API to get the numbers, but other Adapters do not have access to it).

Numbers are reported via Spark application log, after successful task execution, by `TaskWrapper` class on `INFO` log level. For example,
```log
...
21/02/15 15:53:17 INFO DAGScheduler: Job 21 finished: runJob at SparkHadoopWriter.scala:78, took 0.069423 s
21/02/15 15:53:17 INFO SparkHadoopWriter: Job job_20210215155317_0145 committed.
21/02/15 15:53:17 INFO TaskWrapper: One Ring sink signals: 2201590 record(s) read
21/02/15 15:53:17 INFO TaskWrapper: One Ring sink NI: 438558 record(s) read
21/02/15 15:53:17 INFO TaskWrapper: One Ring tee clean_pedestrian/GB: 4358 records(s) written
21/02/15 15:53:17 INFO TaskWrapper: One Ring tee clean_pedestrian/NI: 33 records(s) written
21/02/15 15:53:17 INFO TaskWrapper: One Ring tee auto/NI: 71 records(s) written
21/02/15 15:53:17 INFO TaskWrapper: One Ring tee auto/GB: 5533 records(s) written
21/02/15 15:53:17 INFO TaskWrapper: One Ring tee clean_pedestrian/IE: 119 records(s) written
21/02/15 15:53:17 INFO TaskWrapper: One Ring tee auto/IE: 97 records(s) written
21/02/15 15:53:17 INFO TaskWrapper: One Ring tee clean_pedestrian/FI: 155 records(s) written
21/02/15 15:53:17 INFO TaskWrapper: One Ring tee auto/FI: 93 records(s) written
21/02/15 15:53:17 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
21/02/15 15:53:18 INFO MemoryStore: MemoryStore cleared
21/02/15 15:53:18 INFO BlockManager: BlockManager stopped
21/02/15 15:53:18 INFO BlockManagerMaster: BlockManagerMaster stopped
21/02/15 15:53:18 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
21/02/15 15:53:18 INFO SparkContext: Successfully stopped SparkContext
21/02/15 15:53:18 INFO ShutdownHookManager: Shutdown hook called
21/02/15 15:53:18 INFO ShutdownHookManager: Deleting directory /tmp/spark-2869b8cf-7400-4972-86e7-14b0d07fbb13
```

For each sink, the actual number of records is shown for all consuming Operations throughout the Operation chain. If multiple Operations receive data from same sink, each does it independently of others, so the number of source records will be multiplied as many times as the number of Operations that read from that sink.

Contrary to that, each tee is normally written to the external storage only once (though it could be used by subsequent Operations many times), and the number of records written should be always accurate.

### Detailed DataStream Metrics

In addition to raw record counting for DataStreams at external storage boundaries, there is an instrument for deeper examination of any DataStream, intermediate included.

It comes in a form of TDL control flow directive `$METRICS`, and performs an analysis of targeted DataStreams for `record count`, `unique key count`, `average number of records per counting key`, and `median number of records per counting key` metrics, where 'key' is a different entity for each DataStream type.

This analysis is performed by a pseudo-Operation, that isn't visible in the Operation list nor available for direct calls. Instead of usual Operation context, it operates on a Task-level context and is configured via `task.metrics.` namespace layer.

Please note, deeper analysis mode of execution is not intended for everyday use because it causes the Operation chain to terminate at the set point and restarts it again from the beginning for each `$METRICS` directive in the Operation list. Perform it only when debugging a new Task.

For example, consider us need to analyze the number of `SegmentedTrack`s generated from raw signals left by users travelling different cities:
```properties
task.input.sink=signals
task.operations=create_tracks,$METRICS{tracks},...

ds.input.path.signals={PATH_SIGNALS}
ds.input.columns.signals=userid,lat,lon,ts,city

# tracks
op.operation.create_tracks=trackCsvSource
op.inputs.create_tracks=signals
op.definition.create_tracks.trackid.column=signals.city
op.definition.create_tracks.userid.column=signals.userid
op.definition.create_tracks.lat.column=signals.lat
op.definition.create_tracks.lon.column=signals.lon
op.definition.create_tracks.ts.column=signals.ts
op.outputs.create_tracks=tracks

task.metrics.count.column.tracks=_userid
...
```

Here we take the `tracks` DataStream (a spatially-typed one), emitted by an intermediate Operation `trackCsvSource`, and define its 'counting key' property as '_userid'. Therefore, in `unique key count` we'll count the number of all cities travelled by each user, because we directed `trackCsvSource` to create a track per user per city.

If run with that config, the Spark's application log will contain the following lines somewhere near its end:
```log
...
21/02/15 15:53:17 INFO TaskWrapper: Metrics of data stream 'tracks'
21/02/15 15:53:17 INFO TaskWrapper: Total number of objects: 250385.0
21/02/15 15:53:17 INFO TaskWrapper: Median number of objects per counter: 1.0
21/02/15 15:53:17 INFO TaskWrapper: Average number of objects per counter: 1.56
21/02/15 15:53:17 INFO TaskWrapper: Count of unique objects by property '_userid': 250385.0
...
```

If we change the counting property to '_trackid' (or, 'city' because here it's the same),
```properties
task.metrics.count.column.tracks=_trackid
```
then we'll count the number of user tracks per each city, and `unique key count` metric will show the number of cities that have users with at least one track. And the output be like:
```log
...
21/02/15 15:53:17 INFO TaskWrapper: Metrics of data stream 'tracks'
21/02/15 15:53:17 INFO TaskWrapper: Total number of objects: 250385.0
21/02/15 15:53:17 INFO TaskWrapper: Median number of objects per counter: 37253.41730.33333333333
21/02/15 15:53:17 INFO TaskWrapper: Average number of objects per counter: 41730.83333333333
21/02/15 15:53:17 INFO TaskWrapper: Count of unique objects by property '_trackid': 6.0
...
```

The metric `record count` is always the exact number of record in the DataStream, and both averages will show numbers relative to both counts. These metrics can be used by means of data pipeline analysis and/or data source quality, if compared between the sink itself, and processed data down the Operation chain.

Because we currently have 7 types of DataStreams, the rules of application of `count.column` per each data type are defined as:
* `Plain` — we treat entire record as an opaque byte array, and 'key' is an MD5 hash of that array (effectively counting unique records),
* Columnar (`CSV` and `Fixed`) — we set a single column as a record 'key', defined by its `ds.input.columns.` setting (not `ds.input.sink_schema.` nor `ds.output.columns.`), and delimiter character from its `ds.input.delimiter.`,
* Pair (`KeyValue`) — we always use a 'key' part of a record,
* Spatially-typed (`Point`, `Track`, `Polygon`) — we select a top-level object's property as a record 'key', and never look into nested objects.

Therefore, `unique key count` metrics are reported as "Count of unique opaque records", "Count of unique records by column 'column'", "Count of unique objects by property 'property'", and "Count of unique pair keys" for their respective DataStream types.

`record count` is reported as "Total number of _values_", and averages are "Average number of _values_ per counter" and "Median number of _values_ per counter", where each of '_values_' corresponds to an actual type of that DataStream records, objects or whatever they are.

The directive `$METRICS` can instrument at once a list of DataStreams that are generated by Operations before this directive. If it refers to a DataStream generated later in the Operation chain, it'll be silently skipped.

### Sinks and Tees Metrics and Report Output

For metering Task sinks and tees, there is a special mode of CLI invocation by specifying `-D /path/to/metrics/report` command line key.

This key effectively adds `$METRICS{:sink}` and `$METRICS{:tee}` directives at `task.operations` setting's beginning and ending position, and also redirects the report to the specified file. (You may add these special directives there by yourself, if redirection to a file isn't needed.)

In that mode, ':sink' token will be replaced by a literal value from `task.input.sink` setting, and ':tee' from `task.tee.output`.

Because `$METRICS` works in the global Task context, it doesn't handle any Task Variables. Also, the necessity of handling tees list (that can contain the wildcards) makes `$METRICS` directive support the wildcard DataStream references as well.

To specify the wildcard DataStreams' columns, instead of describing columns for each of them, you may use literal wildcards in `task.metrics.count.column.`, `ds.input.columns.` and `ds.input.delimiter.` (not `ds.output.` even if these are tees!):
```properties
task.metrics.count.column.travel_by_car/*=userid
ds.input.columns.travel_by_car/*=_,_,userid,_
ds.input.delimiter.travel_by_car/*=,
...
task.tee.output=travel_by_car/*
```
In this example, all 'travel_by_car/*' DataStreams, generated in a loop or by different branches, will be treated correctly.
