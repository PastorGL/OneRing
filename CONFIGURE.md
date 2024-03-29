### TDL3 Primer

There is a domain specific language named TDL3 that stands for One Ring Task Definition Language. (There also are other numbered TDLs, but they're discussed in other topics.)

For the language's object model, see [TaskDefinitionLanguage.java](./Commons/src/main/java/ash/nazg/config/tdl/TaskDefinitionLanguage.java). Note that the main form intended for human use isn't JSON but a simple non-sectioned .ini (or Java's .properties) file. We refer to this file as `tasks.ini`, or just a config.

Let us explore its structure by an advanced example. (For less advanced examples, search this repository's resources for test configurations, there are plenty of them.)

```properties
spark.meta.dist.wrap={WRAP:both}

spark.meta.task.input=signals,NI
spark.meta.task.operations=range_filter,h3,timezone,aqua2,create_tracks,track_centroid_filter,motion_filter,track_type,track_centroid_filter_2,$ITER{COUNTRY:GB,FI,IE},select_pedestrians,output1,output2,select_car,output1c,output2c,$END,$ITER{TYPE:clean_pedestrian,auto},match_NI,$END

spark.meta.input.path.signals={PATH_SIGNALS}/{DAILY_PREFIX}/*

spark.meta.ds.input.part_count.signals={PARTS_SIGNALS}
spark.meta.ds.input.delimiter.signals=,
spark.meta.input.schema.signals={SCHEMA_SIGNALS:userid,_,timestamp,lat,lon,_,accuracy,_,_,_,_,_,final_country,_,_,_,_,_,_,_,_,_,_,_}
spark.meta.ds.input.columns.signals=userid,lat,lon,final_country,timestamp,accuracy

spark.meta.input.path.NI={PATH_NI}

spark.meta.ds.input.part_count.NI={PARTS_NI:20}
spark.meta.ds.input.columns.NI=gid,name

# accuracy
spark.meta.op.operation.range_filter=rangeFilter
spark.meta.op.inputs.range_filter=signals
spark.meta.op.definition.range_filter.filtering.column=signals.accuracy
spark.meta.op.definition.range_filter.filtering.range=[0 100]
spark.meta.op.outputs.range_filter=accurate_signals

spark.meta.ds.input.delimiter.accurate_signals=,
spark.meta.ds.input.columns.accurate_signals=userid,lat,lon,final_country,timestamp,accuracy

# h3
spark.meta.op.operation.h3=h3
spark.meta.op.inputs.h3=accurate_signals
spark.meta.op.definition.h3.lat.column=accurate_signals.lat
spark.meta.op.definition.h3.lon.column=accurate_signals.lon
spark.meta.op.definition.h3.hash.level=9
spark.meta.op.outputs.h3=hashed

spark.meta.ds.output.columns.hashed=accurate_signals.userid,accurate_signals.lat,accurate_signals.lon,accurate_signals.final_country,accurate_signals.timestamp,_hash
spark.meta.ds.input.columns.hashed=userid,lat,lon,final_country,timestamp,gid

# timezone
spark.meta.op.operation.timezone=timezone
spark.meta.op.inputs.timezone=hashed
spark.meta.op.definition.timezone.source.timezone.default=GMT
spark.meta.op.definition.timezone.destination.timezone.default={TZ}
spark.meta.op.definition.timezone.source.timestamp.column=hashed.timestamp
spark.meta.op.outputs.timezone=tz

spark.meta.ds.output.columns.tz=hashed.userid,hashed.lat,hashed.lon,hashed.final_country,hashed.timestamp,_output_date,_output_year_int,_output_month_int,_output_dow_int,_output_day_int,_output_hour_int,_output_minute_int,hashed.gid
spark.meta.ds.input.columns.tz=userid,lat,lon,final_country,timestamp,date,year,month,dow,day,hour,minute,gid

# aqua2
spark.meta.op.operation.aqua2=aqua2
spark.meta.op.input.aqua2.signals=tz
spark.meta.op.definition.aqua2.signals.userid.column=tz.userid
spark.meta.op.definition.aqua2.signals.lat.column=tz.lat
spark.meta.op.definition.aqua2.signals.lon.column=tz.lon
spark.meta.op.definition.aqua2.signals.timestamp.column=tz.timestamp
spark.meta.op.definition.aqua2.coordinate.precision=4
spark.meta.op.output.aqua2.signals=aqua2

spark.meta.ds.input.columns.aqua2=userid,lat,lon,final_country,timestamp,date,year,month,dow,day,hour,minute,gid

# tracks
spark.meta.op.operation.create_tracks=trackCsvSource
spark.meta.op.inputs.create_tracks=aqua2
spark.meta.op.definition.create_tracks.trackid.column=aqua2.final_country
spark.meta.op.definition.create_tracks.userid.column=aqua2.userid
spark.meta.op.definition.create_tracks.lat.column=aqua2.lat
spark.meta.op.definition.create_tracks.lon.column=aqua2.lon
spark.meta.op.definition.create_tracks.ts.column=aqua2.timestamp
spark.meta.op.outputs.create_tracks=tracks

# centroid
spark.meta.op.operation.track_centroid_filter=trackCentroidFilter
spark.meta.op.inputs.track_centroid_filter=tracks
spark.meta.op.outputs.track_centroid_filter=track_centroid_filter

# motion
spark.meta.op.operation.motion_filter=motionFilter
spark.meta.op.inputs.motion_filter=track_centroid_filter
spark.meta.op.outputs.motion_filter=motion_filter

# track type
spark.meta.op.operation.track_type=trackType
spark.meta.op.inputs.track_type=motion_filter
spark.meta.op.definition.track_type.target.types=car,pedestrian
spark.meta.op.outputs.track_type=typed

# 2nd centroid
spark.meta.op.operation.track_centroid_filter_2=trackCentroidFilter
spark.meta.op.inputs.track_centroid_filter_2=typed
spark.meta.op.outputs.track_centroid_filter_2=ironfelix

# pedestrian
spark.meta.op.operation.select_pedestrians=spatialToolbox
spark.meta.op.inputs.select_pedestrians=ironfelix
spark.meta.op.definition.select_pedestrians.query=SELECT Point FROM typed WHERE _track_type='pedestrian' AND final_country='{COUNTRY}'
spark.meta.op.outputs.select_pedestrians=pedestrian_{COUNTRY}

# output
spark.meta.op.operation.output1=trackPointOutput
spark.meta.op.inputs.output1=pedestrian_{COUNTRY}
spark.meta.op.outputs.output1=output1_{COUNTRY}
spark.meta.op.operation.output2=pointCsvOutput
spark.meta.op.inputs.output2=output1_{COUNTRY}
spark.meta.op.outputs.output2=clean_pedestrian/{COUNTRY}

spark.meta.ds.output.columns.clean_pedestrian/{COUNTRY}=output1_{COUNTRY}._userid,output1_{COUNTRY}.lat,output1_{COUNTRY}.lon,output1_{COUNTRY}._velocity_kph,output1_{COUNTRY}.timestamp,output1_{COUNTRY}.date,output1_{COUNTRY}.year,output1_{COUNTRY}.month,output1_{COUNTRY}.dow,output1_{COUNTRY}.day,output1_{COUNTRY}.hour,output1_{COUNTRY}.minute,output1_{COUNTRY}.gid

# car
spark.meta.op.operation.select_car=spatialToolbox
spark.meta.op.inputs.select_car=ironfelix
spark.meta.op.definition.select_car.query=SELECT Point FROM typed WHERE _track_type='car' AND final_country='{COUNTRY}'
spark.meta.op.outputs.select_car=car_{COUNTRY}

# output
spark.meta.op.operation.output1c=trackPointOutput
spark.meta.op.inputs.output1c=car_{COUNTRY}
spark.meta.op.outputs.output1c=output1c_{COUNTRY}
spark.meta.op.operation.output2c=pointCsvOutput
spark.meta.op.inputs.output2c=output1c_{COUNTRY}
spark.meta.op.outputs.output2c=auto/{COUNTRY}

spark.meta.ds.output.columns.auto/{COUNTRY}=output1c_{COUNTRY}._userid,output1c_{COUNTRY}.lat,output1c_{COUNTRY}.lon,output1c_{COUNTRY}._velocity_kph,output1c_{COUNTRY}.timestamp,output1c_{COUNTRY}.date,output1c_{COUNTRY}.year,output1c_{COUNTRY}.month,output1c_{COUNTRY}.dow,output1c_{COUNTRY}.day,output1c_{COUNTRY}.hour,output1c_{COUNTRY}.minute,output1c_{COUNTRY}.gid

spark.meta.ds.input.columns.{TYPE}/GB=userid,lat,lon,velocity,timestamp,date,year,month,dow,day,hour,minute,gid

# NI
spark.meta.op.operation.match_NI=splitMatch
spark.meta.op.input.match_NI.source={TYPE}/GB
spark.meta.op.input.match_NI.values=NI
spark.meta.op.definition.match_NI.source.match.column={TYPE}/GB.gid
spark.meta.op.definition.match_NI.values.match.column=NI.gid
spark.meta.op.output.match_NI.matched={TYPE}/NI

spark.meta.ds.output.columns.{TYPE}/NI={TYPE}/GB.userid,{TYPE}/GB.lat,{TYPE}/GB.lon,{TYPE}/GB.velocity,{TYPE}/GB.timestamp,{TYPE}/GB.date,{TYPE}/GB.year,{TYPE}/GB.month,{TYPE}/GB.dow,{TYPE}/GB.day,{TYPE}/GB.hour,{TYPE}/GB.minute,{TYPE}/GB.gid

spark.meta.task.output=clean_pedestrian/*,auto/*

spark.meta.ds.output.path={PATH_OUTPUT}/{DAILY_PREFIX}
```

A recommended practice is to write keys in paragraphs grouped for each Operation, preceded by its Input DataStreams and succeeded by Output DataStreams groups of keys.

### Namespace Layers

As you could see, each key begins with a prefix `spark.meta.`. One Ring can read its configuration directly from Spark context (not only a config file accessible via Hadoop FileSystems), and each Spark property must start with a `spark.` prefix. We add another prefix `meta.` (by convention; this can be any unique token of your choice) to distinguish our own properties from Spark's. Also, a single tasks.ini may contain a number of Processes if properly prefixed, just start their keys with `spark.process1_name.`, `spark.another_process.` and so on.

If you're running One Ring in Local mode, you can supply properties via `tasks.ini` file, and omit all prefixes. Let assume that we've stripped all Spark's prefixes in mind and now look directly into namespaces of keys.

The config is layered into several namespaces, and all parameter names must be unique in their corresponding namespace. These layers are distinguished, again, by prefixes.

There are 'native' layers that map directly into TDL3 internal object model, and 'foreign' ones that work more like a key-value maps under layer's namespace.

### Foreign Layers

First 'foreign' (well-known by, but not native to One Ring CLI) namespace layer is One Ring Dist's `dist.` which instructs that utility to copy source files to the cluster's input location, and resulting files back from cluster's output location:
```properties
dist.wrap={WRAP:both}
```

It is [documented in its own doc](DIST.md). CLI itself ignores all 'foreign' layers.

`metrics.` layer is also discussed in [its own doc](MONITOR.md).

### Variables

If a key, or a value contains a token of the form `{ALL_CAPS}`, it'll be treated by the CLI as a configuration Variable, and will be replaced by the value supplied via command line or variables file (this topic is discussed in depth in the [Process execution how-to](EXECUTE.md)).

If the Variable's value wasn't supplied, no replacement will be made, unless the variable doesn't include a default value for itself in the form of `{ALL_CAPS:any default value}`. Default values may not contain the '{' and '}' symbols.

Considering previously mentioned
```properties
dist.wrap={WRAP:both}
```
so there if we didn't supply the variable `WRAP`, its value will default to 'both'. 

There are a few other restrictions to default values. First, each Variable occurrence has a different default and does not carry one over entire config, so you should set them each time you use that Variable. Second, if a Variable after a replacement forms a reference to another Variable, it will not be processed recursively.

It is notable that Variables may be encountered only on the right side of `=` in the `tasks.ini` lines (with one exception for DataStream names, discussed later on), but there is no limit of them for a single line and/or config file.

### CLI Task of the Process

Next layer is `task.`, and it contains properties that configure the CLI itself for the current Process' as a Spark job, or a CLI Task. All properties are required and can't be omitted.

```properties
task.input=signals,NI

task.operations=range_filter,h3,timezone,aqua2,create_tracks,track_centroid_filter,motion_filter,track_type,track_centroid_filter_2,$ITER{COUNTRY:GB,FI,IE},select_pedestrians,output1,output2,select_car,output1c,output2c,$END,$ITER{TYPE:clean_pedestrian,auto},match_NI,$END

#...

task.output=clean_pedestrian/*,auto/*
```

`task.input` is a comma-separated list of input data sets for the Process. Any DataStream referred here is considered as one sourced from the outside storage, and will be read by CLI from input location, creating an RDD for any Operation to consume.

`task.operations` is a list of Operation names, to be executed in the specified order, optionally with flow control directives. Any number of them is allowed, but no less than one Operation. Operation names must be unique.

`task.output` is a task Process output. Any DataStream referred here can be consumed by Operations as usual, but also will be saved by CLI to the output location as well. Unlike most others, this list can contain prefixed wildcards, by specifying a prefix and an asterisk `*`.

### Flow Control Directives

`task.operations` could contain flow control directives, to execute any Operation in loops, and to branch the flow by evaluating very basic conditions. Note again, we don't want to go fully Turing complete, so those directives are just for convenience.

Loops are defined by `$ITER{VARIABLE:list,of,values},operation1,...,operationN,$END`.

The control Variable of a loop is parsed as a comma-separated list, and operations inside the loop will be executed as many times as the list's length, in the listed order. In the loop, each iteration will have its control variable set to current iteration's value. Of course, loop variable might be specified via the command line.

If it wasn't, and has no default list, the operations in the loop won't be executed, so the `$ITER` loop has an optional `$ELSE` directive for that case:
```properties
task.operations=$ITER{UNSET},never,executed,$ELSE,execute,instead,$END
```

Also, for a convenience, inside the loop an automatic Variable `{ITER}` will have a random unique value for each iteration.

As there is `$ELSE`, there also should exist `$IF`, and it does. Syntax is `$IF{VARIABLE:default},operation1,...,operationN,$ELSE,operationInstead,$END`. Of course, `$ELSE` is optional, and will execute only if control Variable is unset and doesn't have a default.

Both loops and conditionals can be nested, but all of nested directives must be controlled by different variables.

Speaking of control Variables, they may come also from the output of the Operations themselves, not only from the command line or defaults. For that purpose, there is a directive of `$LET{VARIABLE:DS_name}`. It takes a DataStream with the specified name and tries to coalesce its values into a comma-separated list. If the DataStream doesn't exist or is empty, the Variable will be effectively unset.

Note that long datasets are dangerous because of memory consumption on the Spark driver instance (where all of this takes place), and Operations inside long loops will create that many times intermediate entities.

Also, note that all of those entities should have unique names to be not overwritten nor misunderstood by same Operations in subsequent iterations:
```properties
task.operations=$ITER{TYPE:clean_pedestrian,auto},match_NI,$END

ds.input.columns.{TYPE}/GB=userid,lat,lon,velocity,timestamp,date,year,month,dow,day,hour,minute,gid

op.operation.match_NI=splitMatch
op.input.match_NI.source={TYPE}/GB
op.input.match_NI.values=NI
op.definition.match_NI.source.match.column={TYPE}/GB.gid
op.definition.match_NI.values.match.column=NI.gid
op.output.match_NI.matched={TYPE}/NI

ds.output.columns.{TYPE}/NI={TYPE}/GB.userid,{TYPE}/GB.lat,{TYPE}/GB.lon,{TYPE}/GB.velocity,{TYPE}/GB.timestamp,{TYPE}/GB.date,{TYPE}/GB.year,{TYPE}/GB.month,{TYPE}/GB.dow,{TYPE}/GB.day,{TYPE}/GB.hour,{TYPE}/GB.minute,{TYPE}/GB.gid
```

There is also `$METRICS` directive, discussed in [its own doc](MONITOR.md). 

### Operation Instances

Operations share the layer `op.`, which has quite a number of sub-layers.

Operation of a certain name is a certain Java class, but we don't like to call Operations by fully-qualified class names, and ask them nicely how they would like to be called by a shorter nickname.

So, you must specify such short names for each instance of your Operation in the chain, for example:
```properties
op.operation.range_filter=rangeFilter
op.operation.h3=h3
op.operation.timezone=timezone
op.operation.aqua2=aqua2
op.operation.create_tracks=trackCsvSource
op.operation.track_centroid_filter=trackCentroidFilter
op.operation.motion_filter=motionFilter
op.operation.track_type=trackType
op.operation.track_centroid_filter_2=trackCentroidFilter
op.operation.select_pedestrians=spatialToolbox
op.operation.output1=trackPointOutput
op.operation.output2=pointCsvOutput
op.operation.select_car=spatialToolbox
op.operation.output1c=trackPointOutput
op.operation.output2c=pointCsvOutput
op.operation.match_NI=splitMatch
``` 

You see that you may have any number of calls of the same Operation class in your Process, they'll be all initialized as independent instances with different reference names. Also, inside each loop same operations are instantiated as many times as the number of control Variable values of an enclosing loop.

### Operation Inputs and Outputs

Now we go down to Operations' namespace `op.` sub-layers.

First is `op.input.` that defines which DataStreams an Operation is about to consume as 'named'. They names are assigned by the Operation itself internally. There could be multiple different named inputs for one Operation. Also, an Operation could decide to process an arbitrary number (or even wildcard) DataStreams, nameless, but positioned in the order specified by `op.inputs.` layer.

Examples from the config are:
```properties
op.inputs.range_filter=signals
op.inputs.h3=accurate_signals
op.inputs.timezone=hashed
op.input.aqua2.signals=tz
op.inputs.create_tracks=aqua2
op.inputs.track_centroid_filter=tracks
op.inputs.motion_filter=track_centroid_filter
op.inputs.track_type=motion_filter
op.inputs.track_centroid_filter_2=typed
op.inputs.select_pedestrians=ironfelix
op.inputs.output1=pedestrian_{COUNTRY}
op.inputs.output2=output1_{COUNTRY}
op.inputs.select_car=ironfelix
op.inputs.output1c=car_{COUNTRY}
op.inputs.output2c=output1c_{COUNTRY}
op.input.match_NI.source={TYPE}/GB
op.input.match_NI.values=NI
```

Note that the keys end with just a name of an Operation in the case of positional Inputs, or 'name of an Operation' + '.' + 'its internal name of input' for named ones. These layers are mutually exclusive for a given Operation.

All the same goes for the `op.output.` and `op.outputs.` layers that describe DataStreams an Operation is about to produce. Examples:
```properties
op.outputs.range_filter=accurate_signals
op.outputs.h3=hashed
op.outputs.timezone=tz
op.output.aqua2.signals=aqua2
op.outputs.create_tracks=tracks
op.outputs.track_centroid_filter=track_centroid_filter
op.outputs.motion_filter=motion_filter
op.outputs.track_type=typed
op.outputs.track_centroid_filter_2=ironfelix
op.outputs.select_pedestrians=pedestrian_{COUNTRY}
op.outputs.output1=output1_{COUNTRY}
op.outputs.output2=clean_pedestrian/{COUNTRY}
op.outputs.select_car=car_{COUNTRY}
op.outputs.output1c=output1c_{COUNTRY}
op.outputs.output2c=auto/{COUNTRY}
op.output.match_NI.matched={TYPE}/NI
```

Some Operations allow wildcard DataStreams. A wildcard DataStream reference is defined like:
```properties
op.inputs.union=prefix*
```

It'll match all DataStreams with said prefix available at the point of execution, and will be automatically converted into a list with no particular order.

### Parameters of Operations

Next sub-layer is for Operation Parameter Definitions, `op.definition.`. Parameters' names take the rest of `op.definition.` keys. The first prefix of Parameter name is the name of the Operation instance it belongs to.

Each Parameter Definition is supplied to CLI by the Operation itself via TDL2 interface (Task Description Language, [discussed here](EXTEND.md)), and they are strongly typed. So they can have a value of any Java `Number`'s descendants, `String`, `Enum`, `String[]` (string array — as a comma-separated list), and `Boolean` types.

Some Parameters may be defined as optional, and in that case they always have a default value set by the Operation itself (see Operation docs for them).

Some Parameters may be dynamic, in that case they have a fixed prefix and variable ending.

Finally, there is a variety of Parameters that refer specifically to columns of input DataStreams. Their names must end in `.column` or `.columns` by the convention, and values must refer to a valid column or list of columns, or to one of columns generated by the Operation. By convention, generated column names start with an underscore.

Look for some examples:
```properties
op.definition.range_filter.filtering.column=signals.accuracy
op.definition.range_filter.filtering.range=[0 100]
op.definition.h3.lat.column=accurate_signals.lat
op.definition.h3.lon.column=accurate_signals.lon
op.definition.h3.hash.level=9
op.definition.timezone.source.timezone.default=GMT
op.definition.timezone.destination.timezone.default={TZ}
op.definition.timezone.source.timestamp.column=hashed.timestamp
op.definition.aqua2.signals.userid.column=tz.userid
op.definition.aqua2.signals.lat.column=tz.lat
op.definition.aqua2.signals.lon.column=tz.lon
op.definition.aqua2.signals.timestamp.column=tz.timestamp
op.definition.aqua2.coordinate.precision=4
op.definition.create_tracks.trackid.column=aqua2.final_country
op.definition.create_tracks.userid.column=aqua2.userid
op.definition.create_tracks.lat.column=aqua2.lat
op.definition.create_tracks.lon.column=aqua2.lon
op.definition.create_tracks.ts.column=aqua2.timestamp
op.definition.track_type.target.types=car,pedestrian
op.definition.select_pedestrians.query=SELECT Point FROM typed WHERE _track_type='pedestrian' AND final_country='{COUNTRY}'
op.definition.select_car.query=SELECT Point FROM typed WHERE _track_type='car' AND final_country='{COUNTRY}'
op.definition.match_NI.source.match.column={TYPE}/GB.gid
op.definition.match_NI.values.match.column=NI.gid
```

Exact type and details of Parameter usage is defined by the internal logic of each Operation. Some Operations require a quite complex syntax for their `String` Parameters (like SQL queries or regexp templates), documented by that Operation. The only type of Parameters that One Ring CLI validates by itself are `.column(s)` references.

For the exhaustive table of each Operation Parameters, look for the docs inside your [./REST/docs](./REST/docs/index.md) directory (assuming you've successfully built the project, otherwise it'll be empty).

### Parameters of DataStreams

Next layer is the `ds.` configuration namespace of DataStreams, and its rules are quite different.

First off, DataStreams are always parseable text-based, albeit with some degree of freedom. There are types of:
* Columnar:
  * `Plain` RDD is generated by CLI as opaque Hadoop `Text` for each source line, to be handled by Operation.
  * `CSV` (column-based, escaped, and delimited text) with loosely defined schema, but strongly referenced columns;
    * `Fixed` same `CSV`, but the order of columns, and their format are considered fixed.
* `KeyValue` PairRDD with opaque strings for keys, and column-based values like `CSV`.
* Spatial (object-based; remember that One Ring was developed with primarily geoinformational processing in mind):
  * `Point` contains geodesic coordinates with arbitrary set of metadata but mandatory fields of latitude, longitude, and time stamp;
    * `Point`s of interest (POI) have also a radius attribute.
  * `Polygon` contains closed 2D geometry outlines (that may contain holes) with metadata.
  * `SegmentedTrack` contains a collection of `Point`s grouped into `TrackSegment`s by attribute of track ID and further into `SegmentedTrack`s by attribute of user ID; each object in collection can have metadata.

Each DataStream can be configured as an input for a number of Operations, and generated as an output by only one of them. Exact number and types of DataStream that an Operation handles and generates are defined by that Operation.

DataStream name is always the last part of any `ds.` key.

`ds.input.columns.` and `ds.output.columns.` layers define columns for column-based DataStreams or metadata properties for object-based ones. Column names must be unique for that particular DataStream.

Output columns must always refer to valid columns of inputs passed to the Operation that emits said DataStream, or its generated columns (which names start with an underscore).

Input columns list just assigns new column names for all consuming Operations. It may also contain a single underscore `_` instead of some column name to make that column anonymous. Anyways, if a column is 'anonymous', it still may be referenced by its sequential number starting from `_1_`.

One Ring Dist reorders columns according to that schema and `ds.input.columns.` in the process of copying data set to the cluster, and can skip the fields that do not matter. It is ignored by CLI itself. Also, One Ring Dist can deal with Parquet source files, what CLI itself couldn't. For these files schema is built-in, and column names in `input.schema.` must adhere to that schema.

There is an example:
```properties
ds.input.columns.signals=userid,lat,lon,final_country,timestamp,accuracy

ds.input.columns.NI=gid,name

ds.input.columns.accurate_signals=userid,lat,lon,final_country,timestamp,accuracy

ds.output.columns.hashed=accurate_signals.userid,accurate_signals.lat,accurate_signals.lon,accurate_signals.final_country,accurate_signals.timestamp,_hash
ds.input.columns.hashed=userid,lat,lon,final_country,timestamp,gid

ds.output.columns.tz=hashed.userid,hashed.lat,hashed.lon,hashed.final_country,hashed.timestamp,_output_date,_output_year_int,_output_month_int,_output_dow_int,_output_day_int,_output_hour_int,_output_minute_int,hashed.gid
ds.input.columns.tz=userid,lat,lon,final_country,timestamp,date,year,month,dow,day,hour,minute,gid

ds.input.columns.aqua2=userid,lat,lon,final_country,timestamp,date,year,month,dow,day,hour,minute,gid

ds.output.columns.clean_pedestrian/{COUNTRY}=output1_{COUNTRY}._userid,output1_{COUNTRY}.lat,output1_{COUNTRY}.lon,output1_{COUNTRY}._velocity_kph,output1_{COUNTRY}.timestamp,output1_{COUNTRY}.date,output1_{COUNTRY}.year,output1_{COUNTRY}.month,output1_{COUNTRY}.dow,output1_{COUNTRY}.day,output1_{COUNTRY}.hour,output1_{COUNTRY}.minute,output1_{COUNTRY}.gid
ds.output.columns.auto/{COUNTRY}=output1c_{COUNTRY}._userid,output1c_{COUNTRY}.lat,output1c_{COUNTRY}.lon,output1c_{COUNTRY}._velocity_kph,output1c_{COUNTRY}.timestamp,output1c_{COUNTRY}.date,output1c_{COUNTRY}.year,output1c_{COUNTRY}.month,output1c_{COUNTRY}.dow,output1c_{COUNTRY}.day,output1c_{COUNTRY}.hour,output1c_{COUNTRY}.minute,output1c_{COUNTRY}.gid

ds.input.columns.{TYPE}/GB=userid,lat,lon,velocity,timestamp,date,year,month,dow,day,hour,minute,gid

ds.output.columns.{TYPE}/NI={TYPE}/GB.userid,{TYPE}/GB.lat,{TYPE}/GB.lon,{TYPE}/GB.velocity,{TYPE}/GB.timestamp,{TYPE}/GB.date,{TYPE}/GB.year,{TYPE}/GB.month,{TYPE}/GB.dow,{TYPE}/GB.day,{TYPE}/GB.hour,{TYPE}/GB.minute,{TYPE}/GB.gid
```

As you may note, DataStream names can be Variable, and this is the only exception from the general property naming rule. For all other configuration layers, use of Variables on the left side of a property definition is disallowed.

In `CSV` varieties of DataStreams, columns are separated by a separator character, so there are `ds.input.separator.` and `ds.output.separator.` layers, along with default keys `ds.input.separator` and `ds.output.separator` that set them globally. The 'super global' default value of column separator is the tabulation (TAB, 0x09) character, if none are specified in the entire config.

The final `ds.` layers control the partitioning of DataStream underlying RDDs, namely, `ds.input.part_count.` and `ds.output.part_count.`. These are quite important because the only super global default value for the part count is always 1 (one) part, and no explicit defaults are allowed. You must always set them for at least initial input DataStreams from `task.input` list, and may tune the partitioning in the middle of the Process according to the further flow of the Task.

If both `part_count.`s are specifies for some intermediate DataStream, it will be repartitioned first to the output one (immediately after the Operation that generated it), and then to input one (before feeding it to the first consuming Operation). Please keep that in mind.

### Input and Output Storage Locations

Input DataStreams of an entire Process come from the outside world, and output DataStreams are stored somewhere outside. CLI does this job via notion of input and output location and with a help of Dist (as discussed in [its own doc](DIST.md)).

`input.path` is a Hadoop FileSystem path where all source files for DataStreams referenced in `task.input` list reside by default, and `output.path` is a path where to place the resulting `task.output`. For example, all-output default key, set via Variables:
```properties
output.path={PATH_OUTPUT}/{DAILY_PREFIX}
```

These properties have some meaningful defaults, if left unset. For Local execution mode they both are automatically set to `.` (current directory), and for on-cluster execution they are defined as `hdfs:///input` and `hdfs:///output`.

These properties can be overridden per each DataStream. For input ones, overrides are set in the layer `input.path.`. In most cases, path overrides are passed via Variables. For example,
```properties
input.path.signals={PATH_SIGNALS}/{DAILY_PREFIX}/*
input.path.NI={PATH_NI}
```

Same true for `ds.output.path.` keys, that can be specified for each (or some) of DataStreams listed under the `task.output` key.

### Next to Read 

This concludes the configuration of One Ring CLI for a single Process. After you've assembled a library of basic Processes, you'll may want to know [how to compose](COMPOSE.md) them into larger workflows.
