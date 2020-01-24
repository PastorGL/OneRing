There is a DSL named TDL3 that stands for One Ring Task Definition Language. (There also are TDL1 and TDL2, but they're discussed in other topics.)

For the language object model, see [TaskDefinitionLanguage.java](./Commons/src/main/java/ash/nazg/config/tdl/TaskDefinitionLanguage.java), but the main form intended for human use isn't JSON but a simple non-sectioned .ini (or Java's .properties) file. We refer to this file as tasks.ini (or tasks.json).

Let we explore its structure by an advanced example. (For non-advanced examples, search this repository tests resources, there are plenty of them.)

```properties
spark.meta.distcp.wrap=both
spark.meta.distcp.exe=s3-dist-cp


spark.meta.task.operations=range_filter,accuracy_filter,h3,timezone,center_mass_1,track_type,type_other,track_type_filter,remove_point_type,iron_glitch,slow_motion,center_mass_2,aqua2,map_by_user,map_tracks_non_pedestrian,map_pedestrian,map_aqua2,count_by_user,count_tracks_non_pedestrian,count_pedestrian,count_aqua2
spark.meta.task.input.sink=signals


spark.meta.ds.input.path.signals={INPUT_PATH}
spark.meta.ds.input.part_count.signals={PARTS}
spark.meta.ds.input.columns.signals=userid,lat,lon,accuracy,idtype,timestamp


spark.meta.op.operation.range_filter=rangeFilter

spark.meta.op.inputs.range_filter=signals

spark.meta.op.definition.range_filter.filtering.column=signals.accuracy
spark.meta.op.definition.range_filter.filtering.range=[0 50]

spark.meta.op.outputs.range_filter=range_accurate_signals


spark.meta.ds.input.columns.range_accurate_signals=userid,lat,lon,accuracy,idtype,timestamp


spark.meta.op.operation.accuracy_filter=accuracyFilter

spark.meta.op.input.accuracy_filter.signals=range_accurate_signals

spark.meta.op.definition.accuracy_filter.signals.lat.column=range_accurate_signals.lat
spark.meta.op.definition.accuracy_filter.signals.lon.column=range_accurate_signals.lon

spark.meta.op.output.accuracy_filter.signals=accurate_signals


spark.meta.ds.input.columns.accurate_signals=userid,lat,lon,accuracy,idtype,timestamp


spark.meta.op.operation.h3=h3

spark.meta.op.inputs.h3=accurate_signals

spark.meta.op.definition.h3.lat.column=accurate_signals.lat
spark.meta.op.definition.h3.lon.column=accurate_signals.lon
spark.meta.op.definition.h3.hash.level=9

spark.meta.op.outputs.h3=AG


spark.meta.ds.output.columns.AG=accurate_signals.userid,accurate_signals.lat,accurate_signals.lon,accurate_signals.accuracy,accurate_signals.idtype,accurate_signals.timestamp,_hash
spark.meta.ds.input.columns.AG=userid,lat,lon,accuracy,idtype,timestamp,gid


spark.meta.op.operation.timezone=timezone

spark.meta.op.inputs.timezone=AG

spark.meta.op.definition.timezone.source.timezone.default=GMT
spark.meta.op.definition.timezone.destination.timezone.default={TZ}
spark.meta.op.definition.timezone.source.timestamp.column=AG.timestamp

spark.meta.op.outputs.timezone=timezoned


spark.meta.ds.output.columns.timezoned=AG.userid,AG.lat,AG.lon,AG.accuracy,AG.idtype,AG.timestamp,_output_date,_output_year_int,_output_month_int,_output_dow_int,_output_day_int,_output_hour_int,_output_minute_int,AG.gid


spark.meta.op.operation.center_mass_1=trackCentroidFilter

spark.meta.op.input.center_mass_1.signals=timezoned


spark.meta.ds.input.columns.timezoned=userid,lat,lon,accuracy,idtype,timestamp,date,year,month,dow,day,hour,minute,gid


spark.meta.op.definition.center_mass_1.signals.userid.column=timezoned.userid
spark.meta.op.definition.center_mass_1.signals.lat.column=timezoned.lat
spark.meta.op.definition.center_mass_1.signals.lon.column=timezoned.lon
spark.meta.op.definition.center_mass_1.signals.timestamp.column=timezoned.timestamp

spark.meta.op.output.center_mass_1.signals=if_step1


spark.meta.ds.input.columns.if_step1=userid,lat,lon,accuracy,idtype,timestamp,date,year,month,dow,day,hour,minute,gid


spark.meta.op.operation.track_type=trackType

spark.meta.op.input.track_type.signals=if_step1


spark.meta.op.definition.track_type.signals.lat.column=if_step1.lat
spark.meta.op.definition.track_type.signals.lon.column=if_step1.lon
spark.meta.op.definition.track_type.signals.userid.column=if_step1.userid
spark.meta.op.definition.track_type.signals.timestamp.column=if_step1.timestamp

spark.meta.op.output.track_type.signals=tracks


spark.meta.ds.output.columns.tracks=if_step1.userid,if_step1.lat,if_step1.lon,if_step1.accuracy,_velocity,if_step1.timestamp,if_step1.date,if_step1.year,if_step1.month,if_step1.dow,if_step1.day,if_step1.hour,if_step1.minute,if_step1.gid,_track_type
spark.meta.ds.input.columns.tracks=userid,lat,lon,_,_,_,_,_,_,_,_,_,_,_,track_type


spark.meta.op.operation.type_other=exactMatch

spark.meta.op.inputs.type_other=tracks

spark.meta.op.definition.type_other.match.values=car,bike,non_residential,extremely_large
spark.meta.op.definition.type_other.match.column=tracks.track_type

spark.meta.op.outputs.type_other=tracks_non_pedestrian


spark.meta.op.operation.track_type_filter=trackTypeFilter

spark.meta.op.input.track_type_filter.signals=if_step1

spark.meta.op.definition.track_type_filter.signals.lat.column=if_step1.lat
spark.meta.op.definition.track_type_filter.signals.lon.column=if_step1.lon
spark.meta.op.definition.track_type_filter.signals.timestamp.column=if_step1.timestamp
spark.meta.op.definition.track_type_filter.signals.userid.column=if_step1.userid

spark.meta.op.definition.track_type_filter.target.type=pedestrian
spark.meta.op.definition.track_type_filter.stop.time=900
spark.meta.op.definition.track_type_filter.upper.boundary.stop=0.05

spark.meta.op.output.track_type_filter.signals=pedestrian_typed


spark.meta.ds.output.columns.pedestrian_typed=if_step1.userid,if_step1.lat,if_step1.lon,if_step1.accuracy,if_step1.velocity,if_step1.timestamp,if_step1.date,if_step1.year,if_step1.month,if_step1.dow,if_step1.day,if_step1.hour,if_step1.minute,if_step1.gid,_point_type
spark.meta.ds.input.columns.pedestrian_typed=_,_,_,_,_,_,_,_,_,_,_,_,_,_,point_type


spark.meta.op.operation.remove_point_type=arrangeColumns

spark.meta.op.inputs.remove_point_type=pedestrian_typed
spark.meta.op.outputs.remove_point_type=pedestrian


spark.meta.ds.output.columns.pedestrian=pedestrian_typed._1_,pedestrian_typed._2_,pedestrian_typed._3_,pedestrian_typed._4_,pedestrian_typed._5_,pedestrian_typed._6_,pedestrian_typed._7_,pedestrian_typed._8_,pedestrian_typed._9_,pedestrian_typed._10_,pedestrian_typed._11_,pedestrian_typed._12_,pedestrian_typed._13_,pedestrian_typed._14_
spark.meta.ds.input.columns.pedestrian=userid,lat,lon,_,_,timestamp,_,_,_,_,_,_,_,_


spark.meta.op.operation.iron_glitch=ironGlitchFilter

spark.meta.op.input.iron_glitch.signals=pedestrian

spark.meta.op.definition.iron_glitch.signals.userid.column=pedestrian.userid
spark.meta.op.definition.iron_glitch.signals.lat.column=pedestrian.lat
spark.meta.op.definition.iron_glitch.signals.lon.column=pedestrian.lon
spark.meta.op.definition.iron_glitch.signals.timestamp.column=pedestrian.timestamp

spark.meta.op.definition.iron_glitch.glitch.distance=100
spark.meta.op.definition.iron_glitch.glitch.speed=360

spark.meta.op.output.iron_glitch.signals=if_step3


spark.meta.ds.input.columns.if_step3=userid,lat,lon,_,_,timestamp,_,_,_,_,_,_,_,_


spark.meta.op.operation.slow_motion=slowMotionFilter

spark.meta.op.input.slow_motion.signals=if_step3

spark.meta.op.definition.slow_motion.signals.userid.column=if_step3.userid
spark.meta.op.definition.slow_motion.signals.lat.column=if_step3.lat
spark.meta.op.definition.slow_motion.signals.lon.column=if_step3.lon
spark.meta.op.definition.slow_motion.signals.timestamp.column=if_step3.timestamp

spark.meta.op.definition.slow_motion.slow.distance=50
spark.meta.op.definition.slow_motion.slow.time=900

spark.meta.op.output.slow_motion.signals=if_step4


spark.meta.ds.input.columns.if_step4=userid,lat,lon,_,_,timestamp,_,_,_,_,_,_,_,_


spark.meta.op.operation.center_mass_2=trackCentroidFilter

spark.meta.op.input.center_mass_2.signals=if_step4

spark.meta.op.definition.center_mass_2.signals.userid.column=if_step4.userid
spark.meta.op.definition.center_mass_2.signals.lat.column=if_step4.lat
spark.meta.op.definition.center_mass_2.signals.lon.column=if_step4.lon
spark.meta.op.definition.center_mass_2.signals.timestamp.column=if_step4.timestamp

spark.meta.op.output.center_mass_2.signals=ironfelix


spark.meta.ds.input.columns.ironfelix=userid,lat,lon,_,_,timestamp,_,_,_,_,_,_,_,_


spark.meta.op.operation.aqua2=aqua2

spark.meta.op.input.aqua2.signals=ironfelix

spark.meta.op.definition.aqua2.signals.userid.column=ironfelix.userid
spark.meta.op.definition.aqua2.signals.lat.column=ironfelix.lat
spark.meta.op.definition.aqua2.signals.lon.column=ironfelix.lon
spark.meta.op.definition.aqua2.signals.timestamp.column=ironfelix.timestamp

spark.meta.op.definition.aqua2.coordinate.precision=4

spark.meta.op.output.aqua2.signals=aqua2


spark.meta.ds.input.columns.tracks_non_pedestrian=userid,_,_,_,_,_,_,_,_,dow,_,hour,_,_,_
spark.meta.ds.input.columns.aqua2=userid,_,_,_,_,_,_,_,_,dow,_,hour,_,_


spark.meta.op.operation.map_by_user=mapToPair
spark.meta.op.operation.map_tracks_non_pedestrian=mapToPair
spark.meta.op.operation.map_pedestrian=mapToPair
spark.meta.op.operation.map_aqua2=mapToPair

spark.meta.op.inputs.map_by_user=signals
spark.meta.op.inputs.map_tracks_non_pedestrian=tracks_non_pedestrian
spark.meta.op.inputs.map_pedestrian=pedestrian
spark.meta.op.inputs.map_aqua2=aqua2

spark.meta.op.definition.map_by_user.key.columns=signals.userid
spark.meta.op.definition.map_tracks_non_pedestrian.key.columns=tracks_non_pedestrian.userid,tracks_non_pedestrian.dow,tracks_non_pedestrian.hour
spark.meta.op.definition.map_pedestrian.key.columns=pedestrian.userid,pedestrian.dow,pedestrian.hour
spark.meta.op.definition.map_aqua2.key.columns=aqua2.userid,aqua2.dow,aqua2.hour

spark.meta.op.outputs.map_by_user=map_by_user
spark.meta.op.outputs.map_tracks_non_pedestrian=map_tracks_non_pedestrian
spark.meta.op.outputs.map_pedestrian=map_pedestrian
spark.meta.op.outputs.map_aqua2=map_aqua2

spark.meta.op.operation.count_by_user=countByKey
spark.meta.op.operation.count_tracks_non_pedestrian=countByKey
spark.meta.op.operation.count_pedestrian=countByKey
spark.meta.op.operation.count_aqua2=countByKey

spark.meta.op.inputs.count_by_user=map_by_user
spark.meta.op.inputs.count_tracks_non_pedestrian=map_tracks_non_pedestrian
spark.meta.op.inputs.count_pedestrian=map_pedestrian
spark.meta.op.inputs.count_aqua2=map_aqua2

spark.meta.op.outputs.count_by_user=count_by_user
spark.meta.op.outputs.count_tracks_non_pedestrian=count_tracks_non_pedestrian
spark.meta.op.outputs.count_pedestrian=count_pedestrian
spark.meta.op.outputs.count_aqua2=count_aqua2


spark.meta.task.tee.output=timezoned,tracks_non_pedestrian,pedestrian,aqua2,count_by_user,count_tracks_non_pedestrian,count_pedestrian,count_aqua2
spark.meta.ds.output.path={OUTPUT_PATH}
```

The good practice is to write keys in paragraphs grouped for each Operation, preceded by its input and output DataStream.

### Namespace Layers

As you see, each key begins with a prefix `spark.meta.`. One Ring can read its configuration from Spark context, and each Spark property must stark with `spark.` prefix. We add `meta.` (by convention; this can be any unique token of your choice) to distinguish our own properties from Spark's. A single tasks.ini may also contain a number of tasks if properly prefixed (name them `spark.process_name.`, `spark.another_process.` or so).

If you run One Ring in local mode, you can supply properties via .ini file, and omit all prefixes. Let assume that we've stripped them in the mind and now look directly into namespaces of keys.

The config is layered into several namespaces, and all parameter names must be unique in the corresponding namespace. These layers are distinguished, again, by some prefix.

First namespace layer is One Ring DistWrapper's `distcp.` which instructs it to generate a script file for the `dist-cp` utility calls.

```properties
distcp.wrap=both
distcp.exe=s3-dist-cp
```

It is [documented](DISTCP.md) in its own doc. CLI itself ignores all foreign layers.

### Variables

If a key or a value contains a token of the form `{ALL_CAPS}`, it'll be treated by the CLI as a configuration Variable, and will be replaced by the value supplied via command line or variables file (this topic is discussed in depth in the [Process execution how-to](EXECUTE.md)).

```properties
ds.input.part_count.signals={PARTS}
```

If the Variable's value wasn't supplied, no replacement will be made, unless the variable doesn't include a default value for itself in the form of `{ALL_CAPS:any default value}`. Default values may not contain the '}' symbol, but there are no other restrictions.

```properties
ds.input.part_count.signals={PARTS:50}
```

However, if a Variable after a replacement forms a reference to another Variable, it will not be processed recursively. We do not like to build a Turing-complete machine out of tasks.ini.

It is notable that Variables may be encountered at any side of `=` in the tasks.ini lines, and there is no restriction to number of them in a single line. You also can supply them with different defaults for each occurrence, if you desire.

### CLI Task of the Process

Next layer is `task.` and it contains properties that configure the CLI itself for the current Process' as a Spark job, or a CLI Task. 

```properties
task.operations=range_filter,accuracy_filter,h3,timezone,center_mass_1,track_type,type_other,track_type_filter,remove_point_type,iron_glitch,slow_motion,center_mass_2,aqua2,map_by_user,map_tracks_non_pedestrian,map_pedestrian,map_aqua2,count_by_user,count_tracks_non_pedestrian,count_pedestrian,count_aqua2
task.input.sink=signals


task.tee.output=timezoned,tracks_non_pedestrian,pedestrian,aqua2,count_by_user,count_tracks_non_pedestrian,count_pedestrian,count_aqua2
```

`task.operations` is a comma-separated list of Operation names to execute in the specified order. Any number of them, but not less than one. Names must be unique.

`task.input.sink` is an input sink. Any referred here DataStreams are considered as coming from the external sources, and will be created by Storage Adapters of CLI for the consumption of Operations.

`task.tee.output` is a T-connector. Any referred here DataStreams can be consumed by Operations as usual, but also are diverted by Storage Adapters of CLI into the result storage.

### Operation Instances

Operations share the layer `op.`, and have quite a number of sub-layers.

Operation of certain name is a Java class, but we don't like to call Operations by fully-qualified class names, and ask them how they would like to be called.

So, you must specify such short names for each of your Operations in the chain, for example:

```properties
op.operation.range_filter=rangeFilter
op.operation.accuracy_filter=accuracyFilter
op.operation.h3=h3
op.operation.timezone=timezone
op.operation.center_mass_1=trackCentroidFilter
op.operation.track_type=trackType
op.operation.map_by_user=mapToPair
op.operation.map_pedestrian=mapToPair
op.operation.count_pedestrian=countByKey
op.operation.count_aqua2=countByKey
``` 

You see that you may have any number of calls of the same Operation class in your Process, they'll be initialized as independently named instances.

### Operation Inputs and Outputs

Now we go down to Operations' sub-layers.

First is `op.input.` that defines which named DataStreams an Operation is about to consume, and `op.inputs.` for positional DataStreams, if an Operation can process an arbitary number or wildcard DataStreams.

Examples from the config are:

```properties
op.inputs.range_filter=signals
op.input.accuracy_filter.signals=range_accurate_signals
op.inputs.h3=accurate_signals
op.inputs.timezone=AG
op.input.center_mass_1.signals=timezoned
op.input.track_type.signals=if_step1
op.inputs.type_other=tracks
```

Notice that the keys end with just a name of an Operation in the case of positional inputs, or 'name of an Operation' + '.' + 'its internal name of input' for named ones.

Same is true for the `op.output.` and `op.outputs.` layers that describe which DataStreams emits this Operation. Examples:

```properties
op.outputs.range_filter=range_accurate_signals
op.output.accuracy_filter.signals=accurate_signals
op.outputs.h3=AG
op.outputs.timezone=timezoned
op.output.center_mass_1.signals=if_step1
op.output.track_type.signals=tracks
op.outputs.type_other=tracks_non_pedestrian
op.output.track_type_filter.signals=pedestrian_typed
```

A wildcard DataStream reference is defined like:

```properties
op.inputs.union=prefix*
```

It'll match all DataStreams with said prefix and will be automatically converted into a list with no particular order.

### Parameters of Operations

Next sub-layer is for Operation Parameters, `op.definition.`. Parameters have names, that takes the rest of `op.definition.` keys. And the first prefix of parameter name is name of the Operation it is belonging to.

Each Parameter definition is supplied to CLI by the Operation itself, and they are strongly typed. So they can have any `Number` descendant, `String`, `enum`s, `String[]` (as a comma-separated list), and `Boolean` values.

Some Parameters may be defined as optional, and in that case they have a default value.

Some Parameters may be dynamic, in that case they have a fixed prefix and variable ending.

Finally, there is a variety of Parameters that refer specifically to columns of input DataStreams, their names all end in `.column` or `.columns` by the convention, and values must point to a valid column or list of columns, or to one of values generated by the Operation. By convention, generated column names start with an underscore.

Look for some examples:

```properties
op.definition.range_filter.filtering.column=signals.accuracy
op.definition.range_filter.filtering.range=[0 50]
op.definition.h3.hash.level=9
op.definition.timezone.source.timezone.default=GMT
op.definition.timezone.destination.timezone.default={TZ}
op.definition.timezone.source.timestamp.column=AG.timestamp
op.definition.type_other.match.values=car,bike,non_residential,extremely_large
op.definition.track_type_filter.target.type=pedestrian
op.definition.track_type_filter.stop.time=900
op.definition.track_type_filter.upper.boundary.stop=0.05
op.definition.map_pedestrian.key.columns=pedestrian.userid,pedestrian.dow,pedestrian.hour
op.definition.map_aqua2.key.columns=aqua2.userid,aqua2.dow,aqua2.hour
```

Here `filering.column` of Operation names `range_filter` points to column `accuracy` from the DataStream `signals`, as well as `source.timestamp.column` of `timezone` is a reference to `AG.timestamp`. And `map_pedestrian`'s `key.columns` refers to list of `pedestrian` columns.

`hash.level` of `h3` is of type `Byte`, `type_other`'s `match.values` is `String[]`, and `track_type_filter`'s `upper.boundary.stop` is `Double`.

To set an optional Parameter to default value, you may omit that key altogether, or, if you like completeness, comment it out:
```properties
#op.definition.another_h3.hash.level=9
```

For the exhaustive table of each Operation Parameters, look for the docs inside your [./RESTWrapper/docs](./RESTWrapper/docs/index.md) directory (assuming you've successfully built the project, otherwise it'll be empty).

### Parameters of DataStreams

Next layer is the `ds.` configuration namespace of DataStreams, and its rules are quite different.

First off, DataStreams are always typed. There are types of:
* `Plain` (RDD consisting of just opaque Hadoop `Text`)
* `CSV` (column-based `Text` RDD with strong column references)
* `Fixed` (`CSV`, but column order and format is considered fixed)
* `Point` (object-based, contains point coordinates with metadata)
* `Polygon` (object-based, contains polygon outlines with metadata)
* `KeyValue` (PairRDD with an opaque key and column-based value like `CSV`)

Each DataStream can be configured as input for a number of Operations, and as an output of only one of them.

DataStream name is always the last part of any `ds.` key. And the set of DataStream Parameters is fixed.

`ds.input.path.` keys must point to some abstract paths for all DataStreams listed under the `task.input.sink` key. The format of the path must always include the protocol specification, and is validated by a Storage Adapter of the CLI (Adapters are discussed in the last section of this document).

For example, for a DataStream named 'signals' there is a path recognized by the S3 Direct Adapter:
```properties
ds.input.path.signals=s3d://{BUCKET_NAME}/key/name/with/{a,the}/mask*.spec.?
```

Notice the usage of glob expressions. '{a,the}' token won't be processed as a Variable, but it is expanded to list of 'a' and 'the' directories inside '/key/name/with' directory by Adapter.

Same true for `ds.output.path.` keys, that must be specified for all DataStreams listed under the `task.tee.output` key. Let divert DataStream 'scores' to local filesystem by default:
```properties
ds.output.path.scores={OUTPUT_PATH:file:/tmp/testing}/scores
```

But you may cheat. There are all-input and all-output default keys:
```properties
ds.input.path=jdbc:SELECT * FROM scheme.
ds.output.path=aero:output/
```

In that case, for each DataStream that doesn't have its own path, it'll be added to the end of corresponding default key without any separator. We don't recommend usage of these cheat codes in the production environment.

`ds.input.columns.` and `ds.output.columns.` layers define columns for column-based DataStreams or metadata properties for object-based ones. Column names must be unique for that particular DataStream.

Output columns must always refer to valid columns of inputs passed to the Operation that emits that DataStream, or its generated columns (whose names start with an underscore).

Input columns list just assigns new column names for all consuming Operations. It may contain a single underscore instead of some column name to make that column anonymous. Anyways, if a column is 'anonymous', it still may be referenced by its number starting from '_1_'.

There is an exhaustive example of all these rules:
```properties
ds.input.columns.signals=userid,lat,lon,accuracy,idtype,timestamp

ds.output.columns.AG=accurate_signals.userid,accurate_signals.lat,accurate_signals.lon,accurate_signals.accuracy,accurate_signals.idtype,accurate_signals.timestamp,_hash
ds.input.columns.AG=userid,lat,lon,accuracy,idtype,timestamp,gid

ds.output.columns.timezoned=AG.userid,AG.lat,AG.lon,AG.accuracy,AG.idtype,AG.timestamp,_output_date,_output_year_int,_output_month_int,_output_dow_int,_output_day_int,_output_hour_int,_output_minute_int,AG.gid
ds.input.columns.timezoned=userid,lat,lon,accuracy,idtype,timestamp,date,year,month,dow,day,hour,minute,gid

ds.output.columns.tracks=if_step1.userid,if_step1.lat,if_step1.lon,if_step1.accuracy,_velocity,if_step1.timestamp,if_step1.date,if_step1.year,if_step1.month,if_step1.dow,if_step1.day,if_step1.hour,if_step1.minute,if_step1.gid,_track_type
ds.input.columns.tracks=userid,lat,lon,_,_,_,_,_,_,_,_,_,_,_,track_type

ds.output.columns.pedestrian_typed=if_step1.userid,if_step1.lat,if_step1.lon,if_step1.accuracy,if_step1.velocity,if_step1.timestamp,if_step1.date,if_step1.year,if_step1.month,if_step1.dow,if_step1.day,if_step1.hour,if_step1.minute,if_step1.gid,_point_type
ds.input.columns.pedestrian_typed=_,_,_,_,_,_,_,_,_,_,_,_,_,_,point_type

ds.output.columns.pedestrian=pedestrian_typed._1_,pedestrian_typed._2_,pedestrian_typed._3_,pedestrian_typed._4_,pedestrian_typed._5_,pedestrian_typed._6_,pedestrian_typed._7_,pedestrian_typed._8_,pedestrian_typed._9_,pedestrian_typed._10_,pedestrian_typed._11_,pedestrian_typed._12_,pedestrian_typed._13_,pedestrian_typed._14_
ds.input.columns.pedestrian=userid,lat,lon,_,_,timestamp,_,_,_,_,_,_,_,_
```

In `CSV` varieties of DataStreams, columns are separated by a separator character, so there are `ds.input.separator.` and `ds.output.separator.` layers, along with cheat keys `ds.input.separator` and `ds.output.separator` that set them globally. The super global default value of column separator is the tabulation (TAB, 0x09) character.

Final layers control the partitioning of DataStreams underlying RDDs, namely, `ds.input.part_count.` and `ds.output.part_count.` layers. These are quite important because the only super global default value for the part counts is always 1 (one) part, and no cheats are allowed. You must always set them for at least Task initial imput DataStreams, and may tune the partitioning in the middle of the Process according to the further flow of the Task.

If both `part_count.`'s are specifies for some intermediate DataStream, it will be repartitioned first to the output one (immediately after the Operation that generated it), and then to input one (before feeding it to the first consuming Operation). Keep that in the mind.

### Storage Adapters

There are the following Storage Adapters implemented in the CLI:

* Hadoop (fallback, uses all protocols available in your Spark environment, i.e. 'file:', 's3:')
* HDFS (same Hadoop, but just for 'hdfs:' protocol)
* S3 Direct (any S3-compatible storage with a protocol of 's3d:')
* Aerospike ('aero:')
* JDBC ('jdbc:')

The fallback Hadoop Adapter is called if and only if another Adapter doesn't recognize the protocol of the path, so the priority of 'hdfs:' protocol is higher than other platform-supplied ones.

Storage Adapters share two namesake layers of `input.` and `output.`, and all their Parameters are global.

Hadoop Adapter has no explicit Parameters. So does HDFS Adapter.

S3 Direct uses standard Amazon S3 client provider and has only the Parameter for output:
* `output.content.type` with a default of 'text/csv' 

JDBC Adapter Parameters are:
* `input.jdbc.driver` and `output.jdbc.driver` for fully qualified class names of driver, available in the classpath. No default
* `input.jdbc.url` and `output.jdbc.url` for connection URLs. No default
* `input.jdbc.user` and `output.jdbc.user` with no default
* `input.jdbc.password` and `output.jdbc.password` with no default
* `output.jdbc.batch.size` for output batch size, default is '500'

Aerospike Adapter Parameters are:
* `input.aerospike.host` and `output.aerospike.host` defaults to 'localhost'
* `input.aerospike.port` and `output.aerospike.port` defaults to '3000'

This concludes the configuration of One Ring.