**One Ring** is a pipelining framework to perform burst (one-time, on-demand) Apache Spark computations of complex Processes defined by declarative Templates over data sets with a very loosely described schema.
 
Each step of such Process is a self-contained Operation, that has a deliberately described set of not too strongly typed Input and Output RDDs (collectively referred as DataStreams), and can be flexibly configured by a set of more strongly typed Parameter Definitions. Operations are automatically chained together into a single Spark job and produce, consume, and share DataStreams.

Historically One Ring has been aimed at GIS processing, but of course it can also be utilized in any other computation-heavy domain.

There are docs on the main One Ring CLI:
* [How to build](BUILD.md) executable JARs of One Ring utilities
* [How to write](CONFIGURE.md) the Process' Template — that is a most important and most complex step
* [How to execute](EXECUTE.md) it locally and in the Amazon's EMR with the help of [integration scripts](https://github.com/PastorGL/one-ring-emr) and [settings](https://github.com/PastorGL/one-ring-emr-settings)
* [How to extend](EXTEND.md) One Ring with your own Operations

After you've successfully built One Ring, its [self-generated docs on all Operations will be available here](RESTWrapper/docs/index.md), with examples.

And there are docs on One Ring Utilities:
* [One Ring Dist](DISTCP.md) to replace `[s3-]dist-cp` with more effective implementation
* [One Ring REST](REST.md) to call One Ring in more or less REST-ish way
* [One Ring Composer](COMPOSE.md) to create advanced Processes from Template fragments

[Your contributions are welcome](CONTRIBUTE.md)! Please see the list of projects and issues in the original GitHub repo at https://github.com/PastorGL/OneRing
