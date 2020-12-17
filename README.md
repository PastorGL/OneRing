**One Ring** is a pipelining framework to perform burst (one-time, on-demand) Apache Spark computations of complex Processes defined by declarative Templates over data sets with a very loosely described schema.
 
Each step of such Process is a self-contained Operation, that has a deliberately described set of not too strongly typed Input and Output RDDs (collectively referred as DataStreams), and can be flexibly configured by a set of more strongly typed Parameter Definitions. Operations are automatically chained together into a single Spark job and produce, consume, and share DataStreams.

Historically One Ring has been aimed at GIS processing, but of course it can also be utilized in any other computation-heavy domain.

There are the docs:

* [How to build](BUILD.md) executable JARs of One Ring utilities
* [How to write](CONFIGURE.md) the Process' Template — that is a most important and most complex step
* [How to execute](EXECUTE.md) it locally and in the Amazon's EMR
* [How to extend](EXTEND.md) One Ring with your own Operations

[Your contributions are welcome](CONTRIBUTE.md)! Please see the list of projects and issues in the original GitHub repo at https://github.com/PastorGL/OneRing
