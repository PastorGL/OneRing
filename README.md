**One Ring** is a pipelining framework to perform burst (one-time, on-demand) Apache Spark calculations of complex Processes defined by declarative Templates.
 
Each step of such Process is a self-contained Operation, that has a described set of Input and Output RDDs (collectively referred as DataStreams) and can be flexibly configured by a set of strongly-typed Parameter Definitions. Operations are automatically chained together into a single Spark job and produce, consume, and share DataStreams.

Historically One Ring has been aimed at GIS processing, but of course it can also be utilized in any other computation-heavy domain.

There are the docs:

* [How to build](BUILD.md) an executable JAR
* [How to write](CONFIG.md) the Process' Template
* [How to execute](EXECUTE.md) it locally and in the Amazon's EMR
* [How to extend](EXTEND.md) One Ring with your own Operations

[Your contributions are welcome](CONTRIBUTE.md)! Please see the list of projects and issues in the original GitHub repo at https://github.com/PastorGL/OneRing