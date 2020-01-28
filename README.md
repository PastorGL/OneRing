**One Ring** is a pipelining framework to perform burst (one-time, on-demand) calculations for complex processes defined by declarative templates on an Apache Spark cluster.
 
Each step of such Process is a self-contained Operation, that has a described set of Input and Output RDDs (collectively referred as DataStreams) and can be flexibly configured by a set of strongly-typed Definitions, and chained together in a single Spark job.

Originally One Ring has been aimed on GIS processing, but of course it can also be effectively used in any other compuption-heavy domains.

There are the docs:

* [How to build](BUILD.md) an executable JAR
* [How to write](CONFIG.md) the process Template
* [How to execute](EXECUTE.md) it locally and in the Amazon's EMR
* [How to extend](EXTEND.md) One Ring with your own Operations

Your contributions are welcome. Please see the list of projects and issues in the original GitHub repo at https://github.com/PastorGL/OneRing