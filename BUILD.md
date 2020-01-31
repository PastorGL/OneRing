To build One Ring you need Apache Maven, version 3.5 or higher.

Make sure you've cloned this repo with all submodules.
```bash
git clone --recursive https://github.com/PastorGL/OneRing.git
```

### One Ring CLI

If you're planning to compute on an EMR cluster, just `cd` to OneRing directory and execute Maven in the default profile:
```bash
mvn clean package
```

The `./TaskWrapper/target/one-ring-cli.jar` is a fat executable JAR targeted at the Spark environment provided by EMR version 5.23.0.

If you're planning to build a local artifact with full Spark built-in, execute build in 'local' profile:
```bash
mvn clean package -Plocal
```
Make sure the resulting JAR is about ~100 MB in size.

It isn't recommended to skip tests in the build process, but if you're running Spark locally on your build machine, you could add `-DskipTests` to Maven command line, because they will interfere.

After you've built your CLI artifact, look into [./RESTWrapper/docs](./RESTWrapper/docs/index.md) for the automatically generated documentation of available Packages and Operations (in Markdown format).

You may now proceed to [how to configure](CONFIGURE.md) your Process pipeline.

If you have developed and proved a number of Process Templates, and want to fuse some of them together into a fewer number of larger Processes, you may call the Composer utility from the local version of CLI. It is documented [in its own doc](COMPOSE.md).

### One Ring Dist

The `./DistWrapper/target/one-ring-dist.jar` is a fat executable JAR that generates Hadoop's `dist-cp` or EMR's `s3-dist-cp` script to copy the source data from the external storage (namely, S3) to cluster's internal HDFS, and the computation's result back.

It is an optional component, documented in [its own doc](DISTCP.md) too.

### One Ring REST

The `./RESTWrapper/target/one-ring-rest.jar` is a fat executable JAR that serves a REST-ish back-end for the not-yet-implemented (but much wanted) Visual Template Editor. It also serves the docs via dedicated endpoint.

Also an optional component, documented [here](REST.md).