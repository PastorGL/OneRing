### Requirements

To build One Ring you need Apache Maven, version 3.6.0 or higher.

Make sure you've cloned this repo with all submodules.
```bash
git clone --recursive https://github.com/PastorGL/OneRing.git
```

### One Ring CLI

If you're planning to compute on an EMR cluster, just `cd` to OneRing directory and just invoke Maven:
```bash
mvn clean package
```

The `./TaskWrapper/target/one-ring-cli.jar` is a fat executable JAR targeted at the Spark environment provided by EMR version 6.1.0.

If you're planning to build a locally executable artifact with full Spark runtime built-in, useful for debug and development purposes, invoke Maven in 'local' profile:
```bash
mvn clean package -Plocal
```
Make sure the resulting JAR is about ~100 MB in size.

It isn't recommended skipping tests in the build process.

After you've built your CLI artifact (in either profiles), look into [./RESTWrapper/docs](./RESTWrapper/docs/index.md) for the automatically generated documentation of available Packages and Operations (in Markdown format).

You may now proceed to [how to configure](CONFIGURE.md) your Process pipeline.

If you have developed and proved a number of Process Templates, and want to fuse some of them together into a fewer number of larger Processes, you may call the Composer utility from the local version of CLI. It is documented [in its own doc](COMPOSE.md).

### One Ring Dist

The `./DistWrapper/target/one-ring-dist.jar` is a fat executable JAR that replaces Hadoop's `dist-cp` or EMR's `s3-dist-cp` utilities with a more efficient implementation.

Instead of ancient MapReduce it uses Spark and offers much more parallelism. Also, it is able to parse CSVs to get rid of unneeded columns on a source data copy stage, and can read and merge Parquet files.

Use it to copy the source data from the external storage (namely, S3) to cluster's internal HDFS, and the computation's result back, as documented in [its own doc](DISTCP.md).

### One Ring REST

The `./RESTWrapper/target/one-ring-rest.jar` is a fat executable JAR that serves a REST-ish back-end for the not-yet-implemented (but much wanted) Visual Template Editor. It also serves the docs via dedicated endpoint.

It is documented [here](REST.md).