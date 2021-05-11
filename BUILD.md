### Requirements

To build One Ring you need Apache Maven, version 3.6.0 or higher.

Make sure you've cloned this repo with all submodules.
```bash
git clone --recursive https://github.com/PastorGL/OneRing.git
```

### One Ring CLI

If you're planning to compute on an EMR cluster, just `cd` to OneRing directory and invoke Maven:
```bash
mvn clean package
```

The `./CLI/target/one-ring-cli.jar` is a fat executable JAR targeted at the Spark environment provided by EMR version 6.1.0.

If you're planning to build a locally executable artifact with full Spark runtime built-in, useful for debug and development purposes, invoke Maven in 'local' profile:
```bash
mvn clean package -Plocal
```
Make sure the resulting JAR is about ~100 MB in size.

It isn't recommended skipping tests in the build process.

After you've built your CLI artifact (in either profiles), look into [./REST/docs](./REST/docs/index.md) for the automatically generated documentation of available Packages and Operations (in Markdown format).

You may now proceed to [how to configure](CONFIGURE.md) your Process pipeline.

If you have developed and proved a number of Process Templates, and want to fuse some of them together into a fewer number of larger Processes, you may call the Composer utility from the local version of CLI. It is documented [in its own doc](COMPOSE.md).

### One Ring Dist

The `./Dist/target/one-ring-dist.jar` is a utility to replace `[s3-]dist-cp` with an implementation tailored specifically for One Ring workloads and ETL pipelines.

It is able to utilize any FileSystems provided by Hadoop to load data from external storage, and prepare it for One Ring, which doesn't consume anything that is not textual (e.g. CSV). For example, it can read and merge Parquet files while omitting unneeded columns. Also, it provides a lightweight extensible interface for non-FileSystem data storages, like an S3-compatible storage that is not Amazon's S3.

Same for storing One Ring result to Parquet files into an external storage, or into a database via JDBC.

Dist does that in a very efficient fashion, comparing to `[s3]-dist-cp`, with much wider parallelism.

It is documented in [its own doc](DIST.md).

### One Ring REST

The `./REST/target/one-ring-rest.jar` is a fat executable JAR that serves a REST-ish back-end for the Visual Template Editor (and the Editor itself). It also serves the docs via dedicated endpoint.

It is documented [here](REST.md).
