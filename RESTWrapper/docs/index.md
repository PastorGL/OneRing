
# There are following packages available

Package | Description
--- | ---
[`proximity.operations`](./package/proximity.operations.md) | Geometric proximity-related filters
[`math.operations`](./package/math.operations.md) | Perform a number of math calculations on Double-typed column values of an RDD, using row-series, column-series, or aggregating-across functions
[`simplefilters.operations`](./package/simplefilters.operations.md) | Simple filters of CSV row data by a specified column value
[`datetime.operations`](./package/datetime.operations.md) | Read timestamp columns of CSV RDDs, explode into individual fields, convert time zone and filter or split RDDs by timestamp field values
[`populations.operations`](./package/populations.operations.md) | Heuristics that determine user populations in the source RDDs or just formally sort them out
[`commons.operations`](./package/commons.operations.md) | Create RDDs from text or CSV files, split them into PairRDDs, repartition and perform basic SQL-like operations on them
[`geohashing.operations`](./package/geohashing.operations.md) | Calculate geohashes for (lat;lon) pairs using different hashing functions
[`spatial.operations`](./package/spatial.operations.md) | Operations to read spatial Point and Polygon RDD from GeoJSON and CSV files and write them back
[`columnar.operations`](./package/columnar.operations.md) | Arrange columns of CSV RDDs and generate new columns, either plain text or cryptographic digest of other columns
