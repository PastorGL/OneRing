
# Package `spatial.operations`

Operations to read spatial Point and Polygon RDD from GeoJSON and CSV files and write them back

Verb | Description | Examples
--- | --- | ---
[`pointCsvOutput`](../operation/pointCsvOutput.md) | Take a Point RDD and produce a CSV file | [JSON](../operation/pointCsvOutput/example.json) [.ini](../operation/pointCsvOutput/example.ini)
[`polygonH3Source`](../operation/polygonH3Source.md) | Take a csv with H3 hashes and produce a Polygon RDD | [JSON](../operation/polygonH3Source/example.json) [.ini](../operation/polygonH3Source/example.ini)
[`polygonJsonOutput`](../operation/polygonJsonOutput.md) | Take a Polygon RDD and produce a GeoJSON fragment file | [JSON](../operation/polygonJsonOutput/example.json) [.ini](../operation/polygonJsonOutput/example.ini)
[`pointCsvSource`](../operation/pointCsvSource.md) | Take a CSV file and produce a Polygon RDD | [JSON](../operation/pointCsvSource/example.json) [.ini](../operation/pointCsvSource/example.ini)
[`pointJsonSource`](../operation/pointJsonSource.md) | Take GeoJSON fragment file and produce a Point RDD | [JSON](../operation/pointJsonSource/example.json) [.ini](../operation/pointJsonSource/example.ini)
[`polygonJsonSource`](../operation/polygonJsonSource.md) | Take GeoJSON fragment file and produce a Polygon RDD | [JSON](../operation/polygonJsonSource/example.json) [.ini](../operation/polygonJsonSource/example.ini)
[`h3GeoCover`](../operation/h3GeoCover.md) | Create a complete (non-collapsed) H3 coverage from the Polygon or Point RDD. Can pass any properties from the source geometries to the resulting CSV RDD columns, for each hash per each geometry | [JSON](../operation/h3GeoCover/example.json) [.ini](../operation/h3GeoCover/example.ini)


[Back to index](../index.md)
