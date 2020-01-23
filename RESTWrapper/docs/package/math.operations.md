
# Package `math.operations`

Perform a number of math calculations on Double-typed column values of an RDD, using row-series, column-series, or aggregating-across functions

Verb | Description | Examples
--- | --- | ---
[`columnsMath`](../operation/columnsMath.md) | This operation performs one of the predefined mathematical operations on selected set of columns inside each input row, generating a column with a result. Data type is implied Double | [JSON](../operation/columnsMath/example.json) [.ini](../operation/columnsMath/example.ini)
[`keyedMath`](../operation/keyedMath.md) | Take an PairRDD and calculate a 'series' mathematical function over all values (or a selected value column), treated as a Double, under each unique key | [JSON](../operation/keyedMath/example.json) [.ini](../operation/keyedMath/example.ini)
[`seriesMath`](../operation/seriesMath.md) | Take an CSV RDD and calculate a 'series' mathematical function over all values in a set column, treated as a Double | [JSON](../operation/seriesMath/example.json) [.ini](../operation/seriesMath/example.ini)
[`weightedSum`](../operation/weightedSum.md) | Take a number of CSV RDDs, and generate a weighted sum by count and value columns, while using a set of payload column values as a key to join inputs. Each combined payload value is not required to be unique per its input | [JSON](../operation/weightedSum/example.json) [.ini](../operation/weightedSum/example.ini)


[Back to index](../index.md)
