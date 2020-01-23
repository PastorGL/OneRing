
# Package `commons.operations`

Create RDDs from text or CSV files, split them into PairRDDs, repartition and perform basic SQL-like operations on them

Verb | Description | Examples
--- | --- | ---
[`collapsePair`](../operation/collapsePair.md) | Collapse Pair RDD into a CSV RDD | [JSON](../operation/collapsePair/example.json) [.ini](../operation/collapsePair/example.ini)
[`subtract`](../operation/subtract.md) | Take two RDDs and emit an RDD that consists of rows of first (the minuend) that do not present in the second (the subtrahend). If either RDD is a Plain one, entire rows will be matched. If either of RDDs is a PairRDD, its keys will be used to match instead. If either RDD is a CSV, you should specify the column to match | [JSON](../operation/subtract/example.json) [.ini](../operation/subtract/example.ini)
[`joinByKey`](../operation/joinByKey.md) | Takes two PairRDDs and joins them by key. In the terms of joins, first PairRDD is the 'left', second is the 'right'. Missing rows from either PairRDDs are settable and must have a proper column count. | [JSON](../operation/joinByKey/example.json) [.ini](../operation/joinByKey/example.ini)
[`distinct`](../operation/distinct.md) | If input is an CSV RDD, this operation takes a column and extract a list of distinct values occurred in this column. If input is a PairRDD, it returns distinct elements from this PairRDD. | [JSON](../operation/distinct/example.json) [.ini](../operation/distinct/example.ini)
[`mapToPair`](../operation/mapToPair.md) | Take a CSV RDD and transform it to PairRDD using selected columns to build a key, optionally limiting key size | [JSON](../operation/mapToPair/example.json) [.ini](../operation/mapToPair/example.ini)
[`union`](../operation/union.md) | Take a number of RDDs (in the form of the list and/or prefixed wildcard) and union them into one | [JSON](../operation/union/example.json) [.ini](../operation/union/example.ini)
[`nop`](../operation/nop.md) | This operation does nothing, just passes all its inputs as all outputs | [JSON](../operation/nop/example.json) [.ini](../operation/nop/example.ini)
[`countByKey`](../operation/countByKey.md) | Count values under the same key in a given PairRDD. Output is key to Long count PairRDD. | [JSON](../operation/countByKey/example.json) [.ini](../operation/countByKey/example.ini)


[Back to index](../index.md)
