
# Operation `subtract` from [`commons.operations`](../package/commons.operations.md)

Take two RDDs and emit an RDD that consists of rows of first (the minuend) that do not present in the second (the subtrahend). If either RDD is a Plain one, entire rows will be matched. If either of RDDs is a PairRDD, its keys will be used to match instead. If either RDD is a CSV, you should specify the column to match

Configuration examples: [JSON](../operation/subtract/example.json), [.ini](../operation/subtract/example.ini)

## Inputs

### Positional

Allowed types are `KeyValue`, `Plain`, `CSV`

There is a required minimum of **2** positional inputs


## Outputs

### Positional

Resulting types are `Passthru`


## Parameters


### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`subtrahend.column` | `String` | Column to match a value in the subtrahend if it is a CSV RDD |  | `null` — By default, treat subtrahend RDD as a plain one
`minuend.column` | `String` | Column to match a value in the minuend if it is a CSV RDD |  | `null` — By default, treat minuend RDD as a plain one


[Back to index](../index.md)
