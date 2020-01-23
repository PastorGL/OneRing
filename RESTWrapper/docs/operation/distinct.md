
# Operation `distinct` from [`commons.operations`](../package/commons.operations.md)

If input is an CSV RDD, this operation takes a column and extract a list of distinct values occurred in this column. If input is a PairRDD, it returns distinct elements from this PairRDD.

Configuration examples: [JSON](../operation/distinct/example.json), [.ini](../operation/distinct/example.ini)

## Inputs

### Positional

Allowed types are `KeyValue`, `CSV`



## Outputs

### Positional

Resulting types are `Passthru`


## Parameters


### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`unique.column` | `String` | Input column that contains a value treated as a distinction key |  | `null` — By default, no unique column is set for an RDD


[Back to index](../index.md)
