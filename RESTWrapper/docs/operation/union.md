
# Operation `union` from [`commons.operations`](../package/commons.operations.md)

Take a number of RDDs (in the form of the list and/or prefixed wildcard) and union them into one

Configuration examples: [JSON](../operation/union/example.json), [.ini](../operation/union/example.ini)

## Inputs

### Positional

Allowed types are `Plain`



## Outputs

### Positional

Resulting types are `Passthru`


## Parameters


### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`spec` | `UnionSpec` | Union specification |  `CONCAT` — Just concatenate inputs, don't look into records<br> `XOR` — Only emit records that occur strictly in one input RDD<br> `AND` — Only emit records that occur in all input RDDs | `CONCAT` — By default, just concatenate


[Back to index](../index.md)
