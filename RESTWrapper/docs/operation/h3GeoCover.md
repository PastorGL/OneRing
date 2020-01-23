
# Operation `h3GeoCover` from [`spatial.operations`](../package/spatial.operations.md)

Create a complete (non-collapsed) H3 coverage from the Polygon or Point RDD. Can pass any properties from the source geometries to the resulting CSV RDD columns, for each hash per each geometry

Configuration examples: [JSON](../operation/h3GeoCover/example.json), [.ini](../operation/h3GeoCover/example.ini)

## Inputs

### Positional

Allowed types are `Point`, `Polygon`



## Outputs

### Positional

Resulting types are `CSV`


## Parameters


### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`hash.level` | `Integer` | Level of the hash |  | `9` — Default hash level


[Back to index](../index.md)
