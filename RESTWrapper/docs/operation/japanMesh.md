
# Operation `japanMesh` from [`geohashing.operations`](../package/geohashing.operations.md)

For each input row with a coordinate pair, generate Japan Mesh hash with a selected level

Configuration examples: [JSON](../operation/japanMesh/example.json), [.ini](../operation/japanMesh/example.ini)

## Inputs

### Positional

Allowed types are `CSV`



## Outputs

### Positional

Resulting types are `CSV`


## Parameters

### Mandatory

Name | Type | Description | Allowed values
--- | --- | --- | ---
`lat.column` | `String` | Column with latitude, degrees | 
`lon.column` | `String` | Column with longitude, degrees | 

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`hash.level` | `Integer` | Level of the hash |  | `6` — Default hash level


[Back to index](../index.md)
