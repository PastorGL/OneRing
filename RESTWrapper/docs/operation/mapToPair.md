
# Operation `mapToPair` from [`commons.operations`](../package/commons.operations.md)

Take a CSV RDD and transform it to PairRDD using selected columns to build a key, optionally limiting key size

Configuration examples: [JSON](../operation/mapToPair/example.json), [.ini](../operation/mapToPair/example.ini)

## Inputs

### Positional

Allowed types are `CSV`



## Outputs

### Positional

Resulting types are `KeyValue`


## Parameters

### Mandatory

Name | Type | Description | Allowed values
--- | --- | --- | ---
`key.columns` | `String[]` | To form a key, selected column values (in order, specified here) are glued together with an output delimiter | 

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`value.columns` | `String[]` | To form a value, selected column values (in order, specified here) are glued together with an output delimiter |  | `null` — If not set (and by default) use entire source line as a value as it is
`key.length` | `Integer` | Key size limit to set number of characters |  | `-1` — If needed, limit key length by the set number of characters. By default, don't


[Back to index](../index.md)
