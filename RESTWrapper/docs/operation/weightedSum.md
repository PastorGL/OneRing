
# Operation `weightedSum` from [`math.operations`](../package/math.operations.md)

Take a number of CSV RDDs, and generate a weighted sum by count and value columns, while using a set of payload column values as a key to join inputs. Each combined payload value is not required to be unique per its input

Configuration examples: [JSON](../operation/weightedSum/example.json), [.ini](../operation/weightedSum/example.ini)

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
`count.col` | `String` | A column with the Long count | 
`value.col` | `String` | A column with the Double value | 
`payload.columns` | `String[]` | A list of payload columns to use as a join key and pass to the output, sans the input prefix | 

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`default.columns` | `String[]` | A list of default columns for each input, in the case if they are generated or wildcard |  | `null` — By default, no columns for wildcard inputs are specified


[Back to index](../index.md)
