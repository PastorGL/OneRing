
# Operation `customColumn` from [`columnar.operations`](../package/columnar.operations.md)

Insert a custom column in the input CSV at a given index

Configuration examples: [JSON](../operation/customColumn/example.json), [.ini](../operation/customColumn/example.ini)

## Inputs

### Positional

Allowed types are `CSV`



## Outputs

### Positional

Resulting types are `Passthru`


## Parameters

### Mandatory

Name | Type | Description | Allowed values
--- | --- | --- | ---
`custom.column.value` | `String` | Fixed value of a custom column (opaque, not parsed in any way) | 

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`custom.column.index` | `Integer` | Position to insert a column. Counts from 0 onwards from the beginning of a row, or from the end if < 0 (-1 is last and so on) |  | `-1` — By default, add to the end of a row


[Back to index](../index.md)
