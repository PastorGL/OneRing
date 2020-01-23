
# Operation `exactMatch` from [`simplefilters.operations`](../package/simplefilters.operations.md)

This operation is a filter that only passes the rows that have an exact match with a specific set of allowed values in a given column

Configuration examples: [JSON](../operation/exactMatch/example.json), [.ini](../operation/exactMatch/example.ini)

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
`match.column` | `String` | Column to match a value | 
`match.values` | `String[]` | Values to match any of them | 



[Back to index](../index.md)
