
# Operation `listMatch` from [`simplefilters.operations`](../package/simplefilters.operations.md)

This operation is a filter that only passes the RDD rows that have an exact match with a specific set of allowed values in a given column sourced from another RDD's column

Configuration examples: [JSON](../operation/listMatch/example.json), [.ini](../operation/listMatch/example.ini)

## Inputs


### Named

Name | Type | Description
--- | --- | ---
`source` | `CSV` | CSV RDD with to be filtered
`values` | `CSV` | CSV RDD with values to match any of them

## Outputs

### Positional

Resulting types are `Passthru`


## Parameters

### Mandatory

Name | Type | Description | Allowed values
--- | --- | --- | ---
`source.match.column` | `String` | Column to match a value | 
`values.match.column` | `String` | Column with a value to match | 



[Back to index](../index.md)
