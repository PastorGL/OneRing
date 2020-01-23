
# Operation `reach` from [`populations.operations`](../package/populations.operations.md)

Statistical indicator for some audience reach

Configuration examples: [JSON](../operation/reach/example.json), [.ini](../operation/reach/example.ini)

## Inputs


### Named

Name | Type | Description
--- | --- | ---
`values` | `CSV` | Values to group and count
`population` | `Plain` | General population

## Outputs

### Positional

Resulting types are `Fixed`


## Parameters

### Mandatory

Name | Type | Description | Allowed values
--- | --- | --- | ---
`values.count.column` | `String` | Column to count unique values of other column | 
`values.value.column` | `String` | Column for counting unique values per other column | 



[Back to index](../index.md)
