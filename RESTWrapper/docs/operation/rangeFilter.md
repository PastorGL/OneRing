
# Operation `rangeFilter` from [`simplefilters.operations`](../package/simplefilters.operations.md)

In a CSV RDD, take a column to filter all rows that have a Double value in this column that lies outside of the set absolute range

Configuration examples: [JSON](../operation/rangeFilter/example.json), [.ini](../operation/rangeFilter/example.ini)

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
`filtering.column` | `String` | Column with Double values to apply the filter | 
`filtering.range` | `String` | Range syntax is [BOTTOM;TOP) where brackets mean inclusive border and parens exclusive. Either boundary is optional, but not both at the same time. Examples: (0 1000], []-7.47;7.48, [-1000;) | 



[Back to index](../index.md)
