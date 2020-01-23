
# Operation `percentileFilter` from [`simplefilters.operations`](../package/simplefilters.operations.md)

In a CSV RDD, take a column to filter all rows that have a Double value in this column that lies outside of the set percentile range

Configuration examples: [JSON](../operation/percentileFilter/example.json), [.ini](../operation/percentileFilter/example.ini)

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

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`percentile.bottom` | `Byte` | Bottom of percentile range (inclusive) |  | `-1` — By default, do not set bottom percentile
`percentile.top` | `Byte` | Top of percentile range (inclusive) |  | `-1` — By default, do not set top percentile


[Back to index](../index.md)
