
# Operation `seriesMath` from [`math.operations`](../package/math.operations.md)

Take an CSV RDD and calculate a 'series' mathematical function over all values in a set column, treated as a Double

Configuration examples: [JSON](../operation/seriesMath/example.json), [.ini](../operation/seriesMath/example.ini)

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
`calc.column` | `String` | Column with a Double to use as series source | 
`calc.function` | `SeriesCalcFunction` | The mathematical function to perform |  `STDDEV` — Calculate Standard Deviation of a value<br> `FIT` — Re-normalize value into a range of 0..calc.const<br> `RENORMALIZE` — Alias of FIT<br> `NORMALIZE` — Alias of FIT

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`calc.const` | `Double` | An optional constant value for the selected function |  | `100.0` — Default upper value for the renormalization operation


[Back to index](../index.md)
