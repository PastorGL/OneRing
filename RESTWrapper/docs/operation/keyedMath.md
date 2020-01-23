
# Operation `keyedMath` from [`math.operations`](../package/math.operations.md)

Take an PairRDD and calculate a 'series' mathematical function over all values (or a selected value column), treated as a Double, under each unique key

Configuration examples: [JSON](../operation/keyedMath/example.json), [.ini](../operation/keyedMath/example.ini)

## Inputs

### Positional

Allowed types are `KeyValue`



## Outputs

### Positional

Resulting types are `KeyValue`


## Parameters

### Mandatory

Name | Type | Description | Allowed values
--- | --- | --- | ---
`calc.function` | `CalcFunction` | The mathematical function to perform |  `SUM` — Calculate the sum of columns, optionally add a constant<br> `POWERMEAN` — Calculate the power mean of columns with a set power<br> `POWER_MEAN` — Alias of POWERMEAN<br> `MEAN` — Calculate the arithmetic mean of columns, optionally shifted towards a constant<br> `AVERAGE` — Alias of MEAN<br> `RMS` — Calculate the square root of the mean square (quadratic mean or RMS)<br> `ROOTMEAN` — Alias of RMS<br> `ROOT_MEAN` — Alias of RMS<br> `ROOTMEANSQUARE` — Alias of RMS<br> `ROOT_MEAN_SQUARE` — Alias of RMS<br> `MIN` — Find the minimal value among columns, optionally with a set floor<br> `MAX` — Find the maximal value among columns, optionally with a set ceil<br> `MUL` — Multiply column values, optionally also by a constant<br> `MULTIPLY` — Alias of MUL

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`calc.column` | `String` | Column with a Double to use as series source |  | `null` — By default, use entire value as calculation source
`calc.const` | `Double` | An optional constant value for the selected function |  | `null` — By default the constant isn't set


[Back to index](../index.md)
