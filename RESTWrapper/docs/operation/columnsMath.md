
# Operation `columnsMath` from [`math.operations`](../package/math.operations.md)

This operation performs one of the predefined mathematical operations on selected set of columns inside each input row, generating a column with a result. Data type is implied Double

Configuration examples: [JSON](../operation/columnsMath/example.json), [.ini](../operation/columnsMath/example.ini)

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
`calc.columns` | `String[]` | Columns with source values | 
`calc.function` | `CalcFunction` | The mathematical function to perform |  `SUM` — Calculate the sum of columns, optionally add a constant<br> `POWERMEAN` — Calculate the power mean of columns with a set power<br> `POWER_MEAN` — Alias of POWERMEAN<br> `MEAN` — Calculate the arithmetic mean of columns, optionally shifted towards a constant<br> `AVERAGE` — Alias of MEAN<br> `RMS` — Calculate the square root of the mean square (quadratic mean or RMS)<br> `ROOTMEAN` — Alias of RMS<br> `ROOT_MEAN` — Alias of RMS<br> `ROOTMEANSQUARE` — Alias of RMS<br> `ROOT_MEAN_SQUARE` — Alias of RMS<br> `MIN` — Find the minimal value among columns, optionally with a set floor<br> `MAX` — Find the maximal value among columns, optionally with a set ceil<br> `MUL` — Multiply column values, optionally also by a constant<br> `MULTIPLY` — Alias of MUL

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`calc.const` | `Double` | An optional constant value for the selected function |  | `null` — By default the constant isn't set


[Back to index](../index.md)
