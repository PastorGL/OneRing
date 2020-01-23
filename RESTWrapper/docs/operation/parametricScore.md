
# Operation `parametricScore` from [`populations.operations`](../package/populations.operations.md)

Calculate a top of parametric scores for a value by its count and multiplier

Configuration examples: [JSON](../operation/parametricScore/example.json), [.ini](../operation/parametricScore/example.ini)

## Inputs


### Named

Name | Type | Description
--- | --- | ---
`values` | `CSV` | Values to group and count
`multipliers` | `CSV` | Value score multipliers

## Outputs


### Named

Name | Type | Description | Generated columns
--- | --- | --- | ---
`scores` | `CSV` | Parametric scores output | `_group` — Generated column with user ID<br>`_score_*` — Generated column with postcode score<br>`_value_*` — Generated column with User ID

## Parameters

### Mandatory

Name | Type | Description | Allowed values
--- | --- | --- | ---
`values.group.column` | `String` | Column for grouping count columns per value column values | 
`values.value.column` | `String` | Column for counting unique values per other column | 
`values.count.column` | `String` | Column to count unique values of other column | 
`multipliers.value.column` | `String` | Column with value multiplier | 
`multipliers.count.column` | `String` | Column to match multiplier value with count value | 

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`top.scores` | `Integer` | How long is the top scores list |  | `1` — By default, generate only the topmost score


[Back to index](../index.md)
