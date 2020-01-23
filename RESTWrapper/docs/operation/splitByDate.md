
# Operation `splitByDate` from [`datetime.operations`](../package/datetime.operations.md)

Take a CSV RDD that contains exploded timestamp columns and split it into several RDDs by selected columns' values. Output 'template' name is treated as a template for a set of generated outputs that can reference to encountered unique values of selected columns

Configuration examples: [JSON](../operation/splitByDate/example.json), [.ini](../operation/splitByDate/example.ini)

## Inputs

### Positional

Allowed types are `CSV`



## Outputs


### Named

Name | Type | Description | Generated columns
--- | --- | --- | ---
`template` | `CSV` | Template output with a wildcard part, i.e. output_* | 
`distinct_splits` | `CSV` | Optional output that contains all the distinct splits occurred on the input data, in the form of names of the generated inputs | 

## Parameters

### Mandatory

Name | Type | Description | Allowed values
--- | --- | --- | ---
`split.template` | `String` | Template for output names wildcard part in form of {input.column1}/{input.column2} | 

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`date.column` | `String` | If set, split by date of month column value |  | `null` — By default do not explode date of month
`year.column` | `String` | If set, split by year column value |  | `null` — By default do not explode year
`dow.column` | `String` | If set, split by day of week column value |  | `null` — By default do not explode day of week
`month.column` | `String` | If set, split by month column value |  | `null` — By default do not explode month
`hour.column` | `String` | If set, split by hour column value |  | `null` — By default do not explode hour
`minute.column` | `String` | If set, split by minute column value |  | `null` — By default do not explode minute


[Back to index](../index.md)
