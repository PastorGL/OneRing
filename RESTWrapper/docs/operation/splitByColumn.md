
# Operation `splitByColumn` from [`columnar.operations`](../package/columnar.operations.md)

Take a CSV RDD and split it into several RDDs by selected column value. Output 'template' name is treated as a template for a set of generated outputs that can reference to encountered unique values of a selected column

Configuration examples: [JSON](../operation/splitByColumn/example.json), [.ini](../operation/splitByColumn/example.ini)

## Inputs

### Positional

Allowed types are `CSV`



## Outputs


### Named

Name | Type | Description | Generated columns
--- | --- | --- | ---
`template` | `Passthru` | Template output with a wildcard part, i.e. output_* | 
`distinct_splits` | `Fixed` | Optional output that contains all the distinct splits occurred on the input data, in the form of names of the generated inputs | 

## Parameters

### Mandatory

Name | Type | Description | Allowed values
--- | --- | --- | ---
`split.column` | `String` | If set, split by date of month column value | 
`split.template` | `String` | Template for output names wildcard part in form of 'prefix{split.column}suffix' | 



[Back to index](../index.md)
