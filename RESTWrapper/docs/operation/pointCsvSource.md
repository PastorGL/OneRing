
# Operation `pointCsvSource` from [`spatial.operations`](../package/spatial.operations.md)

Take a CSV file and produce a Polygon RDD

Configuration examples: [JSON](../operation/pointCsvSource/example.json), [.ini](../operation/pointCsvSource/example.ini)

## Inputs

### Positional

Allowed types are `CSV`



## Outputs

### Positional

Resulting types are `Point`


## Parameters

### Mandatory

Name | Type | Description | Allowed values
--- | --- | --- | ---
`lat.column` | `String` | Point latitude column | 
`lon.column` | `String` | Point longitude column | 

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`radius.default` | `Double` | If set, generated Points will have this value in the _radius parameter |  | `null` — By default, don't set Point _radius attribute
`radius.column` | `String` | If set, generated Points will take their _radius parameter from the specified column |  | `null` — By default, don't set Point _radius attribute


[Back to index](../index.md)
