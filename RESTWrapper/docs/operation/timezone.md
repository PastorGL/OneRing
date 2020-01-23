
# Operation `timezone` from [`datetime.operations`](../package/datetime.operations.md)

Take a CSV RDD with a timestamp column (Epoch seconds or milliseconds, ISO of custom format) and explode timestamp components into individual columns. Perform timezone conversion, using source and destination timezones from the parameters or another source columns

Configuration examples: [JSON](../operation/timezone/example.json), [.ini](../operation/timezone/example.ini)

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
`source.timestamp.column` | `String` | Source column with a timestamp | 

### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`source.timestamp.format` | `String` | If set, use this format to parse source timestamp |  | `null` — By default, use ISO formatting for the full source date
`source.timezone.col` | `String` | If set, use source timezone from this column instead of the default |  | `null` — By default, do not read source time zone from input column
`source.timezone.default` | `String` | Source timezone default |  | `GMT` — By default, source time zone is GMT
`destination.timestamp.format` | `String` | If set, use this format to output full date |  | `null` — By default, use ISO formatting for the full destination date
`destination.timezone.col` | `String` | If set, use destination timezone from this column instead of the default |  | `null` — By default, do not read destination time zone from input column
`destination.timezone.default` | `String` | Destination timezone default |  | `GMT` — By default, destination time zone is GMT


[Back to index](../index.md)
