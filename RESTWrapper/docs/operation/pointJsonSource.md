
# Operation `pointJsonSource` from [`spatial.operations`](../package/spatial.operations.md)

Take GeoJSON fragment file and produce a Point RDD

Configuration examples: [JSON](../operation/pointJsonSource/example.json), [.ini](../operation/pointJsonSource/example.ini)

## Inputs

### Positional

Allowed types are `Plain`



## Outputs

### Positional

Resulting types are `Point`


## Parameters


### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`radius.default` | `Double` | If set, generated Points will have this value in the _radius parameter |  | `null` — By default, don't add _radius attribute to the point


[Back to index](../index.md)
