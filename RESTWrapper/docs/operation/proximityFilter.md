
# Operation `proximityFilter` from [`proximity.operations`](../package/proximity.operations.md)

Takes a Point RDD and POI Point RDD and generates a Point RDD consisting of all points that are within the range of POIs

Configuration examples: [JSON](../operation/proximityFilter/example.json), [.ini](../operation/proximityFilter/example.ini)

## Inputs


### Named

Name | Type | Description
--- | --- | ---
`signals` | `Point` | Source Point RDD
`pois` | `Point` | Source POI Point RDD with _radius attribute set

## Outputs


### Named

Name | Type | Description | Generated columns
--- | --- | --- | ---
`signals` | `Point` | Output Point RDD | 


[Back to index](../index.md)
