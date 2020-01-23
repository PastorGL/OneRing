
# Operation `areaCovers` from [`proximity.operations`](../package/proximity.operations.md)

Takes a Point RDD and Polygon RDD and generates a Point RDD consisting of all points that are contained inside the polygons

Configuration examples: [JSON](../operation/areaCovers/example.json), [.ini](../operation/areaCovers/example.ini)

## Inputs


### Named

Name | Type | Description
--- | --- | ---
`signals` | `Point` | Source Point RDD
`geometries` | `Polygon` | Source Polygon RDD

## Outputs


### Named

Name | Type | Description | Generated columns
--- | --- | --- | ---
`signals` | `Point` | Output Point RDD | 


[Back to index](../index.md)
