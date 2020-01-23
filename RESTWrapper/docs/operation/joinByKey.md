
# Operation `joinByKey` from [`commons.operations`](../package/commons.operations.md)

Takes two PairRDDs and joins them by key. In the terms of joins, first PairRDD is the 'left', second is the 'right'. Missing rows from either PairRDDs are settable and must have a proper column count.

Configuration examples: [JSON](../operation/joinByKey/example.json), [.ini](../operation/joinByKey/example.ini)

## Inputs

### Positional

Allowed types are `KeyValue`



## Outputs

### Positional

Resulting types are `CSV`


## Parameters


### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`join` | `Join` | Type of the join |  `INNER` — Inner join<br> `LEFT` — Left outer join<br> `RIGHT` — Right outer join<br> `OUTER` — Full outer join | `INNER` — By default, perform an inner join
`default.left` | `String` | Default value for the missing left row |  | `null` — By default, any missing row from left is an empty string
`default.right` | `String` | Default value for the missing right row |  | `null` — By default, any missing row from right is an empty string


[Back to index](../index.md)
