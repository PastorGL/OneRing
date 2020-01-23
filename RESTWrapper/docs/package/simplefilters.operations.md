
# Package `simplefilters.operations`

Simple filters of CSV row data by a specified column value

Verb | Description | Examples
--- | --- | ---
[`percentileFilter`](../operation/percentileFilter.md) | In a CSV RDD, take a column to filter all rows that have a Double value in this column that lies outside of the set percentile range | [JSON](../operation/percentileFilter/example.json) [.ini](../operation/percentileFilter/example.ini)
[`exactMatch`](../operation/exactMatch.md) | This operation is a filter that only passes the rows that have an exact match with a specific set of allowed values in a given column | [JSON](../operation/exactMatch/example.json) [.ini](../operation/exactMatch/example.ini)
[`listMatch`](../operation/listMatch.md) | This operation is a filter that only passes the RDD rows that have an exact match with a specific set of allowed values in a given column sourced from another RDD's column | [JSON](../operation/listMatch/example.json) [.ini](../operation/listMatch/example.ini)
[`rangeFilter`](../operation/rangeFilter.md) | In a CSV RDD, take a column to filter all rows that have a Double value in this column that lies outside of the set absolute range | [JSON](../operation/rangeFilter/example.json) [.ini](../operation/rangeFilter/example.ini)


[Back to index](../index.md)
