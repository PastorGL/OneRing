
# Package `datetime.operations`

Read timestamp columns of CSV RDDs, explode into individual fields, convert time zone and filter or split RDDs by timestamp field values

Verb | Description | Examples
--- | --- | ---
[`filterByDate`](../operation/filterByDate.md) | Filter a CSV RDD by exploded timestamp field values (year, month, day of month, day of week) and optionally full date and/or time of day (hours and minutes) range. Multiple filter values are supported, and all fields are optional | [JSON](../operation/filterByDate/example.json) [.ini](../operation/filterByDate/example.ini)
[`timezone`](../operation/timezone.md) | Take a CSV RDD with a timestamp column (Epoch seconds or milliseconds, ISO of custom format) and explode timestamp components into individual columns. Perform timezone conversion, using source and destination timezones from the parameters or another source columns | [JSON](../operation/timezone/example.json) [.ini](../operation/timezone/example.ini)
[`splitByDate`](../operation/splitByDate.md) | Take a CSV RDD that contains exploded timestamp columns and split it into several RDDs by selected columns' values. Output 'template' name is treated as a template for a set of generated outputs that can reference to encountered unique values of selected columns | [JSON](../operation/splitByDate/example.json) [.ini](../operation/splitByDate/example.ini)


[Back to index](../index.md)
