
# Operation `filterByDate` from [`datetime.operations`](../package/datetime.operations.md)

Filter a CSV RDD by exploded timestamp field values (year, month, day of month, day of week) and optionally full date and/or time of day (hours and minutes) range. Multiple filter values are supported, and all fields are optional

Configuration examples: [JSON](../operation/filterByDate/example.json), [.ini](../operation/filterByDate/example.ini)

## Inputs

### Positional

Allowed types are `CSV`



## Outputs

### Positional

Resulting types are `Passthru`


## Parameters


### Optional

Name | Type | Description | Allowed values | Default value
--- | --- | --- | --- | ---
`date.column` | `String` | Column with date of month |  | `null` — By default do not filter by date of month
`year.column` | `String` | Column with year |  | `null` — By default do not filter by year
`dow.column` | `String` | Column with day of week |  | `null` — By default do not filter by day of week
`month.column` | `String` | Column with month |  | `null` — By default do not filter by month
`hour.column` | `String` | Column with hour |  | `null` — By default do not filter by hour
`minute.column` | `String` | Column with minute |  | `null` — By default do not filter by minute
`start` | `String` | Start of the range filter |  | `null` — By default do not filter by date range start
`end` | `String` | End of the range filter |  | `null` — By default do not filter by date range end
`date.value` | `String[]` | List of date filter values |  | `null` — By default do not filter by date of month
`year.value` | `String[]` | List of year filter values |  | `null` — By default do not filter by year
`dow.value` | `String[]` | List of day of week filter values |  | `null` — By default do not filter by day of week
`month.value` | `String[]` | List of month filter values |  | `null` — By default do not filter by month
`hhmm.start` | `Integer` | Starting time of day, exclusive, in HHMM format, in the range of 0000 to 2359 |  | `null` — By default do not filter by starting time of day
`hhmm.end` | `Integer` | Ending time of day, inclusive, in HHMM format, in the range of 0000 to 2359 |  | `null` — By default do not filter by ending time of day


[Back to index](../index.md)
