
# Operation `populationFilter` from [`populations.operations`](../package/populations.operations.md)

Heuristic that classifies and splits the user tracks into a defined set of track types

Configuration examples: [JSON](../operation/populationFilter/example.json), [.ini](../operation/populationFilter/example.ini)

## Inputs


### Named

Name | Type | Description
--- | --- | ---
`signals` | `CSV` | Source user signals

## Outputs


### Named

Name | Type | Description | Generated columns
--- | --- | --- | ---
`signals` | `CSV` | Filtered user signals | 

## Parameters

### Mandatory

Name | Type | Description | Allowed values
--- | --- | --- | ---
`dates` | `String[]` | Dates to filter users by, YYYY-MM-DD format | 
`min.days` | `Integer` | Minimal number of occurred dates for a user | 
`min.signals` | `Integer` | Minimal count of signals for a user | 
`max.signals` | `Integer` | Maximal count of signals for a user | 
`signals.year.column` | `String` | Column with the year | 
`signals.month.column` | `String` | Column with the month | 
`signals.day.column` | `String` | Column with the day of the month | 
`signals.userid.column` | `String` | Column with the user ID | 
`signals.silos.column` | `String` | Column with the silo ID | 



[Back to index](../index.md)
