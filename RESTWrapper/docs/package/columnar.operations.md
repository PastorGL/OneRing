
# Package `columnar.operations`

Arrange columns of CSV RDDs and generate new columns, either plain text or cryptographic digest of other columns

Verb | Description | Examples
--- | --- | ---
[`customColumn`](../operation/customColumn.md) | Insert a custom column in the input CSV at a given index | [JSON](../operation/customColumn/example.json) [.ini](../operation/customColumn/example.ini)
[`splitByColumn`](../operation/splitByColumn.md) | Take a CSV RDD and split it into several RDDs by selected column value. Output 'template' name is treated as a template for a set of generated outputs that can reference to encountered unique values of a selected column | [JSON](../operation/splitByColumn/example.json) [.ini](../operation/splitByColumn/example.ini)
[`arrangeColumns`](../operation/arrangeColumns.md) | This operation rearranges the order of input CSV columns, optionally omitting unneeded in the output | [JSON](../operation/arrangeColumns/example.json) [.ini](../operation/arrangeColumns/example.ini)
[`digest`](../operation/digest.md) | This operation calculates cryptographic digest(s) for given input column(s), by any algorithm provided by Java platform. Possible generated column list is dynamic, while each column name follows the convention of _PROVIDER_ALGORITHM_source.column. Default PROVIDER is SUN, and ALGORITHMs are MD2, MD5, SHA, SHA-224, SHA-256, SHA-384 and SHA-512 | [JSON](../operation/digest/example.json) [.ini](../operation/digest/example.ini)


[Back to index](../index.md)
