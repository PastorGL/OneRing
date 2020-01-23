
# Operation `digest` from [`columnar.operations`](../package/columnar.operations.md)

This operation calculates cryptographic digest(s) for given input column(s), by any algorithm provided by Java platform. Possible generated column list is dynamic, while each column name follows the convention of _PROVIDER_ALGORITHM_source.column. Default PROVIDER is SUN, and ALGORITHMs are MD2, MD5, SHA, SHA-224, SHA-256, SHA-384 and SHA-512

Configuration examples: [JSON](../operation/digest/example.json), [.ini](../operation/digest/example.ini)

## Inputs

### Positional

Allowed types are `CSV`



## Outputs

### Positional

Resulting types are `CSV`



[Back to index](../index.md)
