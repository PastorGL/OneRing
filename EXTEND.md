One Ring is designed with extensibility in mind.

### Extend Operations

To extend One Ring built-in set of Operations, you have to implement an [Operation](./Commons/src/main/java/ash/nazg/spark/Operation.java) class according to a set of conventions described in this doc.

First off, you should create a Maven module. Place it at the same level as root pom.xml, and include your module in root project's `&lt;modules>` section. You can freely choose the group and artifact IDs you like.

To make One Ring know your module, include its artifact reference in TaskWrapper's pom.xml `&lt;dependencies>`. To make your module know One Ring, include a reference of artifact 'ash.nazg:Commons' in your module's `&lt;dependencies>` (and its test-jar scope too). For an example, look into [Math's pom.xml](./Math/pom.xml).

Now you can proceed to create an Operation package and describe it.

By convention, Operation package must be named `your.package.name.operations` and have `package-info.java` annotated with `@ash.nazg.config.tdl.Description`. That annotation is required by One Ring to recognize the contents of `your.package.name`. [There's an example](./Math/src/main/java/ash/nazg/math/operations/package-info.java).

Place all your Operations there.

If your module contains a number of Operation that share same Parameter Definitions, names of these parameters must be placed in a `public final class your.package.name.config.ConfigurationParameters` class as `public static final String` constants with same `@Description` annotation each. An Operation can define its own parameters inside its class following the same convention.

Parameter Definitions and their default value constants have names, depending on their purpose:
* 'DS_INPUT_' for input DataStream references,
* 'DS_OUTPUT_' for output DataStream references,
* 'OP_' for the operation's Parameter,
* 'DEF_' for any default value, and the rest of the name should match a corresponding 'OP_' or 'DS_',
* 'GEN_' for any column, generated by this Operation.
 
References to columns of input DataStreams must end with '_COLUMN' suffix, and to column lists with '_COLUMNS'.
 
An Operation in essence is
```java
public abstract class Operation implements Serializable {
    public abstract Map<String, JavaRDDLike> getResult(Map<String, JavaRDDLike> input) throws Exception;
}
```
...but enlightened with a surplus metadata that allows One Ring to flexibly configure it, and to ensure the correctness and consistency of all Operation configurations in the Process chain.

It is up to you, the author, to provide all that metadata. In the lack of any required metadata the build will be prematurely failed by One Ring Guardian, so incomplete class won't fail your extended copy of One Ring CLI on your Process execution time.

At least, you must implement the following methods:
* `abstract public String verb()` that returns a short symbolic name to reference your Operation instance in the config file, annotated with a `@Description`,
* `abstract public TaskDescriptionLanguage.Operation description()` that defines Operation's entire configuration space in TDL2 ([Task Description Language](./Commons/src/main/java/ash/nazg/config/tdl/TaskDescriptionLanguage.java)),
* the `getResult()` that contains an entry point of your business code. It'll be fed with all DataStreams accumulated by the current Process at the moment of your Operation invocation, and should return any DataStreams your Operation should emit.

Also you must override `public void setConfig(OperationConfig config) throws InvalidConfigValueException`, call its `super()` at the beginning, and then read all parameters from the configuration space to your Operation class' fields. If any of the parameters have invalid value, you're obliged to throw an `InvalidConfigValueException` with a descriptive message about the configuration mistake.

You absolutely should create a test case for your Operation. See existing tests for a reference.

There is a plenty of examples to learn by, just look into the source code for Operation's descendants. For your convenience, there's a list of most notable ones:
* [FilterByDateOperation](./DateTime/src/main/java/ash/nazg/datetime/operations/FilterByDateOperation.java) with lots of parameters of different types that have defaults,
* [SplitByDateOperation](./DateTime/src/main/java/ash/nazg/datetime/operations/SplitByDateOperation.java) — its sister Operation generates a lot of output DataStreams with wildcard names,
* [DummyOperation](./Commons/src/main/java/ash/nazg/commons/operations/DummyOperation.java) — this one properly does nothing, just creates aliases for its input DataStreams,
* [SubtractOperation](./Commons/src/main/java/ash/nazg/commons/operations/SubtractOperation.java) can consume and emit both RDDs and PairRDDs as DataStreams,
* [WeightedSumOperation](./Math/src/main/java/ash/nazg/math/operations/WeightedSumOperation.java) generates a lot of columns that either come from input DataStreams or are created anew,
* and the package [Proximity](./Proximity/src/main/java/ash/nazg/proximity/operations/package-info.java) contains Operations that deal with Point and Polygon RDDs in their DataStreams.

### Extend Storage Adapters

To extend One Ring with a custom Storage Adapter, you have to implement a pair of [InputAdapter](./Commons/src/main/java/ash/nazg/storage/InputAdapter.java) and [OutputAdapter](./Commons/src/main/java/ash/nazg/storage/OutputAdapter.java) interfaces. They're fairly straightforward, just see existing Adapter [sources](./TaskWrapper/src/main/java/ash/nazg/storage) for the reference.

For example, you could create an Adapter for Spark 'parquet' files instead of CSV if you have your source data stored that way.

A single restriction exists: you can't set your Adapter as a fallback one, as that is reserved to One Ring Hadoop Adapter.

Hopefully this information is enough to extend One Ring.