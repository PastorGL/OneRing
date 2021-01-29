There is an utility in the CLI to merge (or Compose) two or more One Ring Process Templates into one larger Template.

### One Ring Composer

This action is final and should be performed only when the pipeline of all participating Processes is fully established, as it mangles most of named entities from the composed `tasks.ini`s and emits a much less readable config.
 
Name mangling is necessary because `tasks.ini` from different Processes may contain Operations and DataStreams with same names, and we want to avoid reference clashes. DataStreams may persist they names and carry over the resulting config, though.

Command line invocation of Composer is as follows (also available via [REST](REST.md)):
```bash
java -cp ./TaskWrapper/target/one-ring-cli.jar ash.nazg.composer.Composer -X spark.meta -C "/path/to/process1.ini=alias1,/path/to/process2.ini=alias2" -o /path/to/process1and2.ini -M /path/to/mapping.file -v /path/to/variables.file -F
```

`-C` parameter is a list of config files `path=alias` pairs, separated by a comma. Order of Operations in the resulting config follows the order of this list. Source configs may be in `.ini` and JSON formats, and even freely mixed, just use `.json` extension for JSON configs.

`-X` lists task prefix(es), if they're present. If each is different, specify them all in a comma-separated list. If all are the same, specify it only once. If there are no prefixes, just omit this switch.

`-V` same as CLI: name=value pairs of Variables for all configs, separated by a newline and encoded to Base64.

`-v` same as CLI: path to variables file, name=value pairs per each line.

`-M` path to a DataStream mapping file used to pass DataStreams from one Process to other(s).

The syntax of that file is like this:
```plaintext
alias1.name1 alias2.name1
alias1.name2 alias2.name5 alias3.name8
```
This example's first line means that the DataStream `name1` from the Process `alias2` will be replaced by DataStream `name1` from `alias1` and retain the `name1` in the resulting config. Second line replaces `name5` in `alias2` and `name8` in `alias3` with `name2` from `alias1`, and persists `name2` across the merged config. So the principle is simple: if you want to merge several DataStreams from different Processes, place the main one first, and then list the DataStreams to be replaced.

`-o` path to the composed output config file, in `.ini` format by default. For JSON output use `.json` extension.

`-F` perform a Full Compose, if this switch is given. Resulting `task.tee.outputs` will only contain same outputs as the very last config in the chain, otherwise it'll contain outputs from all merged tasks.
