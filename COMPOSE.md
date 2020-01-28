There is an utility in the CLI to merge (or compose) two or more One Ring Processes into one.

This action is final and should be performed only when the Process pipeline is fully established, as it mangles most of named entities from the composed tasks.ini and makes it much less readable. Because tasks.ini from different processes may contain Operations and DataStreams with same names, name mangling is necessary to avoid reference clashes. DataStreams may persist they names and carry over the resulting config, though.

Invocation is as follows:
```bash
java -cp ./TaskWrapper/target/one-ring-cli.jar ash.nazg.composer.Composer -X spark.meta -C "/path/to/process1.ini=alias1,/path/to/process2.ini=alias2" -o /path/to/process1and2.ini -M /path/to/mapping.file -v /path/to/variables.file -F
```

`-C` parameter is a list of config files path=alias pairs, separated by a comma. The order of Operations in the resulting config follows the order of this list. Source configs may be in .ini and JSON formats. For JSON format use .json extension.

`-X` lists task prefix(es), if needed. If each is different, specify all as a comma-separated list. If all are the same, specify it only once. If there are no prefixes, omit this switch.

`-V` same as CLI: name=value pairs of Variables for all configs, separated by a newline and encoded to Base64.

`-v` same as CLI: path to variables file, name=value pairs per each line.

`-M` path to DataStream mapping file to pass DataStreams from one Process to other(s).

The syntax of that file is like this:
```plaintext
alias1.name1 alias2.name1
alias1.name2 alias2.name5 alias2.name8
```
This example's first line means that the DataStream 'name1' from the Process 'alias2' will be replaced by DataStream 'name1' from 'alias1' and retain the 'name1' in the resulting config. Second line replaces 'name5' and 'name8' in 'alias2' with 'name2' from 'alias1', and persists 'name2' across the merged config. So the principle is simple: if you want to merge several DataStreams from different Processes, declare which one is the main one first, and then list the DataStreams to be replaced.

`-o` path to the composed output config file, by default, in .ini format. For JSON output use .json extension.

`-F` perform full compose, if this switch is given. Resulting `task.tee.outputs` will only contain same outputs as the last config in the chain, otherwise it'll contain outputs from all merged tasks.
