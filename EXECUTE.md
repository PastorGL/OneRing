There are two supported ways to execute One Ring Tasks.

### Local Execution

After you've composed the Process configuration, you definitely should test it locally with a small but reasonably representative sample of your source data. It is much easier to debug a new Process locally rather than on the real cluster.

After you've built the local artifact, as described in the [build how-to](BUILD.md), call it with -h (--help):
```bash
java -jar ./CLI/target/one-ring-cli.jar -h
usage: One Ring CLI utility
  -h,--help                    Print a list of command line options and exit
  -c,--config <arg>            Config file (JSON or .ini format)
  -x,--task <arg>              Task prefix in the config file
  -V,--variables <arg>         name=value pairs of substitution variables for the Spark config encoded as Base64
  -v,--variablesFile <arg>     Path to variables file, name=value pairs per each line
  -l,--local                   Run in local mode (its options have no effect otherwise)
  -m,--driverMemory <arg>      Driver memory for local mode, by default Spark uses 1g
  -u,--sparkUI                 Enable Spark UI for local mode, by default it is disabled
  -L,--localCores <arg>        Set cores # for local mode, by default * -- all cores
  -S,--wrapperStorePath <arg>  Path where to store a list of wrapped wildcards outputs
  -i,--input <arg>             Override for default input path
  -o,--output <arg>            Override for default output path
  -D,--metricsStorePath <arg>  Path where to store data stream metrics, if needed
```

`-c` sets the path to `tasks.ini` or JSON config file.

`-l` means the local execution mode of Spark context. Following switches have effect only in that mode:
* `-L #` sets the number of cores to use. By default, it'll use all available cores (`local[*]`).
* `-m` sets the amount of Spark memory, like `4g` or `512m`. Spark's current default is 1g.
* `-u` to start Spark UI for debugging purposes.

`-i` overrides the default input path (`ds.input.path`) for the Process.
`-o` overrides the default output path (`ds.output.path`) for the Process.

`-x` sets the current task prefix, if needed. If you're planning to pass `tasks.ini` to your cluster via Spark context, you should use prefixed `tasks.ini` locally too. If your configuration file contains a number of different tasks, set the prefix to select one of them.

`-S` to interface with One Ring Dist, especially if your config has wildcard outputs, as discussed [in a separate doc](DIST.md).

`-D` to output data stream metrics, described [in a separate doc](MONITOR.md).

`-V` or `-v` (only if previous switch wasn't specified) to pass Variables to the config.

For example, let us assume your Process has two source datasets and one resulting, stored under paths specified by `SOURCE_SIGNALS`, `SOURCE_POIS` and `OUTPUT_SCORES` Variables. Just prepare a newline-separated list of name=value pairs of them, and then you have two ways to pass them to One Ring CLI.

Encode as Base64 string and pass with `-V` command line key:
```bash
cat > /path/to/variables.ini
SIGNALS_PATH=file:/path/to/signals
POIS_PATH=file:/path/to/pois
OUTPUT_PATH=file:/path/to/output
^D

base64 -w0 < /path/to/variables.ini
U0lHTkFMU19QQVRIPWZpbGU6L3BhdGgvdG8vc2lnbmFscwpQT0lTX1BBVEg9ZmlsZTovcGF0aC90by9wb2lzCk9VVFBVVF9QQVRIPWZpbGU6L3BhdGgvdG8vb3V0cHV0Cg==

java -jar ./CLI/target/one-ring-cli.jar -c /path/to/tasks.ini -l -m 6g -V U0lHTkFMU19QQVRIPWZpbGU6L3BhdGgvdG8vc2lnbmFscwpQT0lTX1BBVEg9ZmlsZTovcGF0aC90by9wb2lzCk9VVFBVVF9QQVRIPWZpbGU6L3BhdGgvdG8vb3V0cHV0Cg==
```

Or place into a file accessible to Hadoop and pass its path with `-v` command line key^
```bash
java -jar ./CLI/target/one-ring-cli.jar -c /path/to/tasks.ini -l -m 6g -v /path/to/variables.ini
```

You'll see a lot of Spark output, as well as the dump of your Task. If everything is successful, you'll see no exceptions in that output. If not, read exception messages carefully and fix your `tasks.ini` and/or check the source data files.

### Execution on a Compute Cluster

One Ring officially supports the execution on EMR Spark clusters via [TeamCity](https://www.jetbrains.com/teamcity/) continuous deployment builds, but it could be relatively easy adapted for other clouds, continuous integration services, and automation scenarios.

We assume you're already familiar with AWS and have an utility EC2 instance in that cloud. You may have or may not have to set up TeamCity or some other CI service of your preference on that instance. We like it automated though.

First off, you need to set up some additional environment on the utility instance, starting with latest version of PowerShell (at the very least, version 6 is required) and AWS Tools for PowerShell. Please follow the [official AWS documentation](https://aws.amazon.com/powershell/), and register your AWS API access key with these Tools.

Get the scripts and CloudFormation deployment template:
```powershell
git clone https://github.com/PastorGL/one-ring-emr.git
```

Also get a template of configuration files:
```powershell
git clone https://github.com/PastorGL/one-ring-emr-settings.git
```

And there are TC configs you may import into your TC:
```powershell
git clone https://github.com/PastorGL/one-ring-tc-builds.git
```

Don't forget to customize VCS roots, and always use your own private copy of `one-ring-emr-settings`, because there'll go most sensitive data. In the case of other CI service, you may extract build steps from TC's XMLs. Their structure is pretty straightforward, just dig into them.

The environment set up by build configurations is a directory, where the contents of `one-ring-emr` is augmented with addition of `one-ring-emr-settings` and One Ring artifacts `one-ring-cli.jar` and `one-ring-dist.jar`, so it looks like this (you also may use symlinks to place them into manually):
```
/common
/presets
/settings
 one-ring-cli.jar
 one-ring-dist.jar
 create-cluster.ps1
 list-jobs.ps1
 preset-params.ps1
 remove-cluster.ps1
 run-job.ps1
 cluster.template
```

Your Process definition, in either `tasks.ini` or JSON form should be placed onto any storage that is accessible by One Ring from the cluster via its Adapters, like S3 or HFDS or whatever you like. (Otherwise, it could be put directly to Spark configuration, using `spark.` namespace with an additional prefix of your pick aka the task prefix. But we don't support that way with our scripts.)

Before all, you must fill in all required values in all `.ini` files inside `settings` directory, that conform to your AWS account environment.

We usually put presets for all our Processes in different branches of our copy if the `one-ring-emr-settings` repo, and just switch to the required branch of that repo for each Process' build configuration.

Build steps are executed in the following order:
1. Ask for Variables on TC UI
1. `preset-params.ps1`
1. `create-cluster.ps1`
1. Encode Variables to Base64
1. `run-job.ps1`
1. `remove-cluster.ps1`

Let us explain what each step does.

TC has a feature to define 'build configuration parameters', and provides a UI to set them at build execution time (along with corresponding REST methods). We use these build parameters to set Variables in our Process template, and ask the user for their values. Also, we ask for any additional parameters specific for the environment, such as for a preset of cluster size.

At the next step we select one of four cluster size presets from  `/preset` directory (S, M, L, XL `.ini` files, or default Z to skip) if it was selected on the previous step, and place its contents into build parameters.

`preset-params.ps1` has an ability to override any line of any existing .ini file from `/settings` subdirectory by replacing it with a custom build parameter named as 'filename.ini' + '.' + 'parameter.name', which gives you another level of build parametrization flexibility. This script overwrites `.ini` files with those parameters, so all further scripts receive augmented configurations. As this script interacts with TeamCity, it must receive parameters for switches `-tcBuild` (your build configuration ID), `-tsAddress` (TC base URI, not REST), `-tcUser` and `-tcPassword` (those should be tokens created by TC for a build config).

At the next step we create a Spark cluster in EMR with `create-cluster.ps1` by deploying CloudFormation template augmented with all parameters gathered to this moment, and parameters from `/settings/create.ini` and all `.ini` for cluster software components from `settings`. Its switches accept paths to many `.ini` files from `settings`.

This script outputs three important environment variables needed to be saved for further use (as build parameters or written down):
* `deployment.uniq` is a portion of CloudFormation deployment ID,
* `deployment.cluster.id` is an EMR cluster ID,
* `deployment.master.address` is an URI to cluster's master node, where Livy REST interface listens.

Then we encode Variables with Base64, just as we did in Local mode.

At this moment everything is ready to run the Process on the cluster. `run-job.ini` sets up all required environment (from the per-component `.ini` files from `/settings`), calls Livy REST method on the cluster, and waits for the completion. Its own parameters are set by `/settings/run.ini`.

Path to `tasks.ini` is passed via `-tasksFile` switch, and task prefix via `-tasksPrefix`. If your config contains more than one Task, all their prefixes must be specified as comma separated list, and will be executed in the order of definition. Variables are passed with `-params` (in Base64) or `-paramsFile` (again, placed anywhere accessible from the cluster via Adapters).

The URI to Livy must be passed with `-clusterUri` and cluster's ID with `-clusterId` switches.

Even if any of previous steps fail, `remove-cluster.ps1` should be called the last. This script does the cleanup, and is controlled by `/settings/remove.ini`. It receives deployment ID via `-uniq` switch.

All scripts that deal with the cluster also share parameters from `/settings/aws.ini` global file.

All scripts by default ask for confirmations for all potentially destructive actions, and have switch `-autoConfirm` to control that. Also, they all will interactively ask for any missing `.ini` parameters, or fail if invoked as non-interactive. It may be helpful to familiarize yourself with interactive mode before going fully automated.

We also usually go on the higher level of automation and enqueue TC builds with their REST API.

Anyways, closely watch your CloudFormation, EMR and EC2 consoles for at least few first tries. There may be insufficient access rights, and a lot of other issues, but we assume you are already experienced with AWS and EMR, if you are here.
