There are two supported ways to execute One Ring Tasks.

### Local Execution

After you've composed the Process configuration, you definitely should test it locally with a small but reasonably representative sample of your source data. It is much easier to debug a new Process locally rather than on the real cluster.

You'll need to build the local artifact, as described in the [build how-to](BUILD.md).

```bash
java -jar ./RestWrapper/target/one-ring-cli.jar -c /path/to/tasks.ini -l -m 6g -x spark.meta
```

`-c` sets the path to tasks.ini. It should be local in the local mode, but any Adapter-supported Storage path is valid.

`-l` means the local execution mode ('local[*]', to be precise).

`-m` sets the amount of Spark memory, like '4g' or '512m'.

`-x` sets the current task prefix, if needed. If you're planning to pass tasks.ini to your cluster via Spark context, you should use prefixed tasks.ini locally too.

Also you should pass all the input and output paths via Variables, to ease transition between your local file system storage and the cluster's storage.

For example, we assume your Process has two source datasets and one result, stored under paths specified by `SOURCE_SIGNALS`, `SOURCE_POIS` and `OUTPUT_SCORES` Variables.

Then you have two ways to pass them to One Ring CLI, but first prepare a newline-separated list of name=value pairs, and then
1. Encode them as Base64 string and pass with `-V` command line key
1. Place them the into a file (or other Adapter-supported Storage) and pass with `-v` command line key

If both keys are specified, `-V` has higher priority, and `-v` will be ignored.

For example,

```bash
cat > /path/to/variables.ini
SIGNALS_PATH=file:/path/to/signals
POIS_PATH=file:/path/to/pois
OUTPUT_PATH=file:/path/to/output
^D

base64 -w0 < /path/to/variables.ini
U0lHTkFMU19QQVRIPWZpbGU6L3BhdGgvdG8vc2lnbmFscwpQT0lTX1BBVEg9ZmlsZTovcGF0aC90by9wb2lzCk9VVFBVVF9QQVRIPWZpbGU6L3BhdGgvdG8vb3V0cHV0Cg==

java -jar ./RestWrapper/target/one-ring-cli.jar -c /path/to/tasks.ini -l -m 6g -V U0lHTkFMU19QQVRIPWZpbGU6L3BhdGgvdG8vc2lnbmFscwpQT0lTX1BBVEg9ZmlsZTovcGF0aC90by9wb2lzCk9VVFBVVF9QQVRIPWZpbGU6L3BhdGgvdG8vb3V0cHV0Cg==

java -jar ./RestWrapper/target/one-ring-cli.jar -c /path/to/tasks.ini -l -m 6g -v /path/to/variables.ini
```

You'll see a lot of Spark output, as well as the dump of your Task. If everything is succesfull, you'll see no exceptions in that output. If not, read exception messages carefully and fix your tasks.ini and/or check the source data files.

### Execution on a Compute Cluster

One Ring officially supports execution on EMR Spark clusters via [TeamCity](https://www.jetbrains.com/teamcity/) continuous deployment builds, but can be relatively easy adapted for other clouds, continuous integration services, and automation scenarios.

We assume you're already familiar with AWS and have an utility VM instance in that cloud. You may have or may not have to set up TeamCity or some other CI service of your preference on that instance. We like it automated.

First off, you need to set up some additional environment, starting with latest version of PowerShell (at the very least, version 6 is required) and AWS Tools for PowerShell. Please follow the [official AWS documentation](https://aws.amazon.com/powershell/), and register your AWS API access key with them.

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

Don't forget to customize VCS roots, and always use your own private copy of `one-ring-emr-settings`, because there'll go most sensitive data. In the case of other CI service, you may want to extract build steps from TC's XML. Their structure is pretty straightforward, just look into them.

The environment set up by build configurations is a directory, where the contents of `one-ring-emr` is augmented with addition of `one-ring-emr-settings` and One Ring artifacts `one-ring-cli.jar` and `one-ring-dist.jar`, so it looks like this (you also may use symlinks to place them into):

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
 set-params.ps1
 cluster.template
```

You place your tasks.ini into `/settings` subdirectory alongside other .ini files. Also, you must fill in all required values in all .ini files inside this directory, that conform to your AWS account environment.

We usually put presets for all our Processes in different branches of our copy if the `one-ring-emr-settings` repo, and just switch the branch of that repo for each Process' build configuration.

Build steps are executed in the following order:
1. Ask for Variables on TC UI
1. `preset-params.ps1`
1. `set-params.ps1`
1. `create-cluster.ps1`
1. Encode Variables to Base64
1. `run-job.ps1`
1. `remove-cluster.ps1`

There is an ability to override any line of any .ini file from TC UI by supplying a custom build parameter with name formed as 'file.ini' + '.' + 'parameter.name', which gives you another level of build parametrization flexibility.

It is possible to execute every PowerShell script in the interactive mode and manually copy-paste their output variables between steps. It may be helpful to familiarize yourself with that stuff before going fully automated.

We also usually go on the higher level of automation and enqueue TC builds with their REST API.

Anyways, closely watch your CloudFormation, EMR and EC2 consoles for at least few first tries. There may be insufficient access rights, and a lot of other issues, but we assume you already are experienced with AWS.