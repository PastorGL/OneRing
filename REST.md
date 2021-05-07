One Ring has a standalone REST-ish service that provides documentation and can run Spark tasks in local and TeamCity integrated mode.

### Configuration

One Ring REST is configurable via command line and configuration file.
```bash
java -jar ./REST/target/one-ring-rest.jar -h
usage: One Ring
-c,--configPath <arg>   Path to configuration file
-e,--iface <arg>        Server interface. By default, 0.0.0.0 (all interfaces)
-p,--port <arg>         Server port. By default, 9996
```

Default configuration file path is `/etc/one-ring/one-ring-rest.conf` and the settings are pretty much straightforward:
```properties
tc.instance=http://<host>:<port>

tc.user=<user>
tc.password=<password>

tc.buildType=<build_configuration_ID>

local.driver.memory=<#G or ####M>

aws.profile=default

s3.bucket=<S3_bucket>
```

If configuration isn't available, One Ring REST will print help and exit.

### REST-ish Endpoints
Endpoints are grouped by their purpose.

#### General
For the service status itself.

##### GET /application.wadl[?detail=true]
Get the description of all endpoints as a WADL document, optionally detailed.

##### GET /alive
Returns just string 'Ok' if service is up and running.

#### Documentation
All built-in documentation in mostly Markdown format is served under the path `/docs`. It is generated at compile time and describes everything that is available at that moment.

##### GET /docs/index.md
Entry point to docs.

##### GET /docs/package/{package.name}.md
Index of all Package `{package.name}` Operations and Adapters.

##### GET /docs/operation/{operationName}.md
Description of `{operationName}`, generated from Operation's metadata.

##### GET /docs/operation/{operationName}/example.ini
Auto-generated example config for Operation `{operationName}` in `.ini` format.

##### GET /docs/operation/{operationName}/example.json
Auto-generated example config for Operation `{operationName}` in JSON format.

##### GET /docs/adapter/{AdapterName}.md
Description of Adapter `{AdapterName}`, generated from Adapter's metadata.

#### Package
These endpoints dynamically represent everything that is available to local instance of One Ring at run time, and return JSON object.

##### GET /package
List of all Packages.

##### GET /package/{package.name}/listOperation
Lists all Operations in Package `{package.name}`.

##### GET /package/{package.name}/operation
Returns full TDL descriptions of all operations in Package `{package.name}`.

##### GET /package/{package.name}/listAdapter
Lists all Adapters in Package `{package.name}`.

#### Task
These endpoints perform `tasks.ini` or JSON Process validation and execution.

##### POST /task/validate.json
Consumes JSON Process definition and performs its formal validation. If valid, returns same task converted to `.ini`, or HTTP Error 400 otherwise.

##### POST /task/validate.ini[?prefix=spark.prefix]
Consumes `tasks.ini` with optional task prefix and performs its formal validation. If valid, returns same task converted to JSON, or HTTP Error 400 otherwise.

##### POST /task/run/local.json[?variables=Base64]
Consumes JSON Process definition and optionally Variables encoded as Base64, and enqueues it into local execution queue. Returns local task ID as a simple string.

##### POST /task/run/local.ini[?prefix=spark.prefix&variables=Base64]
Consumes `tasks.ini` with optional task prefix and Variables encoded as Base64, and enqueues it into local execution queue. Returns local task ID as a simple string.

##### POST /task/run/remote/tc.json[?variables=Base64]
Consumes JSON Process definition and optionally Variables encoded as Base64, and passes it to TeamCity. Returns TeamCity task ID as a simple string.

##### POST /task/run/remote/tc.ini[?prefix=spark.prefix&variables=Base64]
Consumes `tasks.ini` with optional task prefix and Variables encoded as Base64, and passes it to TeamCity. Returns TeamCity task ID as a simple string.

##### GET /task/status?taskId=taskID
Returns current status of a Task with specified local or TeamCity ID `taskID`. Might be one of `NOT_FOUND`, `QUEUED`, `RUNNING`, `SUCCESS`, or `FAILURE`.
