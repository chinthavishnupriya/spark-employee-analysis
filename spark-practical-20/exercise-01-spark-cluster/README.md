# Exercise 1 — Understanding the Spark Cluster

## Practical Execution

### Environment

- Apache Spark: 3.5.3
- Scala: 2.12.18
- Java: 17.0.19
- Master: local[*]
- Deploy Mode: client

## 1. Spark Master

Command:

sc.master

Output:

local[*]

## 2. Application Name

Command:

sc.appName

Output:

Spark shell

## 3. Application ID

Command:

sc.applicationId

Output:

local-1787724685374

## 4. Spark Configuration

Command:

sc.getConf.getAll.foreach(println)

Important values observed:

spark.master = local[*]
spark.app.name = Spark shell
spark.submit.deployMode = client
spark.executor.id = driver
spark.driver.host = 10.255.255.254
spark.driver.port = 46449

## 5. Executor Memory Status

Command:

sc.getExecutorMemoryStatus

Output:

Map(10.255.255.254:42327 -> (455501414,455501414))

## 6. Spark Web UI

Command:

sc.uiWebUrl

Output:

Some(http://10.255.255.254:4040)

The Spark Web UI was opened successfully at port 4040.

The Spark Jobs page was verified in the browser.

## Practical Result

The Spark application was successfully started in local mode.

The following were practically verified:

- Spark master
- Application name
- Application ID
- Spark configuration
- Executor memory status
- Spark Web UI
