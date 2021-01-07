# PROUD: PaRallel OUtlier Detection for streams 

**PROUD** is an open-source high-throughput distributed outlier detection engine for intense data streams that is implemented in Scala on top of the [Apache Flink](https://flink.apache.org/) framework.

It supports distance-based outlier detection algorithms both for single-query and multi-query cases.

## Flink job

PROUD can be run within a Flink standalone cluster after the source code has been compiled. Compiling the code with: 

    sbt clean assembly

creates the PROUD-assembly-%%VERSION%%.jar file on the target directory.

To run the job on a Flink standalone cluster the command:

    bin/flink run -d PROUD-assembly-%%VERSION%%.jar --space single --algorithm pmcod --W 10000 --S 500 --k 50 --R 0.35 --dataset STK --partitioning grid

needs to be executed on the JobManager machine (changing the parameters based on the job). The level of **parallelism** of the job is by default set to 16 due to restrictions on the partitioning technique.

The job's parameters are the following:

 - **space**: Represents the query space. Possible values are "single" for single-query, "rk" for multi-query with multiple application parameters  and "rkws" for multi-query with both multiple application and windowing parameters
 - **algorithm**: Represents the outlier detection algorithm. Possible values for *single-query space* are: "*naive*", "*advanced*", "*advanced_extended*", "*slicing*", "*pmcod*" and "*pmcod_net*". Possible values for multi-query space are "*amcod*", "*sop*", "*psod*" and "*pmcsky*"
 - **W**: Windowing parameter. Represents the window size. On the multi-query space many values can be given delimited by ";"
 - **S**: Windowing parameter. Represents the slide size. On the multi-query space many values can be given delimited by ";"
 - **k**: Application parameter. Represents the minimum number of neighbors a data point must have in order to be an inlier. On the multi-query space many values can be given delimited by ";"
 - **R**: Application parameter. Represents the maximum distance that two data points can have to be considered neighbors. On the multi-query space many values can be given delimited by ";"
 - **dataset**: Represents the dataset selected for the input. Affects the partitioning technique
 - **partitioning**: Represents the partitioning technique chosen for the job. "*replication*" partitioning is mandatory for "naive" and "advanced" algorithms whilst "*grid*" and "*tree*" partitioning is available for every other algorithm. "*grid*" technique needs pre-recorded data on the dataset's distribution. "*tree*" technique needs a file containing data points from the dataset in order to initialize the VP-tree
 - **tree_init**: (Optional) Represents the number of data points to be read for initialization by the "*tree*" partitioning technique. *Default value is 10000*

## Deployment

Docker compose can be used in order to deploy PROUD along with the necessary frameworks for input, output and visualization. The stack includes: 

 - [**Apache Kafka**](https://kafka.apache.org/): used for the input stream
 - **Flink**: used for the outlier detection job
 - [**InfluxDB**](https://www.influxdata.com/): used for storing the output of the outlier detection job as well as metrics from Flink
 - [**Grafana**](https://grafana.com/): used for real-time visualization of the results and metrics

 The **docker/compose.sh** file provides all of the necessary functions through 4 possible argument values.

    ./compose.sh build
Builds the project and creates a custom Flink image containing the compiled file along with the dataset files.

    ./compose.sh start
Starts by building the custom Flink image if it does not exist and starts the stack of Kafka, Flink, InfluxDB and Grafana.

    ./compose.sh stop
Stops and removes the stack of the frameworks.

    ./compose.sh ui
Brings online the UI on the Flink JobManager container.

Every framework's version and docker image along with the frameworks' configurations can be changed in the **docker/variables.sh** file.

## UI

A UI form is available from the Flink JobManager's container where the user can start or cancel an outlier detection job. The necessary parameters for the job need to be inputted in the form. 

When a job is initialized through the UI, a custom stream generator job (implemented in Flink) is also initialized to create a dummy input stream based on the dataset's distribution. The generator creates approximately 1 data point per 1 millisecond and writes them to Kafka. The generator's parallelism level is set to 1.

## Docker ports

The available ports that are exposed from the Docker deployment through the above process are:

- 8000: User Interface
- 8081: Flink's Web UI 
- 3000: Grafana's Web UI

## Changelog

### Version 3.2.0

* Added adaptive partitioning on VP tree technique along with 3 algorithms to support the adaptations.

* Initial code for adaptations in package `adapt`. Classes `Adapt`, `NaiveAdapt` and `NoAdapt` contain all the new functionalities.

### Version 3.1.0

* Moved to PROUD repository.
