# PROUD: PaRallel OUtlier Detection for streams 

**PROUD**[1] is an open-source high-throughput distributed outlier detection engine for intense data streams that is implemented in Scala on top of the [Apache Flink](https://flink.apache.org/) framework.

It supports distance-based outlier detection algorithms both for single-query[2] and multi-query[3] parameter spaces. On the multi-query case there are two distinct types. The first one is the multi-app level parameters involving *k* and *R* whilst the second one is the multi-all level with many values for *k*, *R*, *W* and *S*.

It also provides two different ways of partitioning incoming data, namely *grid-based* and *tree-based* technique. The first one is only used for the euclidead space while the second one can be used for any metric space.

Finally an automated adaptive mechanism is introduced that helps the partitioning phase to self-balance the workload based on the incoming distribution and metadata from the processing phase. The adaptation process works only with the *tree* partitioning technique and is implemented using an external main-memory database to create a feedback loop on the job.

## Flink job

PROUD can be run within a Flink standalone cluster after the source code has been compiled. Compiling the code with: 

    sbt clean assembly

creates the PROUD-assembly-%%VERSION%%.jar file on the target directory.

To run the job on a Flink standalone cluster the command:

    bin/flink run -d PROUD-assembly-%%VERSION%%.jar --policy static --algorithm pmcod --W 10000 --S 500 --k 50 --R 0.45 --dataset STK -partitioning tree --partitions 3

needs to be executed on the JobManager machine (changing/adding/removing parameters based on the job).

The job's parameters are the following:

 - **algorithm**: Represents the outlier detection algorithm depending on the parameter space needed. For the *single-query space* the available algorithmms are: "*advanced*", "*cod*", "*slicing*", "*pmcod*" and "*pmcod_net*". For the *multi-app* space the "*amcod_rk*", "*sop_rk*", "*psod_rk*" and "*pmcsky_rk*". Finally for the *multi-all* space the "*sop_rkws*", "*psod_rkws*" and "*pmcsky_rkws*".  
 - **W**: Windowing parameter. Represents the window size. On the multi-query space many values can be given delimited by ";".
 - **S**: Windowing parameter. Represents the slide size. On the multi-query space many values can be given delimited by ";".
 - **k**: Application parameter. Represents the minimum number of neighbors a data point must have in order to be an inlier. On the multi-query space many values can be given delimited by ";".
 - **R**: Application parameter. Represents the maximum distance that two data points can have to be considered neighbors. On the multi-query space many values can be given delimited by ";".
 - **dataset**: Represents the dataset selected for the input. Affects the partitioning technique since it needs a sample to create the data structures.
 - **partitioning**: Represents the partitioning technique chosen for the job. The available techniques are the *grid-based* and *tree-based* chosen with the keywords *grid* and *tree* respectively.
 - **sample_size**: (Optional) Represents the number of data points to be read for initialization by the partitioning techniques. *Default value is 10000*
 - **partitions**: Represents the number of total regions that the space will be split into to distribute the incoming data points. For the *grid* technique the value needs to be the number of splits on each dimension with the ";" delimiter. For the *tree* technique the value is the chosen height of the tree and needs to be a power of two, since the tree is binary.
 - **distance**: (Optional) Represents the type of distance that will be used for the incoming data points. Currently the "euclidean" and "jaccard" distances are implemented. *Default value is euclidean*
 - **policy**: (Optional) Represents the policy used for the adaptation process. *naive* and *advanced* are the implemented techniques that self-balance the workload while the *static* keyword represents the static setting with no adaptation taking place on the partitioning phase.
 
When the adaptation policy is NOT static then additional parameters can be used for the techniques:

 - **adapt_range**: Represents the change in the boundaries of a region when a technique needs to adapt the specific partition. *Used by both adaptation policies*
 - **adapt_over**: Represents the percentage of workload that a region needs to have over the normal to be annotated as overloaded. *Used by both adaptation policies*
 - **adapt_under**: Represents the percentage of workload that a region needs to have below the normal to be annotated as underloaded. *Used only by the advanced policy*
 - **adapt_queue**: Represents the number of historical metadata that will be stored and used in each adaptation decision. *Used only by the advanced policy*
 - **adapt_cost**: Represents the cost function that will be computed by the processing phase. Available values are *1* for the number of non-replicas, *2* for the processing time and *3* for the product of *1* and *2*. *Used by both adaptation policies*
 - **buffer_period**: Represents synchronization period from the time that an adaptation decision is taken until the adaptation process starts. The final period is the product of the value with the window size. *Used by both adaptation policies*
 
## Deployment

Docker compose can be used in order to deploy PROUD along with the necessary frameworks for input, output and visualization. The stack includes: 

 - [**Apache Kafka**](https://kafka.apache.org/): used for the input stream
 - **Flink**: used for the outlier detection job
 - [**Redis**](https://redis.io/): used as the main-memory database for the adaptation process
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

## References

[1] 
Theodoros Toliopoulos, Christos Bellas, Anastasios Gounaris, and Apostolos Papadopoulos. 2020. 
PROUD: PaRallel OUtlier Detection for Streams. 
In Proceedings of the 2020 ACM SIGMOD International Conference on Management of Data (SIGMOD '20). 
Association for Computing Machinery, New York, NY, USA, 2717–2720. 
DOI:https://doi.org/10.1145/3318464.3384688

[2] 
Theodoros Toliopoulos, Anastasios Gounaris, Kostas Tsichlas, Apostolos Papadopoulos, Sandra Sampaio.
Continuous outlier mining of streaming data in flink.
Information Systems, Volume 93, 2020, 101569, ISSN 0306-4379.
DOI:https://doi.org/10.1016/j.is.2020.101569.

[3] 
Theodoros Toliopoulos and Anastasios Gounaris, 
Multi-parameter streaming outlier detection. 
In Proceedings of the 2019 IEEE/WIC/ACM International Conference on Web Intelligence (WI), Thessaloniki, Greece, 2019, pp. 208-216.
DOI:https://doi.org/10.1145/3350546.3352520.
