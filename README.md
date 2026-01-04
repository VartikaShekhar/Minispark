# MiniSpark

MiniSpark is a simplified single node implementation of the Spark data processing framework built using C and POSIX threads. It models Spark style data processing pipelines using Resilient Distributed Datasets and executes transformations in parallel on a shared memory system.


## Project Overview

Distributed data processing frameworks such as Spark allow users to express large scale data transformations using simple declarative APIs. Internally these systems handle parallel execution scheduling synchronization and fault tolerance.

MiniSpark recreates the core execution model of Spark on a single machine using threads. It represents computations as a directed acyclic graph of RDDs where each node corresponds to a dataset and each edge represents a transformation. Execution is deferred until an action is invoked at which point the framework materializes the required partitions in parallel.

## Objectives

This project focuses on:
- Thread creation and synchronization
- Thread pools and work queues
- Condition variables and mutual exclusion
- Parallel execution of dependent tasks
- Memory management in concurrent systems

MiniSpark supports the following transformations:

  ```
  map  
  filter  
  join  
  partitionBy
  ``` 

It also supports the following actions:

  ```
count  
  print  
```
Transformations are lazy and only executed when an action is called.


## Parallel Execution Model

The framework uses a thread pool to execute tasks concurrently. Each task is responsible for materializing a partition of an RDD. Dependencies between RDDs are respected to ensure correctness while still allowing maximum parallelism.

Parallelism is achieved by:
- Executing independent partitions of the same RDD concurrently
- Executing independent subgraphs of the DAG concurrently

The number of worker threads adapts to the number of available CPU cores.

## Metrics and Monitoring

MiniSpark records runtime metrics for each task including:
- Task creation time
- Task start time
- Task execution duration

Metrics are collected in a thread safe queue and written to a log file by a dedicated monitoring thread.


## Code Structure

applications  
Contains example applications built on top of MiniSpark

lib  
Public API used by applications and tests. This code should not be modified

solution  
Contains the MiniSpark implementation including RDD materialization scheduling and thread management

tests  
Automated test cases used to validate correctness


## License

This project was developed for educational purposes as part of a university course (CS537 Operating Systems).
