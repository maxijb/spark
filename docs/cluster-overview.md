---
title: Cluster Mode Overview
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Cluster Mode Overview

This document gives a short overview of how Spark runs on clusters, to make it easier to understand the components involved. Read through the [application submission guide](submitting-applications.html) to learn about launching applications on a cluster.

## Components

Spark applications run as independent sets of processes on a cluster, coordinated by the `SparkContext` object in your main program (called the _driver program_).

Specifically, to run on a cluster, the SparkContext can connect to several types of _cluster managers_ (either Spark's own standalone cluster manager, YARN or Kubernetes), which allocate resources across applications. Once connected, Spark acquires _executors_ on nodes in the cluster, which are processes that run computations and store data for your application. Next, it sends your application code (defined by JAR or Python files passed to SparkContext) to the executors. Finally, SparkContext sends _tasks_ to the executors to run.

![Spark cluster components](../.gitbook/assets/cluster-overview.png)

There are several useful things to note about this architecture:

1. Each application gets its own executor processes, which stay up for the duration of the whole application and run tasks in multiple threads. This has the benefit of isolating applications from each other, on both the scheduling side (each driver schedules its own tasks) and executor side (tasks from different applications run in different JVMs). However, it also means that data cannot be shared across different Spark applications (instances of SparkContext) without writing it to an external storage system.
2. Spark is agnostic to the underlying cluster manager. As long as it can acquire executor processes, and these communicate with each other, it is relatively easy to run it even on a cluster manager that also supports other applications (e.g. YARN/Kubernetes).
3. The driver program must listen for and accept incoming connections from its executors throughout its lifetime (e.g., see [spark.driver.port in the network config section](configuration.html#networking)). As such, the driver program must be network addressable from the worker nodes.
4. Because the driver schedules tasks on the cluster, it should be run close to the worker nodes, preferably on the same local area network. If you'd like to send requests to the cluster remotely, it's better to open an RPC to the driver and have it submit operations from nearby than to run a driver far away from the worker nodes.

## Cluster Manager Types

The system currently supports several cluster managers:

* [Standalone](spark-standalone.html) -- a simple cluster manager included with Spark that makes it easy to set up a cluster.
* [Hadoop YARN](running-on-yarn.html) -- the resource manager in Hadoop 3.
* [Kubernetes](running-on-kubernetes.html) -- an open-source system for automating deployment, scaling, and management of containerized applications.

## Submitting Applications

Applications can be submitted to a cluster of any type using the `spark-submit` script. The [application submission guide](submitting-applications.html) describes how to do this.

## Monitoring

Each driver program has a web UI, typically on port 4040, that displays information about running tasks, executors, and storage usage. Simply go to `http://<driver-node>:4040` in a web browser to access this UI. The [monitoring guide](monitoring.html) also describes other monitoring options.

## Job Scheduling

Spark gives control over resource allocation both _across_ applications (at the level of the cluster manager) and _within_ applications (if multiple computations are happening on the same SparkContext). The [job scheduling overview](job-scheduling.html) describes this in more detail.

## Glossary

The following table summarizes terms you'll see used to refer to cluster concepts:

| Term            | Meaning                                                                                                                                                                                                                                                                |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Application     | User program built on Spark. Consists of a _driver program_ and _executors_ on the cluster.                                                                                                                                                                            |
| Application jar | A jar containing the user's Spark application. In some cases users will want to create an "uber jar" containing their application along with its dependencies. The user's jar should never include Hadoop or Spark libraries, however, these will be added at runtime. |
| Driver program  | The process running the main() function of the application and creating the SparkContext                                                                                                                                                                               |
| Cluster manager | An external service for acquiring resources on the cluster (e.g. standalone manager, YARN, Kubernetes)                                                                                                                                                                 |
| Deploy mode     | Distinguishes where the driver process runs. In "cluster" mode, the framework launches the driver inside of the cluster. In "client" mode, the submitter launches the driver outside of the cluster.                                                                   |
| Worker node     | Any node that can run application code in the cluster                                                                                                                                                                                                                  |
| Executor        | A process launched for an application on a worker node, that runs tasks and keeps data in memory or disk storage across them. Each application has its own executors.                                                                                                  |
| Task            | A unit of work that will be sent to one executor                                                                                                                                                                                                                       |
| Job             | A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action (e.g. `save`, `collect`); you'll see this term used in the driver's logs.                                                                                          |
| Stage           | Each job gets divided into smaller sets of tasks called _stages_ that depend on each other (similar to the map and reduce stages in MapReduce); you'll see this term used in the driver's logs.                                                                        |
