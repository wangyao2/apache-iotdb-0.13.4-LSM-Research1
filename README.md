<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
[English](./README.md) | [中文](./README_ZH.md)
# Introduction of Repository
Welcome to the repository of PreS Compaction.

This repository contains the Server code for the research, "Pre-Select files for compaction" (PreS).

We have integrated PreS into Apache IoTDB, an LSM-tree based time series database, which is an open-source platform with superior performance.

# PreS Compaction Strategy
Pre-Select files for compaction strategy (PreS) is a dynamic compaction strategy to predict query patterns and select files for compaction. PreS is tailored for time series database.
PreS can capture the incoming queries, analyze historical access information, extract the features of the captured queries, and generate samples using the temporal features of time series. 
Based on a machine learning model, PreS predicts the query patterns expected by users, which alleviates the issue of static methods failing to track query trends. 
The predicted query patterns will be used to guide the compaction process of LSM-tree. PreS predicts query patterns and evaluates compaction benefit to find a compromise between the number of files and read amplification, thereby enhancing the adaptability of compaction to queries and reducing query cost.

The following figure shows the architecture of PreS:
Please refer to figure 3. The specific picture will be filled after the paper is accepted.

It consists of four core components: Query Collector, Query Pattern Predictor, Compaction Benefit Analyzer, and File Selector.
For more specific details, please refer to the paper "PreS: A .....".

# IoTDB
About Apache IoTDB:

IoTDB (Internet of Things Database) is a data management system for time series data, which can provide users specific services, such as, data collection, storage and analysis. 
Due to its light weight structure, high performance and usable features together with its seamless integration with the Hadoop and Spark ecology, IoTDB meets the requirements of massive dataset storage, high throughput data input, and complex data analysis in the industrial IoT field.

Main features of IoTDB are as follows:
1. Flexible deployment strategy. IoTDB provides users a one-click installation tool on either the cloud platform or the terminal devices, and a data synchronization tool bridging the data on cloud platform and terminals.
2. Low cost on hardware. IoTDB can reach a high compression ratio of disk storage.
3. Efficient directory structure. IoTDB supports efficient organization for complex time series data structure from intelligent networking devices, organization for time series data from devices of the same type, fuzzy searching strategy for massive and complex directory of time series data.
4. High-throughput read and write. IoTDB supports millions of low-power devices' strong connection data access, high-speed data read and write for intelligent networking devices and mixed devices mentioned above.
5. Rich query semantics. IoTDB supports time alignment for time series data across devices and measurements, computation in time series field (frequency domain transformation) and rich aggregation function support in time dimension.
6. Easy to get started. IoTDB supports SQL-Like language, JDBC standard API and import/export tools which is easy to use.
7. Seamless integration with state-of-the-practice Open Source Ecosystem. IoTDB supports analysis ecosystems such as, Hadoop, Spark, and visualization tool, such as, Grafana.
8. For the latest information about IoTDB(https://iotdb.apache.org/), please visit IoTDB official website.

# Code description
Please refer to the paper for the design concept of the components.
The specific content will be supplemented after the paper is accepted.

## 1 Query Collector
The Java classes you can refer to for specific code are QueryMonitorYaos, in package org.apache.iotdb.db.engine.compaction.

## 2 Query Pattern Predictor
The Java classes you can refer to for specific code are MLQueryAnalyzerYaos, in package org.apache.iotdb.db.engine.compaction;.

## 3 Compaction Benefit Analyzer
The Java classes you can refer to for specific code are YaosSizeCompactionSelector, in package org.apache.iotdb.db.engine.compaction.inner.sizetiered.

Function "selectLevelTask_byYaos_V1()" in the class describes how the compaction evaluator selects files from the disk and submits them for compaction.

## 4 File Selector
The Java classes you can refer to for specific code are YaosSizeCompactionSelector, in package org.apache.iotdb.db.engine.compaction.

With the periodic trigger of the compaction operation, the function "selectLevelTask()"  will trigger all the operations of PreS and ultimately submit the selected files for compaction.