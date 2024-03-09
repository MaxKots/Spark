# Spark

This repo contains various [examples](MaxKots/Spark) of using Spark on python.

Apache Spark is an open-source multi-language Big Data framework for distributed batch and stream processing calculations on single-node machines or clusters.

<!--Пользовательская документация-->
### Documentation
[Here](https://spark.apache.org/docs/latest/api/python/reference/index.html) you can find all necessary documentation.

<!--Компоненты Spark-->
### Apache Spark ecosystem

 
- <b>Spark core</b> is backbone of the platform's execution engine, it provides the basic functionality of the framework such as task management, data distribution, planning and execution of operations in the cluster
- <b>SQL</b> is a mechanism for analytical data processing using SQL-interactive SQL on the Hadoop system
- <b>MLlib</b> is a powerful low-level library for simplifying the development and deployment of scalable machine learning pipelines, which includes most useful ML algorithms
- <b>Streaming</b> - a library for processing unstructured and semi-structured streaming data. Allows to implement data streams of more than a gigabyte per second. Often used in conjunction with SparkSQL and MLlib
- <b>GraphX</b> - module GraphX enables graph-parallel processing for analyzing graph networks using API, providing valuable information about the structure and relationships within the graph network.

<p align="center">
  <img src="https://github.com/MaxKots/Spark/blob/main/assets/SparkSchema.png">
</p>

### Spark session
First of all you need to set up your Spark session with most important config options. We will use Jupyter to work with PySpark.

On some clusters your Spark session configure starts automatically with defailt parameters. You can start default Spark session using Spark Python API with command:
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
```
To get SparkSession parameters run the following:
```
spark.sparkContext.getConf().getAll()
```

Here is my own Spark config, use it at your own discretion, but take into consideration your cluster resources, it`s better not no exceed them.
```
Code
```
