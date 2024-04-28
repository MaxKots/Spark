# Spark

This repo contains various [examples](./examples.ipynb) of using Spark on python.

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
I'll add a few more points soon
```
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark import SparkConf

SPARK_CONFIG = [
    ('spark.driver.maxResultSize', '4g'),  # Должно быть не более 80% от памяти
    ('spark.driver.memory', '4g'), # Память на драйверах
    ('spark.executor.memory', '60g'), # Память на исполнителях
    ('spark.executor.memoryOverhead', '4g'), # Параметр допустимого переполнения памяти исполнителя
    ('spark.driver.memoryOverhead', '4g'), # Параметр допустимого переполнения памяти драйвера
    # Если включаешь динамическое добавление ресурсов, то отключи параметр ниже
    ('spark.executor.insatances', '5'), # Кол-во исполнителей, стоит учитывать, что драйвер и память будут выделены на инстант
    ('spark.executor.cores', '5'), #Было 8. Рекомендовано - 2, обычно ставят 5. Больше ядер - меньше исполнителей, меньше операций ввода-выводла и параллелизма
    ('spark.driver.cores', '5'), #Было 8. Рекомендовано - 2, обычно ставят 5. Больше ядер - меньше исполнителей, меньше операций ввода-выводла и параллелизма
    ('spark.cores.max', '15'), #Было 16, если ставим число не кратное  кол-ву исполнителей, получим висящие ядра, не участвующие в рабте
    ('spark.dynamicAllocation.enabled', 'false'), #Включить динамическое выделение ресурсов
    # ('spark.dynamicAllocation.minExecutors', '0'), #Дефолт - 1,то есть исполнители будут подключены автоматически при необходимости
    # ('spark.dynamicAllocation.maxExecutors', '24'), #Динамическое распределение исполнителей
    # ('spark.dynamicAllocation.executorIdleTimeout', '600s'), #Отключить исполнитель при простое более N-секунд (дефолт - 300)
    # ('spark.dynamicAllocation.cachedExecutorIdleTimeout', '600s'), #Отключить исполнитель с кэшем при простое более N-секунд (кэш храниться до падения сессии(дефолт - infinity))
    ('spark.sql.adaptive.enabled', 'True'),
    ('spark.sql.adaptive.forceApply', 'False'),
    ('spark.sql.adaptive.logLevel', 'info'),
    ('spark.sql.adaptive.advisoryPartitionSizeInBytes', '256m'),
    ('spark.sql.adaptive.coalescePartitions.enabled', 'True'),
    ('spark.sql.adaptive.coalescePartitions.minPartitionNum', '1'),
    ('spark.sql.adaptive.coalescePartitions.initialPartitionNum', '8192'),
    ('spark.sql.adaptive.fetchShuffleBlocksInBatch', 'True'),
    ('spark.sql.adaptive.localShuffleReader.enabled', 'True'),
    ('spark.sql.adaptive.skewJoin.enabled', 'True'),
    ('spark.sql.adaptive.skewJoin.skewedPartitionFactor', '5'),
    ('spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes', '400m'),
    ('spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin', '0.2'),
    ('spark.sql.autoBroadcastJoinThreshold', '-1'),
    ('spark.sql.optimizer.dynamicPartitionPrunning.enabled', 'True'),
    ('spark.sql.cbo.enabled', 'True'), # Включить Cost-based optimisation в Catalyst 
    ('spark.scheduler.mode', 'FAIR'), # Наиболее оптимальный алгоритм шедулера. Включать только в том случае, если он настроен
    ('spark.sql.codegen', 'false'), #Компилирует каждый запрос в байт-код Java на лету
    ('spark.sql.inMemoryColumnarStorage.compressed', 'False'), #Автоматическое сжатие табличного хранилища в памяти
    ('spark.sql.inMemoryColumnarStorage.batchsize', '1000'), #Размер пакета кэширования. Слишком большие значения приводят к ошибкам исчерпания памяти
    ('spark.sql.parquet.compression.codec', 'snappy'), #Используемый кодек сжатия (uncompressed, snappy, gzip, lzo)
    ('spark.sql.broadcastTimeout', 360), #5 минут таймаут ожидания трансляции данных (полезно, если broadcast готов не с самого начала работы запроса, а позднее)
    ('spark.sql.execution.arrow.enabled', 'true'), # Для эффективного преобразования датафрейма Spark в Pandas
    # ('spark.sql.session.timeZone', 'UTC+3'), #Устанавливаем локальный часовой пояс. Может быть конфликт с данными в Hive
    ('spark.bebug.maxToStringFields', '200'), #Максиммальное кол-во записей в строке дебага, все выходящее за кол-во будет скрыто
    ('spark.sql.parquet.binaryAsString', 'True'),
    ('spark.sql.parquet.int96TimestampConversion', 'True'),
    ('spark.sql.parquet.WriteLegacyFormat', 'True'),
    ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'), #на всякий случай объявляем Kryo как механизм сериализации
    ('spark.kryoserializer.buffer', '24m'),
    ('spark.kryoserializer.buffer.max', '48m'), 
    ('spark.sparkContext.setLogLevel', 'OFF'), #- сократить размер логов с варнингами (ERROR\WARN)
    ('spark.sql.shuffle.partitions', '350'), # - Кол-во партиций для DataSet и DataFrame
    ('spark.default.parallelism', '350') # - Кол-во партиций для join и reduceByKey для RDD 
]

conf = SparkConf().setAll(SPARK_CONFIG)
spark = SparkSession.builder.config(conf = conf).getOrCreate()
```
