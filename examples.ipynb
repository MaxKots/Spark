
# Проверочный
from pyspark.sql import SparkSession
import pyspark.pandas as sp

spark = SparkSession.builder.getOrCreate()
spark.sql("select 1").show()
???
spark = (
SparkSession.builder \
.master('yarn')
.appName('meetup_dapp_static_spark_job')
.config('spark.some.config.option','some-value')
.enableHiveSupport() \
.getOrCreate()
)
???
spark.stop()
???
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
???
# %%timeit
import pandas as pd
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as f
from pyspark.conf import SparkConf
import pyspark.pandas as ps

SPARK_CONFIG = [
    ('spark.driver.maxResultSize', '4g'),  # не более 80% от памяти. Было 32g
    ('spark.driver.memory', '4g'), # Рекомендовано 12g
    ('spark.executor.memory', '60g'), # Рекомендовано 12g
    ('spark.executor.memoryOverhead', '4g'), #Было 12g
    ('spark.driver.memoryOverhead', '4g'),
    # Если включаешь динамическое добавление ресурсов, то отключи параметр ниже
    ('spark.executor.insatances', '5'), #Было 5. Кол-во исполнителей, стоит учитывать, что драйвер и память будут выделены на инст!!!
    ('spark.executor.cores', '5'), #Было 8. Рекомендовано - 2, обычно ставят 5. Больше ядер - меньше исполнителей, меньше операций ввода-выводла и параллелизма
    ('spark.driver.cores', '5'), #Было 8. Рекомендовано - 2, обычно ставят 5. Больше ядер - меньше исполнителей, меньше операций ввода-выводла и параллелизма
('spark.cores.max', '15'), #Было 16, если ставим число не кратное  кол-ву исполнителей, получим висящие ядра, не участвующие в рабте
    ('spark.dynamicAllocation.enabled', 'false'), #Включить динамическое выделение ресурсов
    # ('spark.dynamicAllocation.minExecutors', '0'), #Дефолт - 1,то есть исполнители будут подключены автоматически при необходимости
    # ('spark.dynamicAllocation.maxExecutors', '24'), #Дефолт - 24, было 25
  # ('spark.dynamicAllocation.executorIdleTimeout', '600s'), #Отключить исполнитель при простое более N-секунд (дефолт - 300)
    # ('spark.dynamicAllocation.cachedExecutorIdleTimeout', '600s'),    #Отключить экзекьютор\исполнитель с кэшированными данными при простое более N-секунд, так как кэш будет храниться до падения сессии(дефолт - infinity)
        # Добавлено 11.03.2024
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
    ('spark.sql.cbo.enabled', 'True'),
    ('spark.sql.execution.arrow.pyspark.enabled', 'True'),
    ('spark.scheduler.mode', 'FAIR'),
    ('spark.sql.codegen', 'false'), #Компилирует каждый запрос в байт-код Java на лету
    ('spark.sql.inMemoryColumnarStorage.compressed', 'False'), #Автоматическое сжатие табличного хранилища в памяти
    ('spark.sql.inMemoryColumnarStorage.batchsize', '1000'), #Размер пакета кэширования. Слишком большие значения приводят к ошибкам исчерпания памяти
    ('spark.sql.parquet.compression.codec', 'snappy'), #Используемый кодек сжатия (uncompressed, snappy, gzip, lzo)
 ('spark.sql.broadcastTimeout', 360), #5 минут таймаут ожидания трансляции данных (полезно, если broadcast готов не с самого начала работы запроса, а позднее)
    # ('park.sql.execution.arrow.pyspark.enabled', 'true'),
    #('spark.sql.session.timeZone', 'UTC+3'), #Устанавливаем локальный часовой пояс
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

conf = SparkConf().setAppName('JUST_DO_IT').setAll(SPARK_CONFIG)
spark = SparkSession.builder.master('yarn').config(conf = conf).enableHiveSupport().getOrCreate()
sc = spark.sparkContext
# sc.setLogLevel("FATAL") - сократить размер логов с варнингами
sqlContext = SQLContext(sc)
# spark.sparkContext.getConf().getAll() # Отобразить конфиг
???
##### Показать конфиг Spark
spark.sparkContext.getConf().getAll()
???
#### Вызвать Explain
spark.sql('select * from tbl_nm').explain(extended = False)
???
# Можно запускать запрос либо через существующую сессию, лиюо создать новую. Работают оба варианта
# spark.sql("select count(*) from db_name.tbl_name").show()
# или
# spark.newSession().sql("select count(*) from db_name.tbl_name").show()
???
##### Обращение к **Hive** через **HiveSpark**(если так можно выразиться)
from pyspark.sql import HiveContext

hiveCtx = HiveContext(sc)
# print(hiveCtx)
rows = hiveCtx.sql("select count(*) from db_name.tbl_name")
firstRow = rows.first()

firstRow
???
##### Показать spark
spark
???
##### Создать DataFrame 
ds = [
    ['Миша', '111', 'Привет'],
    ['Жора', '111', 'Пока'],
    ['Глаша', '333', 'Как дела']
]
df = spark.createDataFrame(ds, ['col1', 'col2', 'col3'])
???
#### **Важно!**
если наименование столбцов на <u>русском</u>, то **df.filter('col1 like "Глаша"').show()** работать <u>НЕ</u> будет
???
df.show(truncate = 4)
# (truncate = False) или () - для полного отображения данных, без обрезки
# Тут надо дописать, так как написанное выше неправильно
df.printSchema()
или
# df.schema.fields
???
##### <u>Посчитать</u>
???
# Столбцы
len(df.columns)
???
# Строки
df.count()
???
##### <u>Выбрать</u>
???
# df.show(2)
# show(2) - показать кол-во строк 
# df.select('col1').show()
# df = df.select('*')
???
##### <u>Функции</u>
???
from pyspark.sql import functions as f

# Можно просто испортировать функцию col, чтобы потом не пписать f.col каждый раз
# from pyspark.sql.functions import col
???
newdf = df.select('col1', f.concat('col2','col3').alias('конкатенированный')).toPandas()
# 'столбец1' можно заменить на df.columns[0], то есть обратиться к столбцу не по имени, а по номеру
# newdf.show()
newdf
???
##### <u>Удаление колонок</u>
???
df.drop('col1', 'col2').printSchema()
df.drop('col1', 'col2').show()

# Вот так нелья, так как объекты immutable(лол, ранньше не работало, сейчас пашет спокойно)
# df.drop(df['столбец1']).show()
???
##### <u>Добавление колонки</u>
???
# 1 способ - через with Column
newdf = df.withColumn('Новый столбец', f.substring('col1', 1, 1))
newdf.show(1)
???
# 2 способ - через select
nefdf = df.select('*', f.substring('col1', 1, 1).alias('Новый столбец'))
newdf.show(1)
???
### <u>Отбор строк: **filter()** и **distinct()**</u> 
???
df.filter(f.col('col1').startswith('Жор')).show()
# Или
df.filter(f.col('col1').like('Жор%')).show()
# или HiveQL, помни, про имя столбцов на английском
df.filter('col1 like "Жор%"').show()
???
# Питон перед выполнением склеивает все, что в скобках, иначе пришлось бы ставить '/' после каждой строки
(
    df
    .select('col1') # или .select(df.columns[0])
    .filter(df.col1 == "Глаша") # или .filter('col1 = "Глаша"')
    .show()
)
???
#### <u>И\ИЛИ</u>
???
# И
df.filter('col1 like "Г%"').filter('col1 like "Ж%"').show()
???
# ИЛИ
df.filter('col1 like "Г%" or col1 like "Ж%"').show()
???
# StructuredAPI, побитовое ИЛИ - это как надо по хорошему
# Стоит запомнить, что побитовое ИЛИ по рангу выше операций сравнения, которые придется заключать в скобки
(
    df
    .filter(
        f.col('col1').startswith("Ж") | f.col('col1').startswith('Г')
    )
    .show()
)
???
#### <u>Несколько like</u>
???
REP_METALL_B. \
select(f.col('INFO_NORM'), f.col('WO_VAT')). \
where('INFO_NORM like "%НДС(НЕТ)%" or INFO_NORM like "%БЕЗ(НДС%"'). \
show(truncate = False)

# ИЛИ

REP_METALL_B. \
select(f.col('INFO_NORM'), f.col('WO_VAT')). \
where(f.col('INFO_NORM').like('%НДС(НЕТ)%') | f.col('INFO_NORM').like('%БЕЗ(НДС%')). \
show(truncate = False)
???
###### Стоит запомнить, что побитовое ИЛИ по рангу выше операций сравнения, <u>которые придется заключать в скобки</u>
???
# Пример
(
    df
    .filter(
        (f.length('col1') == 4) | (f.length('col1') == 5)
    )
    .show()
)
???
### Distinct
???
df.select('col1').distinct().show()
???
#### Добавление строк - JOIN и UNION
???
ctrydf = spark.createDataFrame(
    [
        [0, "ru", "Russia"],
        [1, "it", "Italy"],
        [2, "de", "Germany"]
    ],["id", "code", "name"]
)
???
ctrydf.show()
???
capitaldf = spark.createDataFrame(
        [
            [100, 'ru', 'Moscow', 140],
            [101, 'it', 'Rome', 100]
        ],['id', 'ctry_code', 'city', 'population']
)
???
capitaldf.show()
???
#### Непосредственно **Join**
???
# Можно через joinexpression, в котором укажем, как будм связывать таблицы
joinExpression = ctrydf['code'] == capitaldf['ctry_code']
joinExpression
???
newdf = ctrydf.join(capitaldf, joinExpression, 'inner')
# иначе пришлось бы писать так:
# newdf = ctrydf.join(capitaldf, ctrydf['code'] == capitaldf['ctry_code'], 'inner')
newdf.show()
???
# Обрати внимание, после join идет дублирование колонок -→ вылетает ошибка
newdf.select('id').show()
# Решить можно через явное указание датафрейма:
newdf.select(capitaldf['id']).show()
# Решить можно при джойне, через alias датафрейма:
#     newdf = ctrydf.join(capitaldf.alias('cap'), joinExpression, 'inner')
#     newdf.select('cap.id').show()
# Решить можно при джойне, через переименование столбца и join по общему столбцу.
# Сначала отображается общий столбец, пттом левая таблица, потом правая:
#     newdf = ctrydf.join(capitaldf.withColumnRenamed('ctry_code', 'code'), 'code', 'inner')
#     newdf.show()
???
#### Конкатенация датафреймов <u>**Union()**</u>
???
##### Датафреймы должны быть одинковыми по структуре, но можно конкатенировать и разные (union by name)
Нельзя конкатенировать датафреймы с разным кол-вом колонок
???
# Одинкковые!
# newdf.printSchema()
# capitaldf.printSchema()

# Union работает быстрее, чем join
newdf = capitaldf.union(capitaldf)
newdf.show()
???
# Разные!
newdf = capitaldf.union(capitaldf.select('city', 'id', 'ctry_code', 'population'))
newdf.show()
???
#### Агрегация и  сортировка
???
##### По всему датафрейму
???
capitaldf.select(f.sum('population')).show()
???
capitaldf.select(f.sum('population'), f.min('population')).show()
???
#### groupBy
???
capitaldf.groupBy('ctry_code').sum('population').show()
???
##### Для вывода нескольких агрегатов, нужно воспользоваться методом **agg** объекта **groupBy**, который в себе ожидает несколько агрегирующих функций
???
capitaldf.groupBy('ctry_code').agg(f.sum('population'), f.max('population')).show()
???
# При отсутствии данных в groupBy, группируется весь датафрейм
capitaldf.groupBy().agg(f.sum('population')).show()
???
df.show()
???
# вариант 1
(
    df
    .groupBy(f.substring('col2', 1, 3).alias('grouped_data'))
    .agg(f.count('col1').alias('count_data'))
    .show()
)
# вариант 2
(
    df
    .groupBy(f.substring('col2', 1, 3))
    .count()
    .toDF('grouped_data', 'count_data')
    .show()
)
# toDF - для массового переименования столбцов
???
#### **Сортировка (Sort)**
???
ctrydf.sort('id').show()
???
# По убыванию:
ctrydf.sort('id', ascending = False).show()
# Или
ctrydf.sort(f.desc('id')).show()
# Или
ctrydf.sort(f.col('id').desc()).show()
???
### **ПО КНИГЕ**
???
# Передача функций на примере лямбда функции RDD
# parallelize создает RDD
rdd = sc.parallelize([1, 2, 3, 4])
lambdadf = rdd.filter(lambda x: 1 in x)
???
# Функцияя map выполняет действие для каждого лемента RDD
# Функция collect возвращает сдержимое набора RDD
nums = sc.parallelize([1, 2, 3, 4])
squared = nums.map(lambda x: x * x).collect()
for num in squared:
    print(num)
???
#### Команды операций с псевдомножествами
1) distinct() - возврат уникальных строк
2) intersection(other RDD) - возврат значений, присутствующих в обоих RDD (очень ресурсоемко)
3) union(other RDD) - объединение двух RDD
4) subtract(other RDD) - возврат значений, присутствующих лишь в первом(левом) RDD (очень ресурсоемко)
5) cartesian(other RDD) - возврат максимально возможного кол-ва пар из левого и правого RDD (декартово произведение)
???
##### REDUCE & FOLD
???
# REDUCE()
# Функция reduce() оперирует двумя переменными, пример:
# 10 потому, что 1 + 2 + 3 + 4 = 10 (сначала первое суммирование 1 + 2 = 3, потом 3 + 4 = 7, потом первая сумма + вторая сумма = 10)
rdd = sc.parallelize([1, 2, 3, 4])
rdd.reduce(lambda x, y: x + y)
???
# В rdd всего две партиции
rdd.getNumPartitions()
???
# И выглядят они так:
rdd.glom().collect()
???
# FOLD()
# Разница между REDUCE() и FOLD()  в том, что FOLD() имеет zero-value, которое добавляется каждому фолду, пример:
rdd = sc.parallelize([1, 2, 3, 4])
rdd.fold(0, lambda x, y: x + y)
???
# если в качестве zero-value указать единицу, то:
rdd.fold(1, lambda x, y: x + y)
# тринадцать потому, что rdd при fold операции восприинимается так: [[1], [1, 2]+1, [3, 4]+1] = 1 + 4 + 8
???
#### Aggregate()????????????????
???
#### take(n)
???
# Взять несколько значений из RDD (может возвращать не в том порядке, в котором ожидается, на больших данных лучше использовать top)
rdd.take(3)
???
#### top(n)
???
# Взять n упорядоченных данных 
rdd.top(3)
???
#### takeSample(withReplacement, num, seed)
???
# Пример(менять местами, кол-во, random seed(опционально))
rdd.takeSample(True, 20, 1)
???
#### foreach()???????????????????
???
#### Кэширование
???
##### Для Датафреймов createOrReplaceTempView()
???
# Создать временную таблицу
df.createOrReplaceTempView()
# Отобразить данные, сохраненные в TempView .show(truncate = False) - не работает
spark.table('name').show()

# Отобразить все TempView
spark.catalog.listTables()
# Удалить TempView
spark.catalog.dropTempView("name")

# отобрать одну строку из временой таблицы
# spark.table('cards').show(1)
# или
spark.table('name').limit(1).toPandas()

# Показать тип данных во временной таблице
spark.table('name').printSchema()


# - если таблица уже зачистилась из кэша, ее можно обновить, перевыполнив создающий ее таск
spark.sql('REFRESH TABLE tbl_name') 


# ВНИМАНИЕ!
# созданные временные таблицы можно применять как переменные в SPARK SQL-запросах
???
##### persist()
???
from pyspark.storagelevel import StorageLevel
dfpersist = df.persist(StorageLevel.MEMORY_AND_DISK)
dfpersist.show()
???
# Для удаления DF из кэша
dfpersist.unpersist()
???
#### cache()
???
# cache() это = persist(StorageLevel.MEMORY_ONLY)
df.cache()
df.show()
???
#### Партиционирование
???
# Показать партиции
spark.sql("""describe extended prod_dds.document_w4vtb_hist""").filter(f.col('col_name') == 'Partition Information').show()


# Партиции и статистика
spark.sql("""describe extended prod_dds.customer_registry_mdm_hist""").filter(f.col('col_name').isin('Partition Information', 'Statistics')).show(truncate = False)

# Или:
(
    spark.sql('describe formatted prod_dds.TRANSACTION_PARAMETER_TB')
    .filter(f.col('col_name').isin('Partition Information', 'Statistics') | f.upper(f.col('col_name')).startswith("PARTITION"))
    .show(truncate = False)
)

# Или показать сами партиции:
spark.sql('show partitions prod_dds.TRANSACTION_PARAMETER_TB').show(truncate = False)
???
#### Подсчет статистики???????????????
???
#### Невозможность отобразить timestamp '5999-12-31 00:00:00'
from pyspark.sql.types import IntegerType, LongType, StringType, DoubleType, DateType, TimestampType, BooleanType

accs_df = accs.withColumn('effective_to_dttm', f.col('effective_to_dttm').cast(StringType())).toPandas()
accs_df

# Или так

all_cards.withColumn('CLOSE_PLAN_DT', f.col('CLOSE_PLAN_DT').cast("date")).withColumn('OPEN_DT', f.col('OPEN_DT').cast("date")).toPandas()
all_cards
???
#### RDD типа ключ\значение
???
##### groupByKey \ groupByValue \ cogroup()
???
# Если у нас есть RDD типа ключ-значение, можем выполнять действия аналогичне reduceByKey(), но намного эффективнее.
# groupByKey группирует по ключу
# groupByValue группирует по значению
# Для работы  с несколькими RDD типа ключ-значение(где ключ - А. B и C - значения), применяем cogroup(), что даст нам соединение по ключу A и отражение 
#     значений B и С

# аналогично с sortByKey()
???
# При join можно узнать, присутствующее значение в RDD функцией isPresent(), а получить результат с помощью get()
# Не работает с типом данных dataFrame
???
#### lookup() и countByKey()
???
rdd = sc.parallelize({
    (1,2),
    (3,4),
    (3,6)})
# Мы млжем вывести все значения по ключам
rdd.lookup(3)
# Можем посчитать кол-во ключей
rdd.countByKey()
???
# Можем разделять RDD на партиции между кластерами памяти, чтобы быстрее обращаться к ним, но это применяется очень редко
rdd.partitionBy(10)
# Например, sortByKey  и groupByKey автоматом хешируют и делят данные по партициям
???








