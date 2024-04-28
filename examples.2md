<h3 style="color #460073">ОБХОД УЗКОГО МЕСТА - toPandas().head()</h3>
???
spark.sql("select count(*) from mytable").show(10000, False)
???
from IPython.core.display import HTML
display(HTML("<style>pre { white-space: pre !important;}</style>")) 
spark.sql("select * from mytable").show(10, False)
???
spark.conf.set('spark.sql.repl.eagerEval.enabled','True')
spark.conf.set('spark.sql.repl.eagerEval.maxNumRows','10')
df_view = spark.sql("select * from mytable")
df_view
???
<h1 style="color #460073">ПОДГОТОВКА ИСТОЧНИКОВ ДАННЫХ</h1>
???
<h2 style="color #460073">ЭТАП:1 - ПОДГОТОВКА СПИСКА ИСТОЧНИКОВ</h2>
???
spark.sql("select * from Кредиты").show()
???
df_loans = spark.sql("select * from Кредиты") 20млн
df2 = df.count()
df2_mortgage = spark.sql("select * from Ипотека") 2 млрд
???
#Список источников
mytable - 
mytable2 - Ипотека
???
<h2 style="color #460073">ЭТАП:2 - АНАЛИЗ ПАРТИЦИЙ ИСТОЧНИКА</h2>
???
#Находим название поля по которому партицирована таблица, если она партицирована
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat, lit

descschema = StructType([ StructField("col_name", StringType())
                       ,StructField("data_type", StringType())
                       ,StructField("comment", StringType())])                  
df = spark.sql(f"describe formatted mytable" )
df2=df.where((f.col("col_name")== 'Part 0') | (f.col("col_name")== 'Part 2') | (f.col("col_name")== 'Name')).select(f.col('data_type'))
df3 =df2.toPandas().transpose()
display(df3)
???
<h2 style="color #460073">ЭТАП3: ФИЛЬТРАЦИЯ ИСТОЧНИКОВ И ПОДСЧЕТ КОЛИЧЕСТВО ЗАПИСЕЙ</h2>
???
%%time
#Фильтруем таблицы по полю партиции, считаем count() и записываем его
df = spark.sql("select * from  кредиты where partition_columns=value")
df.count()
#df_mortgage = spark.sql("select * from  ипотека where partition_columns=value")
#df.count()
???
#Размер df в М ( должно сходиться с выводом hadoop fs -du -s -h path, первая цифра без репликации x3
df_size_in_bytes = spark._jsparkSession.sessionState().executePlan(df._jdf.queryExecution().logical(),df._jdf.queryExecution().mode()).optimizedPlan().stats().sizeInBytes()
df_size_in_bytes/1024/1024
???
<h2 style="color #460073">ЭТАП4: КЭШИРОВАНИЕ ДАННЫХ И СОЗДАНИЕ ТЕМПОВЫХ ТАБЛИЦ</h2>
???
%%time
#Мелкие(не более 30млн или не более 30гб) таблицы сразу кешируем
df_loans_cache = df_loans.persist()
df_loans_cache.count()
???
#Таблицы до 2 млрд строк заливаем к себе
#write.mode Мы или добавляем append или перезаписываем overwrite

#Если у таблицы нет поля партициии и кол-во записей от 30млн до 200млн используем вот этот вариает;
df.repartition(1).write.mode("append").format("parquet").option("compression","snappy").saveAsTable("schema.mytables_sbx_temp")

#Если у таблицы нет поля партициии и кол-во записей от 200млн до 2млрд используем вот этот вариает;
df.repartition(10).write.mode("append").format("parquet").option("compression","snappy").saveAsTable("schema.mytables_sbx_temp")

#Если у таблицы есть поле партиции и кол-во записей от 30млн до 200млн используем вот этот вариает;
df.repartition(1,"partition_columns").write.partitionBy("partition_columns").mode("append").format("parquet").option("compression","snappy").saveAsTable("schema.mytables_sbx_temp")

#Если у таблицы есть поле партиции и кол-во записей от 200млн до 2млрд используем вот этот вариает;
df.repartition(10,"partition_columns").write.partitionBy("partition_columns").mode("append").format("parquet").option("compression","snappy").saveAsTable("schema.mytables_sbx_temp")
???
#У нас есть 7 таблиц. 3 мелких справочника мы поместили в кэш, 4 таблицы фактов до 2млрд мы записали себе в песочницу
???
<h2 style="color #460073">ЭТАП5: СБОР СТАТИСТИКИ ПО ТЕМПОВЫМ ТАБЛИЦАМ</h2>
???
#Собираем статистику по таблицам которые не в кеше но у вас песочнице
spark.sql("analyze table mytables_sbx_temp compute statistics noscan").show(False)
spark.sql("analyze table mytables_sbx_temp compute statistics").show(False)
spark.sql("analyze table mytables_sbx_temp compute statistics for all columns").show(False)
???
<h2 style="color #460073">ЭТАП6: СОЗДАНИЕ АЛИАСОВ К ДАННЫМ</h2>
???
#Зачитываем крупные таблицы из свой песочницы
df = spark.sql("select * from mytables_sbx_temp")
???
#Делаем алиас для таблицы,что бы удобно было с ней работать через sql
# Создаем алиас для работы с таблицей внутри sparк sql
df.createOrReplaceTempView("mytable_fact")

# Обращаемся к таблицу по алиасу
spark.sql("select * from mytable_fact").show(20,False)
???
<h2 style="color #460073">ЭТАП 7: АНАЛИЗ ТИПОВ ДАННЫХ</h2>
???
#Анализируем типы данных
spark.sql("select * from mytable").printSchema()
???
<h1 style="color #460073">РАЗРАБОТКА АЛГОРИТМА</h1>
???
<h2 style="color #460073">ЭТАП8: РАЗРАБОТКА АЛГОРИТМА ДЛЯ ВИТРИНЫ</h2>
???
<h4 style="color #460073">РАЗРАБОТКА АЛГОРИТМА</h4>
Вы уже подготовили источники, по ним собрана статистика, сделана компрессия и файл укрупнены через repartition.
Пишем дальше на обычном sql ваш алгоритм и учитываем правила ниже

<h4 style="color #460073">WITH</h4>
Если используем таблицу несколько раз ее нужно вынести в with, вообще хорошо перечислить ваши источники в with сразу
Там же можно делать distict и любые фильтрации, что бы не выносить их в основое теле запроса

<h4 style="color #460073">DATA_TYPE</h4> 
Типы данных при join должны быть одинаковы или кастованы

<h4 style="color #460073">ПЕРЕКОСЫ</h4> 
Большая таблица должна идти первая и дальше к ней left join меньшие таблицы;

<h4 style="color #460073">ЗАПРОСЫ НУЖНО РАЗБИВАТЬ НА ПРЕД АГРЕГАТЫ</h4>  
Алгориты должны состоят из переподготовленных таблиц агрегатов

<h4 style="color #460073">ПЛОХИЕ АЛГОРИТМЫ</h4>   
DISTINCT, DISTINCT + CASE, ORDER BY, GROPBY, OR в JOIN
???
from IPython.core.display import HTML
display(HTML("<style>pre { white-space: pre !important;}</style>")) 

spark.sql("select * from mytable
          
          
          
          
          
          ").show(20,False)
???
df_data_mart
???
<h2 style="color #460073">ЭТАП9: РУЧНОЙ РАСЧЕТ ВИТРИНЫ ДЛЯ АНАЛИЗА АЛГОРИТМА</h2>
???
#write.mode Мы или добавляем append или перезаписываем overwrite

#Если витрина не большая до 100млн и не более 30гб и для нее не предусматриваеться партиция - используем вот этот вариает;
df_data_mart.repartition(1).write.mode("append").format("parquet").option("compression","snappy").saveAsTable("schema.mydatamart_sbx")

#Если витрина не большая до 100млн и не более 30гб и для нее предусматриваеться партиция - используем вот этот вариает;
df_data_mart.repartiton(1,"partition_columns").write.partitionBy("partition_columns").mode("append").format("parquet").option("compression","snappy").saveAsTable("schema.mydatamart_sbx")

# Если витрина большая более 100млн и более 30гб она должна обязательно быть партицирована  - используем вот этот вариает;
df_data_mart.repartiton(10,"partition_columns").write.partitionBy("partition_columns").mode("append").format("parquet").option("compression","snappy").saveAsTable("schema.mydatamart_sbx")
???
#Собираем статистику по витрине
spark.sql("analyze table schema.mytables compute statistics noscan").show(False)
spark.sql("analyze table schema.mytables compute statistics").show(False)
spark.sql("analyze table schema.mytables compute statistics for all columns").show(False)
???
#Очищаем свой кеш из этапа 4 для всех таблиц
spark.catalog.clearCache()
???
#Удаляем большие таблицы которые залили себе в песочницу миную корзину
spark.sql("drop table mytables purge").show()
???
#Освобождаем ресурсы
spark.stop()
???
<h3 style="color #460073">ЭТАП10: АНАЛИЗ ДАННЫХ В ВИТРИНЕ</h3>
???
spark.sql("select * from mydatamart_sbx").show()
???
#Освобождаем ресурсы
sc.stop()
???
<h2 style="color #460073">РЕКОМЕНДАЦИИ ДЛЯ ETL</h2>
???
 - СНАЧАЛА СОЗДАТЬ DDL ЧЕРЕЗ HIVE;
2 - НЕОБХОДИМО ИСПОЛЬЗОВАТЬ INSERT ВМЕСТО CREATE TABLE AS SELECT (ТАК КАК ВСТАВКА ИДЕТ БЫСТРЕЕ), ТО ЕСТЬ СОЗДАЛИ ПУСТУЮ ТАБЛИЦУ В HIVE И ПОТОМ В НЕЕ ТОЛЬКО ИНСЕРТИМ А НЕ СОЗДАЕМ КАЖДЫЙ РАЗ ЧЕРЕЗ CREATE TABLE;
3 - ТАБЛИЦЫ БОЛЕЕ 30ГБ ИЛИ БОЛЕЕ 100МЛН ЗАПИСЕЙ ДОЛЖНЫ БЫТЬ ПАРТИЦИРОВАНЫ В ОБЯЗАТЕЛЬНО ПОРЯДКЕ!!!!!. ПОЛЕ ПАРТИЦИИ ОБЫЧНО ЭТО ДАТА, НЕ ДАТА С ВРЕМЕНЕМ! ТО ЕСТЬ ПАРТИЦИЯ ЭТО КРУПНОЕ ДЕЛЕНИЕ ТАБЛИЦЫ;
4 - ДЛЯ ВСЕХ РЕГЛАМЕНТНЫХ ВИТРИНЫ ДОЛЖНА СОБИРАТЬСЯ РЕГУЛЯРНО СТАТИСТИКА ПО ТАБЛИЦЕ И ПОЛЯМ ТАБЛИЦЫ; ТО ЕСТЬ ПОСЧИТАЛИ ВИТРИНУ В AIRFLOW И ТАМ ЖЕ ЗАПУСТИЛИ;
        SPARK.SQL("ANALYZE TABLE MYDATAMART COMPUTE STATISTICS NOSCAN").SHOW(FALSE)
        SPARK.SQL("ANALYZE TABLE MYDATAMART COMPUTE STATISTICS").SHOW(FALSE)
        SPARK.SQL("ANALYZE TABLE MYDATAMART COMPUTE STATISTICS FOR ALL COLUMNS").SHOW(FALSE)
    ЕСЛИ ВИТРИНА БОЛЬШАЯ СОБИРАЕМ СТАТИСТИКУ ПО ПАРТИЦИЯМ 
        SPARK.SQL("ANALYZE TABLE MYDATAMART PARTITION(COLUMNS_PARTITION=VALUE) COMPUTE STATISTICS").SHOW(FALSE)
        SPARK.SQL("ANALYZE TABLE MYDATAMART PARTITION(COLUMNS_PARTITION=VALUE) COMPUTE STATISTICS FOR ALL COLUMNS").SHOW(FALSE)
5 - ВСЕГДА ПРИ ЗАПИСИ ЯВНО УКАЗЫВАЕМ КОМПРЕССИЮ .OPTION("COMPRESSION","SNAPPY");
6 - ВСЕГДА ФИЛЬТРУЕМ ТАБЛИЦУ ПО ПОЛЮ ПАРТИЦИИ;
7 - УБИРАЕМ ИЗ КОДА #PRINTLN() ЗАМЕНЫЕМ НА ЛОГИРОВАНИЕ!;
8 - ТИПЫ ДАННЫХ ПРИ JOIN ДОЛЖНЫ БЫТЬ ОДИНАКОВЫ ИЛИ КАСТОВАНЫ.
9 - СТАВИМ SPARK.SPARKCONTEXT.SETLOGLEVEL('OFF') ИЛИ ERROR  ТАК КАК ЛОГИРОВАНИЕ ОЧЕНЬ СИЛЬНО ТОРМОЗИТ SPARK JOB
???
<h2 style="color #460073">САМОЕ ГЛАВНОЕ</h2>
???
1 - СОБИРАТЬ СТАТИСТИКУ ВСЕГДА И ВЕЗДЕ
2 - ВСЕГДА УКАЗЫВАТЬ КОМПРЕССИЮ .option("compression","snappy")
3 - НЕ ПЛАДИТЬ МЕЛКИЕ ФАЙЛЫ ДЛЯ ЭТОГО ПИСАТЬ ЧЕРЕЗ REPARTITION И УПЛОТНЯТЬ МЕЛКИЕ ФАЙЛЫ
4 - НЕ СОБИРАТЬ В ОДНОМ ЭТАПЕ АЛГОРИТМА ВЕСЬ СИНТАКСИС SQL
5 - ORDER BY ВАМ В 99% НЕ НУЖЕН, ЕСЛИ ДЕЛАЕТЕ ORDER BY DESC ТО ДЕЛАЙТЕ ЭТО ОТДЕЛЬНО ОТ ДРУГИХ ОПЕРАЦИЙ
6 - DISTINCT НУЖНО ДЕЛАТЬ ОТДЕЛЬНО ОТ ДРУГИХ ОПЕРАЦИЙ
7 - БОЛЬШЕ ИСПОЛЬЗОВАТЬ АНАЛИТИЧЕСКИХ ФУНКЦИЙ
???
<h2 style="color #460073">МЕТОДЫ КОТОРЫЕ ПОЗВОЛЯЮТ УСКОРИТЬ ВЫПОЛНЕНИЕ SPARK JOB</h2>
???
1 - ПЕРЕХОД НА СТАТИСТИЧЕСКОЕ ВЫДЕЛЕНИЕ РЕСУРСОВ
2 - СТАТИСТИКА, КОМПРЕССИЯ, КРУПНЫЕ ФАЙЛ, ФИЛЬТРАЦИЯ ПО ПАРТИЦИЯМ, ВЫКЛЮЧИННОЕ ЛОГИРОВАНИЕ
3 - ЯВНОЕ ВКЛЮЧЕНИЕ spark.sql.adaptive.enabled=True
4 - ЯВНОЕ ВКЛЮЧЕНИЕ spark.sql.cbo.enabled=True
5 - ПЕРЕХОД НА spark.scheduler.mode=FAIR
6 - ИСПОЛЬЗОВАТЬ PANDAS ЧЕРЕЗ SPARK, ЕСЛИ НЕТ ВОЗМОЖНОСТИ ЗАБИРАТЬ СЕБЕ ФАЙЛ ПАРКЕТА НА ВОРКЕР AIRFLOW И ТАМ ЕГО ДУБАСИТЬ У ВАС 4CPU/64RAM 100ГБ, НЕ НУЖНО ЧАНКАМИ ЧИТАТЬ 100ЛЕТ ДАННЫЕ,СПАРК ДЖОБА УЖЕ ДАВНО ЗАКОНЧИЛА А ВЫ ЕЕ ДЕРЖИТЕ.
7 - ЛЮБЫЕ ОПЕРАЦИ С ДАННЫМИ ЧЕРЕЗ ЧИСТЫЙ PYTHON, PANDAS И ТД ВСЕ ИДЕТ В ОДИН ПОТОК НА ВАШЕМ ДРАЙВЕРЕ 12ГБ RAM
???
<h2 style="color #460073">ПРИМЕР ТЮНИНГА ПАРАМЕТРОВ СЕССИИ</h2>
???
spark.stop()

from pyspark.sql import SparkSession
import pyspark.pandas as ps
import pandas as pd

spark = (
SparkSession.builder
.master('yarn') \
.appName('meetup_dapp_static_spark_job') \
.config('spark.deployMode', 'client') \
.config('spark.default.parallelism', '140') \
.config('spark.sql.adaptive.enabled','True') \
.config('spark.sql.adaptive.forceApply','False') \
.config('spark.sql.adaptive.logLevel','info') \
.config('spark.sql.adaptive.advisoryPartitionSizeInBytes','256m') \
.config('spark.sql.adaptive.coalescePartitions.enabled','True') \
.config('spark.sql.adaptive.coalescePartitions.minPartitionNum','1') \
.config('spark.sql.adaptive.coalescePartitions.initialPartitionNum','8192') \
.config('spark.sql.adaptive.fetchShuffleBlocksInBatch','True') \
.config('spark.sql.adaptive.localShuffleReader.enabled','True') \
.config('spark.sql.adaptive.skewJoin.enabled','True') \
.config('spark.sql.adaptive.skewJoin.skewedPartitionFactor','5') \
.config('spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes','400m') \
.config('spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin','0.2') \
.config('spark.sql.autoBroadcastJoinThreshold','-1') \
.config('spark.sql.optimizer.dynamicPartitionPruning.enabled','True') \
.config('spark.sql.cbo.enabled','True') \
.config('spark.sql.execution.arrow.pyspark.enabled','True') \
.config('spark.scheduler.mode','FAIR') \
.config('spark.scheduler.pool','default') \
.config('spark.scheduler.allocation.file','file:///home/vtbвашаучетка/fairscheduler.xml') \
.getOrCreate()
       )
spark.sparkContext.setLogLevel('OFF')
???
<h2 style="color #460073">fairscheduler.xml</h2>
???
<?xml version="1.0"?>
<allocations>
  <pool name="default">
    <schedulingMode>FAIR</schedulingMode>
    <weight>1</weight>
    <minShare>2</minShare>
  </pool>
</allocations>
???
<h2 style="color #460073">СТРАТЕГИИ ВЗАИМОДЕЙСТВИЯ PANDAS И SPARK</h2>
???
<h4 style="color #460073">Читаем данные через spark df</h4>
???
spark_df = spark.sql("select * from mytable")
???
<h4 style="color #460073">Конвертим в pyspark-pandas.df для обработки данных</h4>
???
pyspark_pandas_df = spark_df.to_pandas_on_spark()
type(pyspark_pandas_df)
???
<h4 style="color #460073">Конвертим обработанный в pyspark-pandas.df в spark df для записи данных)</h4>
???
spark_df = pyspark_pandas_df.to_spark()
type(spark_df)
???
<h4 style="color #460073">SUPPORTED PANDAS API</h4>
???
ГУГЛИ - PYSPARK 3.3.2 PYSPARK-PANDAS SUPPORTED PANDAS API
https://spark.apache.org/docs/3.3.2/api/python/user_guide/pandas_on_spark/index.html
???
<h4 style="color #460073">API PANDAS В SPARK</h4>
???
ГУГЛИ - PYSPARK 3.3.2 API PANDAS ON SPARK
https://spark.apache.org/docs/3.3.2/api/python/reference/paspark.pandas/index.html
???
<h2 style="color #460073">КАК ОПРЕДЕЛИТЬ РАЗМЕР SPARK JOB STATIC</h2>
???
# num-executors*executor-cores=общее число потоков
# размер данных/общее число потоков=Xgb меньше executor-memory

# Пример
# num-executors=5*executor-cores=5=25 общее число потоков
# Размер данных=300гб/общее число потоков=25=12GB < executor-memory=16G - ок

