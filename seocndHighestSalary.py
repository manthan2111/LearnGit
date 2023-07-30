from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import col,row_number,rank , dense_rank
from pyspark.sql.window import *
from pyspark.sql.types import *
from pyspark.sql.column import *

my_conf = SparkConf()
my_conf.set("spark.app.name","secondHighestSalary")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf = my_conf).getOrCreate()

my_list = [ (1, 100),(2, 200),(3, 300) ]

df1 = spark.createDataFrame(my_list).toDF("id","salary")

df1.createOrReplaceTempView("emp")

spark.sql("""with my_cte as
(
select salary, dense_rank() over(order by salary desc) as rn
 from emp
 )
select ifnull ((select distinct salary   from my_cte 
where rn= 2 ),null) as secondhighestsalary""").show()

my_window = Window.orderBy(col("salary").desc())

df1.withColumn("rn", dense_rank().over(my_window)).filter(col("rn")==2)\
.select(col("salary").alias("2nd_highest_salary")).show()
