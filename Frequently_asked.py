from audioop import avg

import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import dense_rank
import pyspark.sql.functions as F


my_conf = SparkConf()
my_conf.set("spark.app.name","frequently ask questions")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

data = [(1, "John", 30, "Sales", 50000.0),
 (2, "Alice", 28, "Marketing", 60000.0),
 (3, "Bob", 32, "Finance", 55000.0),
 (4, "Sarah", 29, "Sales", 52000.0),
 (5, "Mike", 31, "Finance", 58000.0)
]

schema = StructType([
    StructField("id",IntegerType(),nullable=False),
    StructField("name",StringType(),nullable=False),
    StructField("age",IntegerType(),nullable=False),
    StructField("department",StringType(),nullable=False),
    StructField("salary",DoubleType(),nullable=False)
])
# 1st find average salary of each department

employeeDf = spark.createDataFrame(data,schema)

avgsalaryDf = employeeDf.groupBy("department").agg(avg("salary").alias("average_salary"))
#avgsalaryDf.show()

#2nd Add a new column named "bonus" that is 10% of the salary for all employees.

bonusDf = employeeDf.withColumn("bonus",expr("salary * 0.1 as bonus"))
#bonusDf.show()

#Question 3: Group the data by department and find the employee with the highest salary in each department

my_window = Window.partitionBy("department").orderBy(F.col("salary").desc())

highest_salary_df = employeeDf.withColumn("salary_rank",dense_rank().over(my_window)).where(F.col("salary_rank")==1)
#highest_salary_df.show()

#Question 4: Find the top 3 departments with the highest total salary

#column expression
totalSalaryByDep_Df = employeeDf.groupBy("department").agg(expr("sum(salary) as totalSalaryByDept"))\
    .orderBy("totalSalaryByDept")
#column string  expression
totalSalaryByDep_Df = employeeDf.groupBy("department").agg(sum("salary").alias("totalSalaryByDept"))\
    .orderBy("totalSalaryByDept")
#totalSalaryByDep_Df.show()

#Question5: Find the topmost department having highest salary

df_cte = employeeDf.groupBy("department").agg(sum("salary").alias("totalSalaryByDept"))

new_window = Window.orderBy(df_cte["totalSalaryByDept"].desc())

highestTotalSalaryDept_df  =df_cte.withColumn("rank",dense_rank().over(new_window)).filter(F.col("rank")==1)\
    .select("department","totalSalaryByDept")

# Question 6: Filter the DataFrame to keep only employees aged 30 or above and working in the "Sales" department

result_df = employeeDf.filter( (F.col("age")>=30 ) & ( F.col("department") =='Sales') )

#Question 7: Calculate the difference between each employee's salary and the average salary of their respective department

window1 = Window.partitionBy("department")

difference_df = employeeDf.withColumn("averageSalary", avg("salary").over(window1))\
    .withColumn("diff",expr("averageSalary - salary"))\
#    .show()


#8. Calculate the sum of salaries for employees whose names start with the letter "J".

sumJ_df = employeeDf.filter(F.col("name").startswith("J")).agg(sum("salary"))

# 9. Sort the DataFrame based on the "age" column in ascending order and
# then by "salary" column in descending order

sorted_df = employeeDf.sort(F.col("age"),F.col("salary").desc())

#0. Replace the department name "Finance" with "Financial Services" in  the DataFrame:

employeeDf\
    .withColumn("department", when(F.col("department") == "Finance","Financial_services").otherwise(F.col("department")))

employeeDf.withColumn("department",regexp_replace("department","Finance","Financial_services"))

#11. Calculate the percentage of total salary each employee contributes to their respective department.

window2 = Window.partitionBy("department")

employeeDf.withColumn("total_salary",sum("salary").over(window2))\
    .withColumn("percentage",expr("salary/total_salary*100"))\
    .show()











