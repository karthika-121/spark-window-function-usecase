import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import dense_rank, isnotnull
from pyspark.sql.window import Window
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"  # <----- 🔴JAVA PATH🔴
os.environ["HADOOP_HOME"] = r"C:\hadoop"
spark = SparkSession.builder.appName("SecondHighestSalary").getOrCreate()
def inspect_df(df, name):
    print("\n" + "=" * 70)
    print(f"DATAFRAME: {name}")
    print("=" * 70)
    print(f"Rows: {df.count()}")
    df.show( truncate=False)

data = [

    (1, "Alice", "Engineering", 120000),
    (2, "Hannah", "Sales", None),
    (3, "Charlie", "Engineering", 100000),
    (4, "Mia", "Marketing", 75000),
    (5, "Evan", "Sales", 95000),
    (6, "Fiona", "Sales", 80000),
    (7, "Diana", "Engineering", 85000),
    (8, "George", "Sales", 80000),
    (9, "Kevin", "Marketing", 110000),
    (10, "Ian", "HR", 70000),
    (11, "Bob", "Engineering", 120000),
    (12, "Julia", "HR", 70000),
    (13, "Liam", "Marketing", 90000),

]

columns = ["emp_id", "name", "department", "salary"]
df=spark.createDataFrame(data, columns)
df.show()

department_window =Window.partitionBy("department").orderBy(F.col("salary").desc())
department_window_asc =Window.partitionBy("department").orderBy(F.col("salary").asc())
base_df=df.filter( F.col("salary").isNotNull ())
#   #   2ND HIGHEST_salary FROM EVERY DEPT
second_salary=(base_df
               .withColumn("rank",F.dense_rank().over(department_window))
               .filter(F.col("rank")==2)
               .drop("rank")
               )
inspect_df(second_salary, "SECOND HIGHEST SALARY")
#   HIGHEST_salary FROM EVERY DEPT
highest_salary=(base_df
                .withColumn("rank",F.dense_rank().over(department_window))
                .filter(F.col("rank")==1)
                .drop("rank")
                )

inspect_df(highest_salary, "HIGHEST SALARY")
#   lowest_salary FROM EVERY DEPT
lowest_salary=(base_df
               .withColumn("rank",F.dense_rank().over(department_window_asc))
               .filter(F.col("rank")==1)
               .drop("rank")
               )
inspect_df(lowest_salary, "LOWEST SALARY")
#   Highest paying department
Highest_paying_department= (base_df
                            .groupBy("department")
                            .agg(F.max("salary").alias ("max_salary") )
                            .orderBy(F.col("department").desc())
                            )
inspect_df(Highest_paying_department, "Highest paying department")
Highest_paying_department.limit(1).show()
#   LOWEST PAYING department
lowest_paying_department= (base_df
                           .groupBy("department")
                           .agg(F.min("salary").alias ("min_salary") )
                           .orderBy(F.col("department").asc())
                           )
inspect_df(lowest_paying_department, "LOWEST PAYING DEPT")

#   Department with highest average salary
average_salary=base_df.groupBy("department").agg(F.avg("salary").alias("avg_salary"))
inspect_df(average_salary, "Department with highest average salary")