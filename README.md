# 📘 PySpark Basic Commands

---

## 🔹 1. Initialize SparkSession

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataEngineerApp") \
    .getOrCreate()
```

---

## 🔹 2. Read Data from Various Formats

```python
# CSV
df = spark.read.option("header", True).csv("/path/to/file.csv")

# JSON
df = spark.read.json("/path/to/file.json")

# Parquet
df = spark.read.parquet("/path/to/file.parquet")

# Delta (Databricks)
df = spark.read.format("delta").load("/path/to/delta")
```

---

## 🔹 3. Write Data to Storage

```python
# Parquet
df.write.mode("overwrite").parquet("/output/path")

# Delta
df.write.format("delta").mode("overwrite").save("/output/path")

# CSV
df.write.option("header", True).csv("/output/path")
```

---

## 🔹 4. Create DataFrame from Python Collection

```python
data = [("Alice", 30), ("Bob", 25)]
df = spark.createDataFrame(data, ["name", "age"])
```

---

## 🔹 5. Data Inspection Commands

```python
df.show()                 # View top 20 records
df.printSchema()          # Print schema
df.columns                # List of columns
df.describe().show()      # Summary statistics
```

---

## 🔹 6. Select, Filter, and Conditional Operations

```python
from pyspark.sql.functions import col

df.select("name", "age").show()

df.filter(df.age > 30).show()

df.where("salary > 50000").show()

df.selectExpr("name", "age + 5 as age_plus_5").show()
```

---

## 🔹 7. Add, Drop, Rename Columns

```python
df = df.withColumn("bonus", col("salary") * 0.10)

df = df.drop("old_column")

df = df.withColumnRenamed("old_name", "new_name")
```

---

## 🔹 8. Handling Null Values

```python
df.na.drop()  # Drop rows with nulls

df.na.fill({"age": 0, "salary": 1000})  # Fill nulls with default values
```

---

## 🔹 9. Sorting & Ordering

```python
df.orderBy("age").show()

df.orderBy(col("salary").desc()).show()
```

---

## 🔹 10. Type Casting Columns

```python
df = df.withColumn("age", col("age").cast("int"))
```

---

## 🔹 11. GroupBy and Aggregation

```python
df.groupBy("department").count().show()

df.groupBy("dept").agg({"salary": "avg"}).show()

from pyspark.sql.functions import avg, sum

df.groupBy("dept").agg(
    avg("salary").alias("avg_salary"),
    sum("salary").alias("total_salary")
).show()
```

---

## 🔹 12. Joins

```python
df1.join(df2, df1.id == df2.id, "inner").show()

df1.join(df2, ["id"], "left").show()
```

---

## 🔹 13. Union and Distinct

```python
df.union(df2).show()

df.select("name").distinct().show()
```

---

## 🔹 14. Window Functions (Basic)

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("dept").orderBy("salary")

df.withColumn("row_num", row_number().over(windowSpec)).show()
```

---

## 🔹 15. UDFs (User Defined Functions)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def to_upper(name):
    return name.upper()

upper_udf = udf(to_upper, StringType())

df = df.withColumn("upper_name", upper_udf(col("name")))
```

---

## 🔹 16. Using SQL in PySpark

```python
df.createOrReplaceTempView("employees")

result = spark.sql("SELECT dept, COUNT(*) FROM employees GROUP BY dept")
result.show()
```

---

## 🔹 17. Save and Load Tables (Databricks)

```python
# Save as Delta Table
df.write.format("delta").saveAsTable("my_table")

# Read from Table
df = spark.read.table("my_table")
```

---

## 🔹 18. Read from JDBC Source

```python
jdbc_url = "jdbc:mysql://host:port/database"
properties = {"user": "user", "password": "password"}

df = spark.read.jdbc(jdbc_url, "table_name", properties=properties)
```

---

## 🔹 19. Schema Inspection

```python
for field in df.schema.fields:
    print(f"{field.name}: {field.dataType}")
```

---

## 🔹 20. Basic File Commands (Databricks)

```python
# List files in DBFS
dbutils.fs.ls("/mnt/your-folder")

# Read text file
df = spark.read.text("/path/to/file.txt")
```
