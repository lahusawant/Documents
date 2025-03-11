# Databricks notebook source
# MAGIC %md
# MAGIC ## DATA LOADING AND READING

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df_csv_1 = spark\
            .read\
            .format('csv')\
            .option('inferSchema',True)\
            .option('header', True)\
            .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df_csv_1.display()

# COMMAND ----------

df_csv_2 =  spark\
            .read\
            .csv('/FileStore/tables/BigMart_Sales.csv',inferSchema=True,header=True)

# COMMAND ----------

df_csv_2.display()

# COMMAND ----------

df_json_1 = spark\
            .read\
            .format("json")\
            .option("inferSchema",True)\
            .option("header",True)\
            .option("multiLine",False)\
            .load("/FileStore/tables/drivers-1.json")

# COMMAND ----------

df_json_1.display()

# COMMAND ----------

df_json_2 = spark\
            .read\
            .json("/FileStore/tables/drivers-1.json")

# COMMAND ----------

df_json_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema defination

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL schema

# COMMAND ----------

ddl_schema = '''
                Item_Identifier  STRING ,
                Item_Weight  STRING ,
                Item_Fat_Content  STRING ,
                Item_Visibility  DOUBLE ,
                Item_Type  STRING ,
                Item_MRP  DOUBLE ,
                Outlet_Identifier  STRING ,
                Outlet_Establishment_Year  INT ,
                Outlet_Size  STRING ,
                Outlet_Location_Type  STRING ,
                Outlet_Type  STRING ,
                Item_Outlet_Sales  DOUBLE 
            '''

# COMMAND ----------

df_schema_1 = spark\
                .read\
                .format("csv")\
                .schema(ddl_schema)\
                .option("header",True)\
                .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

df_schema_1.display()

# COMMAND ----------

df_schema_1.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

struct_schema = StructType([
                            StructField('Item_Identifier',StringType(),True),
                            StructField('Item_Weight',StringType(),True),
                            StructField('Item_Fat_Content',StringType(),True),
                            StructField('Item_Visibility',StringType(),True),
                            StructField('Item_Type',StringType(),True),
                            StructField('Item_MRP',StringType(),True),
                            StructField('Outlet_Identifier',StringType(),True),
                            StructField('Outlet_Establishment_Year',StringType(),True),
                            StructField('Outlet_Size',StringType(),True),
                            StructField('Outlet_Location_Type',StringType(),True),
                            StructField('Outlet_Type',StringType(),True),
                            StructField('Item_Outlet_Sales',StringType(),True)
                        ])

# COMMAND ----------

df_schema_2 = spark\
                .read\
                .format("csv")\
                .option("header",True)\
                .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

df_schema_2.display()

# COMMAND ----------

df_schema_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select

# COMMAND ----------

df_schema_1.select("Item_Identifier",\
                    "Item_Weight",\
                    "Item_Fat_Content")\
            .display()

# COMMAND ----------

df_schema_1.select(col("Item_Identifier"),\
                    col("Item_Weight"),\
                    col("Item_Fat_Content"))\
            .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Alias

# COMMAND ----------

df_schema_1.select(col("Item_Identifier")\
            .alias("Item_ID"))\
            .display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Filter

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 1

# COMMAND ----------

df_schema_1.filter(col("Item_Fat_Content")=="Regular").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 2

# COMMAND ----------

df_schema_1.filter(\
                    (col("Item_Type") == "Soft Drinks") & (col("Item_Weight") < 10)\
                    ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 3

# COMMAND ----------

df_schema_1.filter(\
                (col("Outlet_Location_Type").isin("Tier 1","Tier 2"))\
                    & (col("Outlet_Size").isNull()))\
            .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### withColumnRenamed

# COMMAND ----------

df_schema_1.withColumnRenamed("Item_Weight","Item_WT").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 1

# COMMAND ----------

df_schema_1.withColumn("flag",lit("new")).display()

# COMMAND ----------

df_schema_1.withColumn("Multiple",col("Item_weight")*col("Item_MRP")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Example 2

# COMMAND ----------

df_schema_1.withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"),"Regular","Reg"))\
            .withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"),"Low Fat","LF"))\
            .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Type Casting

# COMMAND ----------

df_type_cast = df_csv_1.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))
df_type_cast.printSchema() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### sort / orderBy

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1

# COMMAND ----------

df_csv_1.sort(col("Item_Weight").desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2

# COMMAND ----------

df_csv_1.sort(col("Item_Visibility").asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3

# COMMAND ----------

df_csv_1.sort(["Item_Weight","Item_Visibility"],ascending = [0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 4

# COMMAND ----------

df_csv_1.sort(['Item_weight','Item_Visibility'], ascending = [0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 5

# COMMAND ----------

df_csv_1.sort(['Item_MRP'], ascending = [1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limit

# COMMAND ----------

df_csv_1.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DROP

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1

# COMMAND ----------

df_csv_1.drop("Item_Visibility").limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2

# COMMAND ----------

df_csv_1.drop("Item_Visibiliy","Item_Type").limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DROP_DUPLICATES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1

# COMMAND ----------

df_csv_1.dropDuplicates().display() # DeDups

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2

# COMMAND ----------

df_csv_1.dropDuplicates(subset=["Item_Type"]).display()

# COMMAND ----------

df_csv_1.distinct().display()

# COMMAND ----------


