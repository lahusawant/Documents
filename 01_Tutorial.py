# Databricks notebook source
# MAGIC %md
# MAGIC ## DATA LOADING AND READING

# COMMAND ----------

df_csv_1 = spark\
            .read\
            .format('csv')\
            .option('inferSchema',True)\
            .option('header', True)\
            .load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df_csv_1.limit(10).display()

# COMMAND ----------

df_csv_2 =  spark\
            .read\
            .csv('/FileStore/tables/BigMart_Sales.csv',inferSchema=True,header=True)

# COMMAND ----------

df_csv_2.limit(10).display()

# COMMAND ----------

df_json_1 = spark\
            .read\
            .format("json")\
            .option("inferSchema",True)\
            .option("header",True)\
            .option("multiLine",False)\
            .load("/FileStore/tables/drivers-1.json")

# COMMAND ----------

df_json_1.limit(10).display()

# COMMAND ----------

df_json_2 = spark\
            .read\
            .json("/FileStore/tables/drivers-1.json")

# COMMAND ----------

df_json_2.limit(10).display()

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

df_schema_1.limit(10).display()

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

df_schema_2.limit(10).display()

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
                .limit(10)\
                .display()

# COMMAND ----------

df_schema_1.select(col("Item_Identifier"),\
                    col("Item_Weight"),\
                    col("Item_Fat_Content"))\
                .limit(10)\
                .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Alias

# COMMAND ----------

df_schema_1.select(col("Item_Identifier")\
            .alias("Item_ID"))\
                .limit(10)\
            .display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Filter

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 1

# COMMAND ----------

df_schema_1.filter(col("Item_Fat_Content")=="Regular").limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 2

# COMMAND ----------

df_schema_1.filter(\
                    (col("Item_Type") == "Soft Drinks") & (col("Item_Weight") < 10)\
                    ).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 3

# COMMAND ----------

df_schema_1.filter(\
                (col("Outlet_Location_Type").isin("Tier 1","Tier 2"))\
                    & (col("Outlet_Size").isNull()))\
            .limit(10)\
            .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### withColumnRenamed

# COMMAND ----------

df_schema_1.withColumnRenamed("Item_Weight","Item_WT").limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 1

# COMMAND ----------

df_schema_1.withColumn("flag",lit("new")).limit(10).display()

# COMMAND ----------

df_schema_1.withColumn("Multiple",col("Item_weight")*col("Item_MRP")).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Example 2

# COMMAND ----------

df_schema_1.withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"),"Regular","Reg"))\
            .withColumn("Item_Fat_Content",regexp_replace(col("Item_Fat_Content"),"Low Fat","LF"))\
            .limit(10)\
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

df_csv_1.sort(col("Item_Weight")\
        .desc())\
        .limit(10)\
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2

# COMMAND ----------

df_csv_1.sort(col("Item_Visibility")
        .asc())\
        .limit(10)\
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3

# COMMAND ----------

df_csv_1.sort(["Item_Weight","Item_Visibility"],ascending = [0,0])\
        .limit(10)\
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 4

# COMMAND ----------

df_csv_1.sort(['Item_weight','Item_Visibility'], ascending = [0,1])\
    .limit(10)\
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 5

# COMMAND ----------

df_csv_1.sort(['Item_MRP'], ascending = [1])\
    .limit(10)\
    .display()

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

df_csv_1.dropDuplicates().limit(10).display() # DeDups

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2

# COMMAND ----------

df_csv_1.dropDuplicates(subset=["Item_Type"]).limit(10).display()

# COMMAND ----------

df_csv_1.distinct().limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## UNION & UNION BY NAME

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing DataFrames

# COMMAND ----------

data1 = [('1','kad'),('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

data3 = [('kad','1'),('sid','2')]
schema3 = 'name STRING, id STRING' 

df3 = spark.createDataFrame(data3,schema3)


# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Union

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### UNION BY NAME

# COMMAND ----------

df3.union(df2).display()

# COMMAND ----------

df3.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## String Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### initcap(), upper(), lower()

# COMMAND ----------

df_csv_1\
    .select(\
        (initcap('Item_Type').alias("initcap")),\
        (upper('Item_Type').alias('upper')),\
        (lower('Item_Type').alias('lower'))\
    )\
    .limit(10)\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Function

# COMMAND ----------

# MAGIC %md
# MAGIC #### current_date()

# COMMAND ----------

df_csv_1 = df_csv_1.withColumn('curr_date',current_date())

# COMMAND ----------

df_csv_1.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### date_add()

# COMMAND ----------

df_csv_1 = df_csv_1.withColumn('week_after',date_add('curr_date',7))

# COMMAND ----------

df_csv_1.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### date_sub()
# MAGIC #### method - 1 

# COMMAND ----------

df_csv_1 = df_csv_1.withColumn('week_before_method1',date_sub('curr_date',7))

# COMMAND ----------

df_csv_1.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### date_sub()
# MAGIC #### method - 2

# COMMAND ----------

df_csv_1 = df_csv_1.withColumn('week_before_method2',date_add('curr_date',-7))

# COMMAND ----------

df_csv_1.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### datediff()

# COMMAND ----------

df_csv_1 = df_csv_1.withColumn('datediff',datediff('week_after','curr_date'))

# COMMAND ----------

df_csv_1.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### date_format()

# COMMAND ----------

df_csv_1 = df_csv_1.withColumn('dateformat',date_format('week_before_method2','dd-MM-yyyy'))

# COMMAND ----------

df_csv_1.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to handle null?
# MAGIC #### Option 1 - Dropping null
# MAGIC #### Option 2 - Filling null

# COMMAND ----------

df_csv_1.select('Outlet_Size').distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dropping nulls

# COMMAND ----------

df_csv_1.dropna('all')\
        .limit(10)\
        .display()

# COMMAND ----------

df_csv_1.dropna('any')\
        .limit(10)\
        .display()

# COMMAND ----------

df_csv_1.dropna(subset=['Outlet_Size'])\
        .limit(10)\
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### filling nulls

# COMMAND ----------

df_csv_1.fillna('NotAvailable')\
        .limit(10)\
        .display()

# COMMAND ----------

df_csv_1.fillna('NotAvailable',subset=['Outlet_Size'])\
        .limit(10)\
        .display()

# COMMAND ----------


