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

df1.display()

data2 = [('3','rahul'),('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

df2.display()

data3 = [('kad','1'),('sid','2')]
schema3 = 'name STRING, id STRING' 

df3 = spark.createDataFrame(data3,schema3)

df3.display()


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

df_csv_1.select('Outlet_Size')\
        .dropna(subset=['Outlet_Size'])\
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

df_csv_1.select('Outlet_Size')\
        .fillna('NotAvailable',subset=['Outlet_Size'])\
        .limit(10)\
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SPLIT and Indexing

# COMMAND ----------

df_csv_1.select('Outlet_Type')\
        .withColumn('Outlet_Type',split('Outlet_Type',' ')[1])\
        .limit(10)\
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode

# COMMAND ----------


split_df = df_csv_1.withColumn('Outlet_Type',split('Outlet_Type',' '))

split_df.select('Outlet_Type').limit(20).display()

# COMMAND ----------

explode_df = split_df.withColumn('Outlet_Type',explode('Outlet_Type'))

explode_df.select('Outlet_Type').limit(40).display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### array_contains()

# COMMAND ----------

array_contains_df = split_df.withColumn('Type1_flag',array_contains('Outlet_Type','Type1'))

array_contains_df.select('Type1_flag').limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## groupBy()

# COMMAND ----------

df_csv_1.groupBy('Item_Type')\
        .agg(sum('Item_MRP'))\
    .display()

# COMMAND ----------

df_csv_1.groupBy('Item_Type')\
        .agg(avg('Item_MRP'))\
    .display()

# COMMAND ----------

df_csv_1.groupBy('Item_Type','Outlet_Size')\
        .agg(sum('Item_MRP'))\
        .alias('Total_MRP')\
        .display()

# COMMAND ----------

df_csv_1.groupBy('Item_Type','Outlet_Size')\
        .agg(\
            sum('Item_MRP').alias('Total_MRP'),\
            avg('Item_MRP').alias('Avg_MRP')\
        )\
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## collect_list()

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

df_book.groupBy('user')\
        .agg(collect_list('book'))\
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## pivot

# COMMAND ----------

df_csv_1.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## when-otherwise

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 1 - if meat then non-veg otherwise veg 

# COMMAND ----------

veg_flg_df = df_csv_1.withColumn('veg_flag',when(col('Item_Type')=='Meat','Non-Veg').otherwise('Veg'))

veg_flg_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Example 2 - ItemType - veg and MRP < 100 then inexpensive otherwise expensive

# COMMAND ----------

veg_flg_df.withColumn('veg_exp_flg',\
                when((col('veg_flag')=='Veg') & (col('Item_MRP')<100),'Veg_Inexpensive')\
                .when((col('veg_flag')=='Veg') & (col('Item_MRP')>100),'Veg_Exxpensive')\
            .otherwise('Non-Veg')\
                )\
            .limit(50)\
            .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## joins 

# COMMAND ----------

dataj1 = [('1','gaur','d01'), ('2','kit','d02'), ('3','sam','d03'), ('4','tim','d03'), ('5','aman','d05'), ('6','nad','d06')]

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING'

df1 = spark.createDataFrame(dataj1,schemaj1)

df1.display()

dataj2 = [('d01','HR'), ('d02','Marketing'), ('d03','Accounts'), ('d04','IT'), ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### inner

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### left

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### right

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### anti

# COMMAND ----------

df1.join(df2,\
    df1['dept_id']==df2['dept_id'],\
        'anti')\
            .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## window function

# COMMAND ----------

# MAGIC %md
# MAGIC #### row_number
# MAGIC #### rank
# MAGIC #### dense_rank

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df_csv_1.select(col('Item_Identifier'))\
            .withColumn('rowNum',row_number().over(Window.orderBy(col('Item_Identifier').desc())))\
            .withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
            .withColumn('dense_rank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc())))\
            .limit(50).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### cumulative sum

# COMMAND ----------

df_csv_1.select('Item_Type','Item_MRP')\
    .withColumn('cum_sum',sum('Item_MRP')\
        .over(Window.partitionBy('Item_Type').orderBy('Item_MRP')))\
    .withColumn('cum_sum_range_preceding',sum('Item_MRP')\
        .over(Window.partitionBy('Item_Type').orderBy('Item_MRP')\
            .rowsBetween(Window.unboundedPreceding,Window.currentRow)))\
    .withColumn('cum_sum_range_following',sum('Item_MRP')\
        .over(Window.orderBy('Item_MRP').partitionBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)))\
    .limit(50).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## user defined functions udf

# COMMAND ----------

def my_func(x):
  return x*x

# COMMAND ----------

square_function = udf(my_func)

# COMMAND ----------

df_csv_1.select('Item_MRP').withColumn('sqtFun',square_function('Item_MRP')).limit(50).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## data writting

# COMMAND ----------

# MAGIC %md
# MAGIC #### csv comma separated value

# COMMAND ----------

df_csv_1.write.format('csv').save('/FileStore/tables/CSV.data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC # data writting modes

# COMMAND ----------

# MAGIC %md
# MAGIC #### append

# COMMAND ----------

df_csv_1.write.format('csv').mode('append').save('/FileStore/tables/CSV.data.csv')

df_csv_1.write.format('csv').mode('append').option('path','/FileStore/tables/CSV.data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### overwrite

# COMMAND ----------

df_csv_1.write.format('csv').mode('overwrite').save('/FileStore/tables/CSV.data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### error

# COMMAND ----------

df_csv_1.write.format('csv').mode('error').save('/FileStore/tables/CSV.data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### ignore

# COMMAND ----------

df_csv_1.write.format('csv').mode('ignore').save('/FileStore/tables/CSV.data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### parquet

# COMMAND ----------

df_csv_1.write.format('parquet').mode('overwrite').option('path','/FileStore/tables/CSV.data.csv').save()

# COMMAND ----------

#### datalake file format - explore

# COMMAND ----------

# MAGIC %md
# MAGIC ## table

# COMMAND ----------

df_csv_1.write.format('parquet').mode('overwrite').saveAsTable('my_table')

# COMMAND ----------

# MAGIC %md
# MAGIC ## spark sql

# COMMAND ----------

# MAGIC %md
# MAGIC #### create temp view 

# COMMAND ----------

df_csv_1.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_Fat_Content = 'LF'

# COMMAND ----------

df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'LF'")

# COMMAND ----------

df_sql.display()

# COMMAND ----------


