# COMMAND ----------

#display(dbutils.fs.ls(f"/mnt/s3data"))
dbutils.fs.unmount("/mnt/s3data")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/airbnb_la_data/"))
#dbutils.fs.rm('/FileStore/airbnb_la_data/reviews_fixed.csv', True)
#dbutils.fs.mv("/mnt/s3data/reviews.csv", "/FileStore/airbnb_la_data/reviews.csv")

# COMMAND ----------

file_location = '/FileStore/airbnb_la_data/listings.csv'
file_type = 'csv'

infer_schema = 'true'
first_row_is_header = 'true'
delimiter = ","

df_listing = spark.read.format(file_type) \
            .option("inferSchema",infer_schema) \
            .option("header",first_row_is_header) \
            .option("sep",delimiter) \
            .option("quote","\"") \
            .option("escape","\"") \
            .option("multiline","true") \
            .load(file_location)

display(df_listing)

#hive metastore wont be deleted by default so below command is used
#spark.conf.set("spark.sql.legacy.allowCreaingManagedTableUsingNonemptyLocation","true")

#listing_table_name = "listing"

#df_listing.write.format("parquet").saveAsTable(listing_table_name)
#creating permanent vs temp tables, can we also do that in df?
#different formats to use sql in pyspark

#check why table is not getting created in each case

# COMMAND ----------

#Creating temp listing table
listing_table_name = "listing"
#df_listing.write.format("parquet").saveAsTable(listing_table_name)
df_listing.createOrReplaceTempView(listing_table_name)


# COMMAND ----------

from pyspark.sql.types import *
file_location = "/FileStore/airbnb_la_data/neighbourhoods.csv"
file_type = 'csv'

infer_schema = 'false'
first_row_is_header = 'true'
delimiter = ","

neighbourhoodSchema = StructType([
    StructField("neighbourhood_group", StringType(),True),
    StructField("neighbourhood", StringType(),True)
])

df_neighbourhoods = spark.read.format(file_type) \
            .option("inferSchema",infer_schema) \
            .option("header",first_row_is_header) \
            .option("sep",delimiter) \
            .option("quote","\"") \
            .option("escape","\"") \
            .option("multiline","true") \
            .load(file_location)

display(df_neighbourhoods)

#spark.sql("""drop table neighbourhoods""")
#hive metastore wont be deleted by default so below command is used
#spark.conf.set("spark.sql.legacy.allowCreaingManagedTableUsingNonemptyLocation","true")

#listing_table_name = "neighbourhoods"

#df_neighbourhoods.write.format("parquet").saveAsTable(listing_table_name)
#creating permanent vs temp tables, can we also do that in df?
#different formats to use sql in pyspark

# COMMAND ----------

neighbourhood_table_name = "neighbourhood"
#df_listing.write.format("parquet").saveAsTable(listing_table_name)
df_neighbourhoods.createOrReplaceTempView(neighbourhood_table_name)

# COMMAND ----------

file_location = '/FileStore/airbnb_la_data/reviews.csv'
file_type = 'csv'

infer_schema = 'true'
first_row_is_header = 'true'
delimiter = ","

df_review = spark.read.format(file_type) \
            .option("inferSchema",infer_schema) \
            .option("header",first_row_is_header) \
            .option("sep",delimiter) \
            .option("quote","\"") \
            .option("escape","\"") \
            .option("multiline","true") \
            .load(file_location)

display(df_review)

#hive metastore wont be deleted by default so below command is used
#spark.conf.set("spark.sql.legacy.allowCreaingManagedTableUsingNonemptyLocation","true")

#listing_table_name = "review"

#df_review.write.format("parquet").saveAsTable(listing_table_name)
#creating permanent vs temp tables, can we also do that in df?
#different formats to use sql in pyspark

# COMMAND ----------

review_table_name = "reviews"
#df_listing.write.format("parquet").saveAsTable(listing_table_name)
df_review.createOrReplaceTempView(review_table_name)

# COMMAND ----------

df_listing.printSchema()
df_neighbourhoods.printSchema()
df_listing.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from neighbourhoods;
# MAGIC select * from listing;
# MAGIC select * from review;

# COMMAND ----------

#what if csv has multiple returns? #not required
temp_df = spark.read.text("/FileStore/airbnb_la_data/reviews.csv")
header = temp_df.head(1)

#filtering records that has /20 basically which has that year mentioned
fixed_df = temp_df.filter(temp_df.value.contains("/2020"))

#display(fixed_df)
fixed_df.count()

# COMMAND ----------

#add header and fixed df using union #not required
final_df=spark.createDataFrame(header).union(fixed_df)
final_df.repartition(1).write.text("/FileStore/airbnb_la_data/reviews_fixed")
#this will store in s3 as a single file and wont create multiple partitions of file, it written using map reduce(check how file is written)

# COMMAND ----------

#not required
file_location = '/FileStore/airbnb_la_data/reviews.csv'
file_type = 'csv'

infer_schema = 'true'
first_row_is_header = 'true'
delimiter = ","

df_review = spark.read.format(file_type) \
            .option("inferSchema",infer_schema) \
            .option("header",first_row_is_header) \
            .option("sep",delimiter) \
            .option("quote","\"") \
            .option("escape","\"") \
            .option("multiline","true") \
            .load(file_location)

display(df_review)

#hive metastore wont be deleted by default so below command is used
spark.conf.set("spark.sql.legacy.allowCreaingManagedTableUsingNonemptyLocation","true")

listing_table_name = "review"

df_review.write.format("parquet").saveAsTable(listing_table_name)
#creating permanent vs temp tables, can we also do that in df?
#different formats to use sql in pyspark

# COMMAND ----------

#1 What property types are available in airbnb listing and distribution of those?
from pyspark.sql.functions import *
#display(df_listing.groupBy('property_type').count())
df_listing.groupBy('property_type').count().orderBy(col('count').desc()).show()

# COMMAND ----------

#2 Average price across each property type
#Wont work because price column has $
df_listing.groupBy("property_type","price").agg(({"price":"average"})).show()

# COMMAND ----------

from pyspark.sql.types import FloatType

def trim_char(string):
    return string.strip('$')

spark.udf.register("trim_func",trim_char) #use in spark sql
trim_func_udf=udf(trim_char) #use in dataframe

# COMMAND ----------

#3
from pyspark.sql.functions import sum,avg,max,col
#df_listing.select("property_type","price",trim_func_udf("price")).show()
#df_listing.select("property_type",trim_func_udf("price").cast(FloatType()).alias("price_f")).groupBy("property_type").agg(({"price_f":"average"})).orderBy("property_type").show()
df_listing.select("property_type",trim_func_udf("price").cast(FloatType()).alias("price_f")).groupBy("property_type").agg(avg("price_f").alias("Test")).orderBy("property_type").show()

# COMMAND ----------

df_listing.where(col("property_type") == "Barn").select("price").show()

# COMMAND ----------

#4. Percentage of Listings in Neigbourhood
import pyspark.sql.functions as f
from pyspark.sql.window import Window

df_neigh_count = df_listing.groupBy("neighbourhood").agg(({"neighbourhood":"count"})).withColumnRenamed("count(neighbourhood)","count")

df_neigh_count = df_neigh_count.withColumn('percentage',round((f.col('count')/f.sum('count').over(Window.partitionBy())*100),3)).orderBy('percentage',ascending=False)

display(df_neigh_count)
#check why you are getting California and United States in each row even though dataset isnt that way

# COMMAND ----------

#5. Join neighbourhood to get neigh_group and plot

nei_group = df_neigh_count.alias("nc").join(df_neighbourhoods.alias("ne"),col("nc.neighbourhood")==col("ne.neighbourhood")).select("nc.*","ne.neighbourhood_group")

display(nei_group)

