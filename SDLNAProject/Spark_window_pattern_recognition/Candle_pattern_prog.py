
from pyspark.sql.functions import col, row_number, count
import pyspark
from pyspark.sql import functions as func
from delta import *
#
builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "6G") \
        .config("spark.driver.maxResultSize", "0") \
        .config("spark.kryoserializer.buffer.max", "2000M")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read\
    .format("Delta")\
    .load("hdfs://localhost:9000/user/akash/data_2022-04-07/stock_data/")

df = df.withColumn("date", func.to_date(func.col("date")))
# df.printSchema()

# ---------------------------------------------------------------------------------------------
choice = int(input("Enter the no. of days to check the lagging trend:(Keep it below 7)"))


band_3 = [1,2,3]
band_7=[x for x in range(4, (4+choice))]
trendup = ["up","stable"]
trenddown = ["down", "stable"]
windowSpec  = pyspark.sql.window.Window.partitionBy("Company_Name").orderBy(col("date").desc())
df1 = df.withColumn("row_number",row_number().over(windowSpec))


df_3_up=df1.filter(df1.row_number.isin(band_3) & df1.Day_Trend.isin(trendup)).drop("row_number")
df_7_down=df1.filter(df1.row_number.isin(band_7) & df1.Day_Trend.isin(trenddown)).drop("row_number")
df_3_down=df1.filter(df1.row_number.isin(band_3) & df1.Day_Trend.isin(trenddown)).drop("row_number")
df_7_up=df1.filter(df1.row_number.isin(band_7) & df1.Day_Trend.isin(trendup)).drop("row_number")

df_3_up_cnt=df_3_up.groupBy("Company_Name").agg(count('Day_Trend').alias('count_of_days')).where(col('count_of_days')==3)

df_7_down_cnt=df_7_down.groupBy("Company_Name").agg(count('Day_Trend').alias('count_of_days')).where(col('count_of_days')==choice)

df_3_down_cnt=df_3_down.groupBy("Company_Name").agg(count('Day_Trend').alias('count_of_days')).where(col('count_of_days')==3)

df_7_up_cnt=df_7_up.groupBy("Company_Name").agg(count('Day_Trend').alias('count_of_days')).where(col('count_of_days')==choice)

print("Last 3 days uptrending Companies are:")
print()
df_3_up_cnt.show()

print(f"Lagging {choice} days downtrending Companies are:")
print()
df_7_down_cnt.show()

print("Last 3 days downtrending Companies are:")
print()
df_3_down_cnt.show()

print(f"Lagging {choice} days uptrending Companies are:")
print()
df_7_up_cnt.show()

print(f"Companies to Buy stocks based on {choice+3} days analysis are:")
list_7_down=(df_7_down_cnt.select('Company_Name').
      rdd.flatMap(lambda x: x).collect())
df_3_up_cnt.filter(df_3_up_cnt.Company_Name.isin(list_7_down)).select('Company_Name').show()

print(f"Companies to short the stocks based on {choice+3} days analysis are:")
list_7_up=(df_7_up_cnt.select('Company_Name').
      rdd.flatMap(lambda x: x).collect())
df_3_down_cnt.filter(df_3_down_cnt.Company_Name.isin(list_7_up)).select('Company_Name').show()
# Output:

Enter the no. of days to check the lagging trend:(Keep it below 7)5
Last 3 days uptrending Companies are:

# Output:
# +------------+-------------+
# |Company_Name|count_of_days|
# +------------+-------------+
# |  ABBOTINDIA|            3|
# |         JSL|            3|
# |  SCHAEFFLER|            3|
# |   VIKASPROP|            3|
# |    FMGOETZE|            3|
# |       TITAN|            3|
# |      GLOBAL|            3|
# |      BALAXI|            3|
# |  TRITURBINE|            3|
# |  DBSTOCKBRO|            3|
# |    A2ZINFRA|            3|
# |     CHEMCON|            3|
# |        ATUL|            3|
# |      RALLIS|            3|
# |    CUBEXTUB|            3|
# |    PRIVISCL|            3|
# |      NUVOCO|            3|
# |    DIVISLAB|            3|
# |     HUBTOWN|            3|
# |     SYNGENE|            3|
# +------------+-------------+
# only showing top 20 rows
#
# Lagging 5 days downtrending Companies are:
#
# +------------+-------------+
# |Company_Name|count_of_days|
# +------------+-------------+
# |      NUVOCO|            5|
# |     XELPMOC|            5|
# |   KANSAINER|            5|
# |     SANDHAR|            5|
# |    SUVIDHAA|            5|
# |    ARENTERP|            5|
# |  NAGREEKCAP|            5|
# |    AHLUCONT|            5|
# |  SURYAROSNI|            5|
# +------------+-------------+
#
# Last 3 days downtrending Companies are:
#
# +------------+-------------+
# |Company_Name|count_of_days|
# +------------+-------------+
# |    GANESHBE|            3|
# |     ACRYSIL|            3|
# |        HFCL|            3|
# |       GLOBE|            3|
# |   PANAMAPET|            3|
# |    BLISSGVS|            3|
# |        MMTC|            3|
# |        CERA|            3|
# |     SMSLIFE|            3|
# |      JKTYRE|            3|
# |   CENTRALBK|            3|
# |         HAL|            3|
# |  SUNCLAYLTD|            3|
# |    UNIENTER|            3|
# |    AGROPHOS|            3|
# |  KEYFINSERV|            3|
# |      RUSHIL|            3|
# |     LAOPALA|            3|
# |        LASA|            3|
# |    KRISHANA|            3|
# +------------+-------------+
# only showing top 20 rows
#
# Lagging 5 days uptrending Companies are:
#
# +------------+-------------+
# |Company_Name|count_of_days|
# +------------+-------------+
# |  REMSONSIND|            5|
# |     SKIPPER|            5|
# |   VIKASPROP|            5|
# |      AAKASH|            5|
# |       GET&D|            5|
# |    MINDTREE|            5|
# |  DBSTOCKBRO|            5|
# |       WORTH|            5|
# |      PRAENG|            5|
# |   SMARTLINK|            5|
# |       MICEL|            5|
# |    BAGFILMS|            5|
# |   BLKASHYAP|            5|
# |     KMSUGAR|            5|
# |     CHEMFAB|            5|
# |  ANKITMETAL|            5|
# |      MITTAL|            5|
# |    RTNPOWER|            5|
# |  CAPLIPOINT|            5|
# |   DIGISPICE|            5|
# +------------+-------------+
# only showing top 20 rows
#
# Companies to Buy stocks based on 8 days analysis are:
# +------------+
# |Company_Name|
# +------------+
# |      NUVOCO|
# |     XELPMOC|
# |   KANSAINER|
# |     SANDHAR|
# |    SUVIDHAA|
# |    ARENTERP|
# +------------+
#
# Companies to short the stocks based on 8 days analysis are:
# +------------+
# |Company_Name|
# +------------+
# |      KOPRAN|
# |       DHRUV|
# |        IZMO|
# |    BFINVEST|
# |  PREMIERPOL|
# |        IMFA|
# +------------+




