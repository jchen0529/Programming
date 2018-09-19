import pyspark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
import pyspark.sql.functions as func
from pyspark.sql.functions import udf #pyspark df column manipulation

sc = SparkContext()
sqlContext = SQLContext(sc)

#read in file from hdfs
sc.setLogLevel("ERROR")
crime = sc.textFile("Crimes_-_2001_to_present.csv")

header = crime.first() 

#only keep case and date columns for the bar plot
df=crime.filter(lambda row:row != header).map(lambda x: x.split(",")[1:3]).toDF(["case","date"])

#extract and add month and year cols
month = udf(lambda x:x[:2],StringType())
df = df.withColumn("month", month('date'))

year = udf(lambda x:x[6:10],StringType())
df = df.withColumn("year", year('date'))

avg_plot = df.groupBy("year", "month").count().groupBy("month").avg('count')
barplot = avg_plot.toPandas()

barplot['month'] = barplot['month'].astype(str).astype(int)

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
plt.bar(barplot.month, barplot['avg(count)'], align='center', alpha=0.5)
plt.xticks(barplot.month)
plt.title('Average monthly crimes')
plt.savefig('chen_1_monthly average crime.png')