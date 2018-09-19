import pyspark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf #pyspark df column manipulation
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

sc = SparkContext()
sqlContext = SQLContext(sc)

#read in file from hdfs
sc.setLogLevel("ERROR")
crime = sc.textFile("jchen/crime/Crimes_-_2001_to_present.csv")

header = crime.first() 

#only keep case and date columns for the bar plot
df=crime.filter(lambda row:row != header).map(lambda x: x.split(",")[1:3]).toDF(["case","date"])

#monthly pattern
month = udf(lambda x:x[:2],StringType())
df = df.withColumn("month", month('date'))
monthly = df.groupBy("month").count().toPandas()
monthly = monthly.sort_values(by ='count', ascending = False)
monthly['month'] = monthly['month'].astype(str).astype(int)

#monthly pattern/plot
plt.bar(monthly.month, monthly['count'], align='center', alpha=0.5)
plt.xticks(monthly.month)
plt.title('Total crimes per month since 2001')
plt.savefig('chen_4_monthly_pattern.png')

#hourly pattern
hour_24 = udf(lambda x: (int(x[11:13]) + 12) if (x[20:22] == 'PM') else int(x[11:13]))
df = df.withColumn("hour", hour_24('date'))
hourly = df.groupBy("hour").count().toPandas()
hourly = hourly.sort_values(by ='count', ascending = False)
hourly['hour'] = hourly['hour'].astype(str).astype(int)

hourly.plot(x = 'hour', y='count', kind='bar')
plt.title('Total crimes per hour since 2001')
plt.savefig('chen_4_hourly_pattern.png')

#weekly pattern
weekday = udf(lambda x: datetime.date(int(x[6:10]), int(x[:2]), int(x[3:5])).strftime("%A"))
df = df.withColumn("day", weekday('date'))
weekly = df.groupBy("day").count().toPandas()
weekly = weekly.sort_values(by ='count', ascending = False)

weekly.plot(x = 'day', y='count', kind='bar')
plt.title('Total crimes per weekday since 2001')
plt.tight_layout()
plt.savefig('chen_4_weekly_pattern.png')


























