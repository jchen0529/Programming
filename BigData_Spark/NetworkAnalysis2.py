import pyspark
import pandas as pd
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics
from pyspark.sql import functions as func
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lower, col
import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import re
from functools import reduce
from operator import add
from pyspark.mllib.clustering import KMeans, KMeansModel

sc = SparkContext()
sqlContext = SQLContext(sc)

#read in file from hdfs
sc.setLogLevel("ERROR")
venmo = sc.textFile("jchen/venmo/venmoSample.csv")

header = venmo.first() 

#save as a df
venmo_df = venmo.filter(lambda x: x!=header).map(lambda x: x.split(",")).toDF(header.split(","))
df = venmo_df.select(venmo_df["transacton_type"],venmo_df["description"], venmo_df["datetime"])

"""
#Q2-1: Find the top 10 most popular emojis
"""

#define a udf to get emojis
def emoji(desc):
    return (re.findall(r'[^\w\s,!.\'\"\/+-^.$%&*/:;?@\'️'']' , desc))
emoji = udf(emoji, StringType())
df = df.withColumn("emoji", emoji(df['description']))

#flat out emoji and filter out empty ones
es = df.select("emoji").rdd.flatMap(lambda row: row).filter(lambda x: x!=('[]'))
#flat again and get count
es_count = es.flatMap(lambda row: row.strip(',')).filter(lambda x: x not in ('[', ']', ',',' ',')','')).map(lambda emoji: (emoji,1)).reduceByKey(add).collect()
top10 = sorted(es_count,key=lambda x:(-x[1],x[0]))[:10]

with open('exercise2_1.txt', 'w') as fp:
    fp.write('\n'.join('%s %s' % x for x in top10))

"""
#Q2-2: Find the top 5 most popular emojis by weekday
"""

weekday = udf(lambda x: datetime.date(int(x[:4]), int(x[5:7]), int(x[8:10])).strftime("%A"))
df2 = df.withColumn("day", weekday('datetime'))


df3 = df2.select("day", "emoji")
df3 = df3.filter(df3["emoji"]!='[]')

days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday','Friday', 'Saturday','Sunday']

top = {}
for i in range(0,7):
    data = df3.filter(df3['day'] == days[i]).select("emoji").rdd.flatMap(lambda row: row)
    cnt = data.flatMap(lambda row: row.strip(',')).filter(lambda x: x not in ('[', ']', ',', ' ',')','','(')).map(lambda emoji: (emoji,1)).reduceByKey(add).collect()
    top[days[i]] = sorted(cnt,key=lambda x:(-x[1],x[0]))[:5]

fo = open("exercise2_2.txt", "w")

for k, v in top.items():
    fo.write(str(k) + ' >>> '+ str(v) + '\n\n')

fo.close()

"""
#Q2-3: Analyze the content of all transaction messages with at least 5 attributes

5 features created based on description:

1. did the payment occur on weekday or weekend
2. did the payment occur in the am or pm
3. is the payment related to rides
4. is the payment related to rent
5. msg length

"""
#weekday or weekend
wkd = udf(lambda day: 1 if day in ["Saturday", "Sunday"] else 0, IntegerType())
df_cluster = df2.withColumn("weekend", wkd('day'))

#am/pm pattern based on hour
hour = udf(lambda x: x[11:13])
df_cluster = df_cluster.withColumn("hour", hour('datetime'))

hour_pm = udf(lambda x: 0 if (int(x[0][0]) == 0 or x in ['10', '11']) else 1)
df_cluster = df_cluster.withColumn("pm", hour_pm('hour'))

#ride or rent related transactions 

searchfor_rides = ['ride', 'uber', 'lyft', '🚗']
searchfor_rent = ['rent', '🏠']

df_cluster = df_cluster.withColumn('description_lower', lower(func.col('description')))
df_cluster = df_cluster.withColumn('check_rides', func.when(func.col('description_lower').rlike('|'.join(searchfor_rides)), 1).otherwise(0))
df_cluster = df_cluster.withColumn('check_rent', func.when(func.col('description_lower').rlike('|'.join(searchfor_rent)), 1).otherwise(0))

#count length of msgs
def length(desc):
    return(len(desc))
length = udf(length, IntegerType())

df_cluster = df_cluster.withColumn('msg_length', length('description'))


final_df = df_cluster.select("weekend", "pm", "check_rides", "check_rent", "msg_length")
scaled_df = final_df.withColumn("s_wkd", col("weekend") / final_df.agg(stddev_samp("weekend")).first()[0])\
            .withColumn("s_pm", col("pm") / final_df.agg(stddev_samp("pm")).first()[0])\
            .withColumn("s_rides", col("check_rides") / final_df.agg(stddev_samp("check_rides")).first()[0])\
            .withColumn("s_rent", col("check_rent") / final_df.agg(stddev_samp("check_rent")).first()[0])\
            .withColumn("s_msg_len", col("msg_length") / final_df.agg(stddev_samp("msg_length")).first()[0])

cluster_rdd = scaled_df.select("s_wkd", "s_pm", "s_rides", "s_rent", "s_msg_len").rdd.map(lambda x: (x[0], x[1], x[2], x[3], x[4]))

# Evaluate clustering by computing Within Set Sum of Squared Errors
sse_in = []
centers = []

for i in range(1,6):
    # kmeans clustering for each cluster size
    clusters = KMeans.train(cluster_rdd, i, maxIterations=10, initializationMode="random") 
    sse = clusters.computeCost(cluster_rdd)
    sse_in.append(sse)
    centers.append(clusters.centers)


best_k = sse_in.index(np.min(sse_in)) 
best_centers = centers[best_k]


with open("exercise2_3.txt","w") as f:
    f.write("Best WSSE: %s" % sse_in[best_k])
    f.write("\nNumber Clusters: %s" %(best_k+1))
    f.write("\n")
    f.write("\nCluster centers:" )
    for i in centers[best_k]:
        f.write(str(i) + '\n')
    
f.close()

#plot cluster and within sses
plt.plot(range(1,6), sse_in)
plt.title("K-means Cluster Size and Within SSEs")
plt.xlabel("Clusters")
plt.xticks(range(1,6))
plt.ylabel("Within SSEs")
plt.savefig("exercise2_3.png")













