import pyspark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics
from pyspark.sql.types import StringType
from pyspark.sql import functions as func
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime

sc = SparkContext()
sqlContext = SQLContext(sc)

#read in file from hdfs
sc.setLogLevel("ERROR")
venmo = sc.textFile("jchen/venmo/venmoSample.csv")

header = venmo.first() 

#save as a df
venmo_df = venmo.filter(lambda x: x!=header).map(lambda x: x.split(",")).toDF(header.split(","))

"""
#Q1-1: plot venmo's degree distribution by treating the network as undirected
"""

#define a udf to get the set of u1u2 combinations
def combo(u1, u2):
    #u1s = str(u1)
    #u2s=str(u2)
    if int(u1) < int(u2):
        return (u1 + u2)
    else:
        return (u2 + u1)

combo = udf(combo, StringType())
venmo_df = venmo_df.withColumn('u1u2',combo(venmo_df['user1'], venmo_df['user2']))
#drop duplicated sets
venmo_2 = venmo_df.dropDuplicates(['u1u2'])

u1_count = venmo_2.groupby("user1").agg({"user1":"count"}).withColumnRenamed("count(user1)", "count1")
u2_count = venmo_2.groupby("user2").agg({"user2":"count"}).withColumnRenamed("count(user2)", "count2")

join = u1_count.join(u2_count, u1_count.user1 == u2_count.user2,how='full')
join = join.na.fill(0)
join_degree = join.withColumn("degree", join["count1"] + join["count2"])
indegree = join_degree.select("degree").toPandas()

#fig, ax = plt.subplots()


#hist(ax, ndegree['degree'], bins = 20, color=['red'])

indegree.hist(bins = 150, rwidth = 0.8);
plt.title("Undirected network degree distribution")
plt.savefig('exercise1_1') 
"""
#Q1-2: plot venmo's in-degree and out-degree distributions
"""

#out-degree is count of user2 and in-degree is count of user1


u1_countd = venmo_df.groupBy('user1').agg(countDistinct('user2').alias('count1')) #outdegree: going to u2
u2_countd = venmo_df.groupBy('user2').agg(countDistinct('user1').alias('count2')) #indegree: coming from u1


ind = u2_countd.toPandas()
outd = u1_countd.toPandas()

ind.hist('count2', bins = 200, rwidth = 0.8)
plt.title("In-degree distributions")
plt.savefig('exercise1_2_indegree')

outd.hist('count1', bins = 200, rwidth = 0.8)
plt.title("Out-degree distributions")
plt.savefig('exercise1_2_outdegree')

"""
#Q1-3: 
a. What is the percentage of reciprocal transactions in the network?
b. plot venmo's percentage of reciprocal relationships over time, with 6 month increments between 1/1/2010 and 6/1/2016
"""

def myConcat(*cols):
    return func.concat(*[func.coalesce(c, func.lit("*")) for c in cols])

venmo_df = venmo_df.withColumn('u1u2_concat', myConcat(venmo_df['user1'], venmo_df['user2']))
users = venmo_df.select('u1u2','u1u2_concat').groupBy('u1u2','u1u2_concat').count()
users2 = users.groupBy('u1u2').agg(countDistinct(users.u1u2_concat).alias('receip'))
tot_rel = users2.count()
receip = users2.filter(col('receip')>1).count()
output = receip/tot_rel*100

with open('exercise1_3.txt', 'w') as f:
    f.write('Percentage of reciprocal relationships: %s %%' % output)

#plot 6-month cumulative reciprocal relationships

df2 = venmo_df.withColumn("date",to_date(substring(venmo_df.datetime,0,10))).orderBy(col('date').desc())

df2.registerTempTable("df2")

#add 6-month mark
df_month = sqlContext.sql('''SELECT * , (case when (date < cast('2012-01-01' as date)) then 1
                                         when (date >= cast('2012-01-01' as date)) and (date < cast('2012-07-01' as date)) then 2
                                         when (date >= cast('2012-07-01' as date)) and (date < cast('2013-01-01' as date)) then 3
                                         when (date >= cast('2013-01-01' as date)) and (date < cast('2013-07-01' as date)) then 4
                                         when (date >= cast('2013-07-01' as date)) and (date < cast('2014-01-01' as date)) then 5
                                         when (date >= cast('2014-01-01' as date)) and (date < cast('2014-07-01' as date)) then 6
                                         when (date >= cast('2014-07-01' as date)) and (date < cast('2015-01-01' as date)) then 7
                                         when (date >= cast('2015-01-01' as date)) and (date < cast('2015-07-01' as date)) then 8
                                         when (date >= cast('2015-07-01' as date)) and (date < cast('2016-01-01' as date)) then 9
                                         when (date >= cast('2016-01-01' as date)) and (date < cast('2016-07-01' as date)) then 10
                                         when (date >= cast('2016-07-01' as date)) then 11
                             else 0 end )as period from df2''')

rates = []
for i in range(1,12):
    print(i)
    portion = df_month.filter(df_month.period <= i)
    users = portion.select('u1u2', 'u1u2_concat').groupBy('u1u2','u1u2_concat').count()
    users2 = users.groupBy('u1u2').agg(countDistinct(users.u1u2_concat).alias('receip'))
    tot_rel =users2.count()
    receip = users2.filter(col('receip')>1).count()
    output = receip/tot_rel*100
    rates.append(output)

dates = ['2011-07','2012-01','2012-07','2013-01','2013-07','2014-01','2014-07','2015-01','2015-07','2016-01', '2016-07']
xn = range(len(dates))
plt.figure(figsize=(10,8))
plt.plot(xn,rates)
plt.xticks(xn, dates)
plt.ylabel('Percentage')
plt.xlabel('Period')
plt.title('Six-month cumulative percentage of Reciprocal Transactions since 2011')
plt.savefig('exercise1_3.png')



































