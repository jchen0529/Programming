import pyspark
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import csv

sc = SparkContext()
sqlContext = SQLContext(sc)

#read in file from hdfs
sc.setLogLevel("ERROR")
crime = sc.textFile("jchen/crime/Crimes_-_2001_to_present.csv")
economic = sc.textFile("jchen/crime/census_socioeconomic.csv")

header = crime.first() 
e_header = economic.first()

""" data readin and filter
1. read in data
2. keep only the columns needed
3. from crime: filter on year = 2015 since we are predicting for the next week in 2015
"""

#economic data
econ = economic.filter(lambda x: x!=e_header).map(lambda x: x.split(",")).toDF(e_header.split(","))
econ.registerTempTable('econ')
econ_df = sqlContext.sql('select `Community Area Number` as community, `PER CAPITA INCOME ` as income, `HARDSHIP INDEX` as hardship from econ')

#crime data
def csv_parse(line):
    csv_reader = csv.reader([line])
    fields = None
    for row in csv_reader:
        fields = row
    return fields

data = crime.map(csv_parse).persist()

#filter on 2015 data to predict for the next week in 2015
df = data.filter(lambda row:row[0]!='ID').filter(lambda row:row[2][6:10] == '2015').map(lambda x: x[1:14]).toDF(header.split(",")[1:14])

#find week and year number
df2 = df.withColumn('datetime',from_unixtime(unix_timestamp(df[1], 'MM/dd/yyyy HH:mm:ss')))
df2 = df2.where(col('datetime').isNotNull())
df2 = df2.withColumn('week', weekofyear(df2.datetime)).withColumn('year', year(df2.datetime))

# Aggregate data
df2.registerTempTable('weekly')
df3 = sqlContext.sql('select Beat as beat, `Community Area` as community, year, week, count(*) as crime_count from weekly group by Beat, `Community Area`, year, week')

# Join with external socioeconomic data
df3.registerTempTable('df3')
econ_df.registerTempTable('econ')
merged = sqlContext.sql('select df3.beat, df3.year, df3.week, df3.crime_count, df3.community, econ.income, econ.hardship from df3 left join econ on df3.community = econ.community')

# keep the beat with avg value in income and hardship
final_df = merged.groupBy("beat", "week").agg({"crime_count": "sum", "income": "avg", "hardship":"avg", "community":"count"})
final_df = final_df.withColumn("beat", col("beat").cast("double"))
final_df = final_df.withColumn("sum(crime_count)", col("sum(crime_count)").cast("double"))

#create prediction df for week of 22 of each beat
test_df = final_df[["beat", "avg(income)", "avg(hardship)", "count(community)"]].dropDuplicates()
test_df = test_df.withColumn("week", lit(22))

"""Linear regression
train a LR model, check model summary, and predict on test df
"""

# Assemble features
vectors = VectorAssembler(inputCols = ['week','avg(income)','avg(hardship)', 'beat','count(community)'], outputCol = 'features')
features = vectors.transform(final_df)
train_final = features.withColumn("label", final_df["sum(crime_count)"])

lr = LinearRegression(featuresCol = 'features', labelCol='label', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_final)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

#trainingSummary = lr_model.summary
#print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
#print("r2: %f" % trainingSummary.r2)

#predict
test_features = vectors.transform(test_df)
predictions = lr_model.transform(test_features)
output = predictions.select("beat", "prediction").toPandas()
output.to_csv("chen_3_beat_predictions.txt")




























