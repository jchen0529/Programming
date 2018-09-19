import pyspark
import pandas as pd
import numpy as np
from pyspark import SparkContext
import csv
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics

sc = SparkContext()

#read in file from hdfs
sc.setLogLevel("ERROR")
crime = sc.textFile("Crimes_-_2001_to_present.csv")

header = crime.first() 
crime = crime.filter(lambda row:row != header) #filter out header

#csv parser
def csv_parse(line):
    csv_reader = csv.reader([line])
    fields = None
    for row in csv_reader:
        fields = row
    return fields

data = crime.map(csv_parse).persist()

#Q2(1) - find top 10 blocks with count of crimes in 2013 - 2015

recent = data.filter(lambda x: x[2][6:10] in ['2015','2014','2013'])
crime_by_block = recent.map(lambda x: (x[3], 1)).reduceByKey(lambda a, b: a+b)

#sort by descending order
top_10_block = crime_by_block.takeOrdered(10, key = lambda x: -x[1])

with open('chen_2a.txt', 'w') as fp:
    fp.write('\n'.join('%s %s' % x for x in top_10_block))


#Q2(2) - find 2 beats with highest correlation in crimes in past 5 years
recent5 = data.filter(lambda x: x[17] in ['2015','2014','2013','2012', '2011'])

crime_by_beats = recent5.map(lambda x: ((x[10], x[17]),1)).reduceByKey(lambda a, b: a+b).map(lambda x: (x[0][0],[(x[0][1], x[1])]))

#fill in missing year crime count and sort
def impute(vals):
    years = [i[0] for i in vals]
    if '2011' not in years:
        vals.append(('2011', 0)) #append takes 1 argument
    if '2012' not in years:
        vals.append(('2012', 0))
    if '2013' not in years:
        vals.append(('2013', 0))
    if '2014' not in years:
        vals.append(('2014', 0))
    if '2015' not in years:
        vals.append(('2015', 0))
    vals.sort(key = lambda x: int(x[0]))
    return vals

#Reduce again to put value pairs associated with the beats together ('beat', [(year1,crime), (year2,crime)])
#apply year imputation for each beat
beats_annual = crime_by_beats.reduceByKey(lambda a,b: a+b).map(lambda x: (x[0], impute(x[1])))

#drop years now that crimes are sorted from 2011 - 2015
beats_crimes = beats_annual.map(lambda x: (x[0], [i[1] for i in x[1]]))

beats = beats_crimes.map(lambda x: x[0])
crimes = beats_crimes.map(lambda x: x[1])
rddkey = beats.collect()
rdd = crimes.collect()

corr_matrix = np.corrcoef(rdd)
corr_df = pd.DataFrame(corr_matrix, index=rddkey, columns=rddkey)

#identify top 20 correlated beats
unstacked = corr_df.unstack()
corrs = pd.DataFrame(unstacked).reset_index()
corrs.columns = ["beat1", "beat2", "correlation"]
corrs = corrs[corrs.beat1 != corrs.beat2]
top_beats = corrs.nlargest(20, "correlation")

top_beats.to_csv("chen_2b.csv")

top2 = corr_df.loc['2132', '2133']
print ("Based on map, the highest correlated and adjacent beats are 2132 and 2133 with correlation:", top2)

#Q2(3) - find 2 beats with highest correlation in crimes in past 5 years
daly = data.filter(lambda x: (int(x[17]) > 1988 and int(x[17]) < 2011)).persist()
emanuel = data.filter(lambda x: (int(x[17]) > 2010)).persist()

# Count number of days in office 
daly_days = daly.map(lambda x: x[2][0:10]).distinct().count() #3652
e_days = emanuel.map(lambda x: x[2][0:10]).distinct().count() #1600

#normalized crime count by beats
daly_beats = daly.map(lambda x: (x[10], 1)).reduceByKey(lambda a,b: a+b).map(lambda x: (x[0], x[1]/daly_days)) #304 beats
e_beats = emanuel.map(lambda x: (x[10], 1)).reduceByKey(lambda a,b: a+b).map(lambda x: (x[0], x[1]/e_days)) #303 beats

#inner join the 2
d_e = daly_beats.join(e_beats) #303 beats

d_val = Vectors.dense([i[1][0] for i in d_e.collect()])
e_val= Vectors.dense([i[1][1] for i in d_e.collect()])

#conduct chiSqTest to check if the differences in crime counte by beats is statistically significant
results = Statistics.chiSqTest(d_val, e_val)
print("%s\n" % results)




























































































