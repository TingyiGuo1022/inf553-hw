from functools import reduce
from graphframes import GraphFrame
from pyspark import SparkContext, SQLContext
import sys
import time
from itertools import combinations
from operator import add
import types
import collections

start = time.time()
threshold = int(sys.argv[1])
inputfile = sys.argv[2]
outputfile = sys.argv[3]
sc = SparkContext(master="local[3]")
sc.setLogLevel("WARN")
sql_sc = SQLContext(sc)
#sql_sc.sql("set spark.sql.shuffle.partitions=200")
data = sc.textFile(inputfile).map(lambda x: x.split(',')).filter(lambda x: x[0] != 'user_id')
user_pairs = data.map(lambda x: [x[1],x[0]]).groupByKey().mapValues(sorted).mapValues(lambda x: combinations(x, 2)).flatMap(
    lambda x: x[1]).flatMap(lambda x: [[x, 1], [x[::-1],1]]).reduceByKey(add).map(lambda x: [x[0][0],x[0][1], x[1]]).filter(lambda x: x[2]>=threshold)
users = user_pairs.flatMap(lambda x: x[:2]).distinct().map(lambda x: [x])
print(users.count())
vertices = sql_sc.createDataFrame(users, ['id'])
edges = sql_sc.createDataFrame(user_pairs, ["src", "dst", "intersection"])
graph = GraphFrame(vertices, edges)
result = graph.labelPropagation(maxIter=5).select('id', 'label').rdd.map(lambda x: [x[1],x[0]]).groupByKey().map(
    lambda x: sorted(list(x[1]))).sortBy(lambda x: x[0]).sortBy(len).collect()
file  = open(outputfile, 'w')
for item in result:
    file.write("'"+"', '".join(item)+"'")
    file.write('\n')

print("Duration: ", time.time() - start)