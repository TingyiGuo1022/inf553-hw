from pyspark import SparkContext
from operator import add
import sys
import json
import bisect

sc = SparkContext(appName="inf553")

def cal_Szie(iterator): 
	count = 0
	for x in iterator:
		count += 1
	yield count

inputfile = sys.argv[1]
output = sys.argv[2]
partition_type = sys.argv[3]
part_size = sys.argv[4]
n = sys.argv[5]

if partition_type == 'default':
	ans = dict()
	review = sc.textFile(inputfile).map(lambda x: json.loads(x)).map(lambda x: [x['business_id'], 1])
	ans['n_partitions'] = review.getNumPartitions()
	ans['n_items'] = review.mapPartitions(cal_Szie).collect()
	count = review.reduceByKey(add).filter(lambda x: x[1]>int(n)).map(lambda x: [x[0],x[1]])
	ans['result'] = count.collect()
	json.dump(ans, open(output, 'w'))

if partition_type == 'customized':

	ans = dict()
	review = sc.textFile(inputfile).map(lambda x: json.loads(x)).map(lambda x: [x['business_id'], 1]).partitionBy(int(part_size), lambda x: hash(x)//int(part_size))
	ans['n_partitions'] = review.getNumPartitions()
	ans['n_items'] = review.mapPartitions(cal_Szie).collect()
	count = review.reduceByKey(add).filter(lambda x: x[1]>int(n)).map(lambda x: [x[0],x[1]])
	ans['result'] = count.collect()
	json.dump(ans, open(output, 'w'))
