from pyspark import SparkContext
from operator import add
import sys
import json
import re

def flat(x):
	res = []
	for item in sorted(x[1]):
		res.append([item,x[0]])
	return res

inputfile = sys.argv[1]
outputfile = sys.argv[2]
stop = sys.argv[3]
year = sys.argv[4]
m = sys.argv[5]
n = sys.argv[6]


stop_set = set()
for word in open(stop, 'r'):
	stop_set.add(word[:-1])
stop_set.update(['(', '[', ',', '.', '!', '?', ':', ';', ']', ')',''])
sc = SparkContext(appName="inf553")
#convert json into rdd
rdd = sc.textFile(inputfile).map(lambda x: json.loads(x))
answer = dict()
#store the number of review
answer['A'] = rdd.count()
#filter review with specific year
select_y = rdd.filter(lambda x : x['date'].startswith(year))
#store the number of reviews which are writed in specific year
answer['B'] = select_y.count()
# distinct user
distinct_user = rdd. map(lambda x: x['user_id']).distinct()
answer['C'] = distinct_user.count()
# Top m users
count_user = rdd.map(lambda x: [x['user_id'],1]).reduceByKey(add).map(lambda x: 
		[x[1],x[0]]).groupByKey().sortByKey(False).flatMap(lambda x: flat(x))
answer['D'] = count_user.take(int(m))
# Top n frequent words in the review text
text = rdd.filter(lambda x: 'text' in x.keys()).flatMap(lambda x: re.split(r'[;,.:?!\s()\[\]]', x['text'])).filter(lambda x : 
	x.lower() not in stop_set).map(lambda x: [x,1]).reduceByKey(add).map(lambda x: 
		[x[1],x[0]]).groupByKey().sortByKey(False).flatMap(lambda x: [item for item in x[1]])
answer['E'] = text.take(int(n))
#write into file
json.dump(answer, open(outputfile, 'w'))
