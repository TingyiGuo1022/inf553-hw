from pyspark import SparkContext
from operator import add
import sys
import json
import collections

def give_star(x):
	res = []
	for category in x[0]:
		res.append([category.strip(), x[1]])
	return res

def flat(x):
	res = []
	for item in sorted(x[1]):
		res.append([item,x[0]])
	return res

input1 = sys.argv[1]
input2 = sys.argv[2]
output = sys.argv[3]
operate = sys.argv[4]
n = sys.argv[5]

sc = SparkContext(appName="inf553")
if operate == 'spark':
	#convert json into rdd
	review_rdd = sc.textFile(input1).map(lambda x: json.loads(x)).map(lambda x : [x['business_id'], x['stars']])
	business_rdd = sc.textFile(input2).map(lambda x: json.loads(x)).map(lambda x: [x['business_id'], x['categories']]).filter(lambda x: x[1] != None)
	#natural join
	categories_star = business_rdd.join(review_rdd).map(lambda x: [x[1][0].split(', '), x[1][1]])
	category_star = categories_star.flatMap(lambda x: give_star(x))
	# get answer
	ans = category_star.aggregateByKey((0,0), lambda U,v: (U[0] + v, U[1] + 1), lambda U1,U2: 
		(U1[0] + U2[0], U1[1] + U2[1])).map(lambda x: (x[0], x[1][0]/x[1][1], 1)).map(lambda x: 
		[x[1],x[0].strip()]).groupByKey().sortByKey(False).flatMap(lambda x: flat(x))
	json.dump({"result": ans.take(int(n))}, open(output,'w'))

if operate == 'no_spark':
	business_dict = collections.defaultdict(dict)
	#read review
	business_star = list(map(lambda item: [item["business_id"],item['stars']], list(map(json.loads, open(input1,'r')))))
	#read business.json
	business_cat = filter(lambda x: x[1] != None, list(map(lambda item: [item["business_id"],item['categories']],list(map(json.loads, open(input2,'r'))))))
	
	#join
	for business_inf in business_cat:
		if 'cat' not in business_dict[business_inf[0]].keys():
			business_dict[business_inf[0]]['cat'] = []
		business_dict[business_inf[0]]['cat'] += business_inf[1].split(', ')


	for business_inf in business_star:
		if 'cat' not in business_dict[business_inf[0]].keys(): 
			continue
		if 'star' not in business_dict[business_inf[0]].keys():
			business_dict[business_inf[0]]['star'] = []
		business_dict[business_inf[0]]['star'].append(business_inf[1])

	ans = collections.defaultdict(list)
	for business in business_dict.keys():
		if 'cat' in business_dict[business].keys():
			for c in business_dict[business]['cat']:
				if 'star' in business_dict[business].keys():
					for s in business_dict[business]['star']:
						ans[c.strip()].append(s)

	result = []
	for cat in ans.keys():
		result.append([cat, sum(ans[cat])/len(ans[cat])])

	result = sorted(sorted(result, key = lambda x: x[0]), key = lambda x : x[1], reverse = True)


	json.dump({"result": result[:int(n)]}, open(output,'w'))









