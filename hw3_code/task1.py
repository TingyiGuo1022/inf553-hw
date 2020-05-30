from pyspark import SparkContext
import json
import os
from operator import add
import sys
import itertools
import random
import time
import collections

def build_list(users_dict, rated_list):
    res = list()
    for x in set(rated_list):
        res.append(user_dict[x])
    return sorted(res)

def cal_min_hash(a, b, m, value_list):
    ans = list()
    for i in range(len(b)):
        cur_min = m+1
        for x in value_list:
            cur_min= min(cur_min, (a[i]*x+b[i])%m)
        ans.append(cur_min)
    return ans


def split_list(value_list, size):
    bands = list()
    index = 0
    for i in range(size, len(value_list)+1, size):
        cur = str(index)+","
        cur += str(value_list[i-2])+","+str(value_list[i-1])
        bands.append(cur)
    return bands

def combination(value_list):
    value_list = sorted(value_list)
    if(len(value_list)==2):
        return [value_list]
    ans = list()
    for i in range(len(value_list)):
        for j in range(i+1, len(value_list)):
            ans.append(sorted([value_list[i], value_list[j]]))
    return ans


inputfile = sys.argv[1]
outputfile = sys.argv[2]
start_time = time.time()
support = 0.05
sc = SparkContext(appName="inf553")
review = sc.textFile(inputfile).map(json.loads).map(lambda x: [x['business_id'], x['user_id']])
users_list = review.map(lambda x: x[1]).sortBy(lambda x: x).distinct().collect()
index = 0
user_dict = collections.defaultdict(int)
for item in users_list:
    user_dict[item] = index
    index+=1
rated = review.groupByKey().mapValues(lambda x: build_list(user_dict,x)).sortByKey()
m = len(users_list)
min_hash_size = 1100
a_list = random.sample([i for i in range(m)], min_hash_size)
b_list = random.sample([i for i in range(m)], min_hash_size)
min_hash = rated.mapValues(lambda x: cal_min_hash(a_list,b_list, m, x))
best_b = 550
best_r = 2
bands = min_hash.flatMapValues(lambda x: split_list(x, best_r)).map(
    lambda x: [x[1],x[0]]).groupByKey().filter(lambda x: len(x[1])>=2).map(lambda x: list(x[1]))
rate = rated.collect()
rated_dict = collections.defaultdict(list)
for item in rate:
    rated_dict[item[0]] = item[1]
candidates = bands.flatMap(lambda x: itertools.combinations(x,2)).distinct().map(lambda x: tuple([x, len(set(rated_dict[x[0]])&(
    set(rated_dict[x[1]])))/len(set(rated_dict[x[0]]).union(set(rated_dict[x[1]])))])).filter(lambda x: x[1]>=0.05).map(
        lambda x: json.dumps({"b1": x[0][0], "b2": x[0][1], "sim": x[1]}))

output = open(outputfile, 'w')
for item in candidates.collect():
    output.write(item)
    output.write('\n')
print("Duration: ",int(time.time()-start_time))