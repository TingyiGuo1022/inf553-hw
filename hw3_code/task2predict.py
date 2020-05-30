from pyspark import SparkContext
import json
import os
import re
from operator import add
import sys
import random
import time
import collections
import math


def predict(user, business, x):
    user_profile = user[x[0]]
    business_profile = business[x[1]]
    intersection_res = set(user_profile).intersection(set(business_profile))
    base = math.sqrt(len(user_profile))*math.sqrt(len(business_profile))
    if(base == 0):
        return [x[0],x[1], 0.0]
    sim = len(intersection_res)/base
    return [x[0],x[1], sim]


start = time.time()
test_file = sys.argv[1]
model_file = sys.argv[2]
output_file = sys.argv[3]
sc = SparkContext(appName="inf553")
model = sc.textFile(model_file).map(json.loads)
user_dict = collections.defaultdict(list)
user_profile = model.filter(lambda x: x['type'] == 'user').map(lambda x: [x['name'], x['profile']]).collect()
for item in user_profile:
    user_dict[item[0]] = item[1]
business_dict = collections.defaultdict(list)
business_profile = model.filter(lambda x: x['type'] == 'business').map(lambda x: [x['name'], x['profile']]).collect()
for item in business_profile:
    business_dict[item[0]] = item[1]
test = sc.textFile(test_file).map(json.loads).map(lambda x: [x['user_id'],x['business_id']])
result = test.map(lambda x: predict(user_dict, business_dict, x)).filter(lambda x: x[2]>=0.01).map(
        lambda x: json.dumps({"b1": x[0], "b2": x[1], "sim": x[2]})).collect()
file = open(output_file,'w')
for item in result:
    file.write(item)
    file.write('\n')

print('duration: ', time.time()-start)