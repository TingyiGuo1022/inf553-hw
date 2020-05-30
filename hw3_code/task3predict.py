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

def build_dict(value_list):
    ans = collections.defaultdict(float)
    for item in value_list:
        ans[item[0]] = item[1]
    return ans

def merage(value1, value2):
    if value1 != None and value2 != None:
        return sorted(list(value1)+list(value2), key = lambda x: x[1], reverse = True)
    if value1 == None:
        if value2 != None:
            return sorted(list(value2), key = lambda x: x[1], reverse = True)
        else: return None
    if value2 == None:
        if value1 != None:
            return sorted(list(value1), key = lambda x: x[1], reverse = True)
        else: return None

def predict(model_dict, train_dict, item_avg_dict, item):
    user_inf = train_dict[item[0]]
    model_inf = model_dict[item[1]]
    if len(model_inf) == 1: return None
    cur = 0
    base, total = 0, 0
    for business_inf in model_inf:
        
        if business_inf[0] in user_inf:
            cur += 1
            base += abs(business_inf[1])
            total += user_inf[business_inf[0]]*business_inf[1]
        if cur == 3: break
    if base == 0 or total==0 or cur == 1:
        return None
    return [item[0], item[1], total/base]

def predict_user_based(model_dict, train_dict, rates, item):
    business_inf = train_dict[item[1]]
    model_inf = model_dict[item[0]]
    if len(model_inf) == 1: return None
    cur = 0
    base, total = 0, 0
    avg = rates[item[0]][0]/(rates[item[0]][1]+1)
    for user_inf in model_inf:
        if user_inf[0] in business_inf.keys():
            cur += 1
            base += abs(user_inf[1])
            total += (business_inf[user_inf[0]] - (
                rates[user_inf[0]][0]-business_inf[user_inf[0]])/rates[user_inf[0]][1])*user_inf[1]
        if cur == 3: break
    if base == 0 or total == 0 or cur ==1 :
        return None
    return [item[0], item[1], avg + total/base]



start = time.time()
train_file = sys.argv[1]
test_file = sys.argv[2]
model_file = sys.argv[3]
output_file = sys.argv[4]
cf_type = sys.argv[5]

sc = SparkContext(appName="inf553")
if cf_type == 'item_based':
    model1 = sc.textFile(model_file).map(json.loads).filter(lambda x: x['b1']!=x['b2']).map(
        lambda x: [x['b1'],(x['b2'], x['sim'])]).groupByKey()
    model2 = sc.textFile(model_file).map(json.loads).filter(lambda x: x['b1']!=x['b2']).map(
        lambda x: [x['b2'],(x['b1'], x['sim'])]).groupByKey()
    model = model1.fullOuterJoin(model2).map(lambda x: [x[0], merage(x[1][0], x[1][1])]).collect()
    model_dict = collections.defaultdict(list)
    for item in model:
        model_dict[item[0]] = item[1]
    test = sc.textFile(test_file).map(json.loads).map(lambda x: [x['user_id'], x['business_id']])
    train = sc.textFile(train_file).map(json.loads).map(lambda x: [x['user_id'],(
        x['business_id'],x['stars'])]).groupByKey().mapValues(build_dict).collect()
    item_avg = sc.textFile(train_file).map(json.loads).map(lambda x: [x['business_id'],x['stars']]).groupByKey().mapValues(
        lambda x: sum(list(x))/len(list(x))).collect()
    item_avg_dict = collections.defaultdict(int)
    for item in item_avg:
        item_avg_dict[item[0]] = item[1]
    train_dict = collections.defaultdict(dict)
    for item in train:
        train_dict[item[0]] = item[1]
    print('cal result!', time.time()-start)
    result = test.map(lambda x: predict(model_dict,train_dict,item_avg_dict, x)).filter(lambda x: x!=None).collect()
    file = open(output_file,'w')
    for item in result:
        json.dump({'user_id': item[0], 'business_id': item[1], 'stars': item[2]}, file)
        file.write('\n')
else:
    model1 = sc.textFile(model_file).map(json.loads).filter(lambda x: x['u1']!=x['u2']).map(
        lambda x: [x['u1'],(x['u2'], x['sim'])]).groupByKey()
    
    model2 = sc.textFile(model_file).map(json.loads).filter(lambda x: x['u1']!=x['u2']).map(
        lambda x: [x['u2'],(x['u1'], x['sim'])]).groupByKey()
    model = model1.fullOuterJoin(model2).map(lambda x: [x[0], merage(x[1][0], x[1][1])]).collect()
    model_dict = collections.defaultdict(list)
    for item in model:
        model_dict[item[0]] = item[1]
    test = sc.textFile(test_file).map(json.loads).map(lambda x: [x['user_id'], x['business_id']])
    train = sc.textFile(train_file).map(json.loads).map(lambda x: [x['business_id'],(
        x['user_id'],x['stars'])]).groupByKey().mapValues(build_dict).collect()
    user_rate = sc.textFile(train_file).map(json.loads).map(lambda x: (
        x['user_id'],x['stars'])).groupByKey().mapValues(lambda x: [sum(list(x)), len(list(x))-1]).collect()
    rates = collections.defaultdict(list)
    for item in user_rate:
        rates[item[0]]= item[1]
    train_dict = collections.defaultdict(dict)
    for item in train:
        train_dict[item[0]] = item[1]
    print('cal result!', time.time()-start)
    result = test.map(lambda x: predict_user_based(model_dict,train_dict, rates, x)).filter(lambda x: x!=None).map(
        lambda item: json.dumps({'user_id': item[0], 'business_id': item[1], 'stars': item[2]})).collect()
    file = open(output_file,'w')
    for item in result:
        file.write(item)
        file.write('\n')



print('Duration: ', time.time()- start)