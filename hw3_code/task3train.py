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


def build_list(v_list):
    rate_dict = collections.defaultdict(int)
    sum_value, count = 0, 0
    for item in v_list:
        rate_dict[item[0]] = item[1]
        sum_value += item[1]
        count += 1
    return rate_dict

def build_model(business_base, combine_value):
    inf1 = business_base[combine_value[0]]
    inf2 = business_base[combine_value[1]]
    set1, set2 = set(inf1.keys()), set(inf2.keys())
    intersect = set1.intersection(set2)
    if len(intersect)<3:
        return None
    sum1, sum2 = 0, 0
    total, base1, base2 =0, 0, 0 
    for item in intersect:
        sum1 += inf1[item]
        sum2 += inf2[item]
    avg1, avg2 = sum1/len(intersect), sum2/len(intersect)
    for item in intersect:
        total += (inf1[item]-avg1)*(inf2[item]-avg2)
        base1 += (inf1[item]-avg1)*(inf1[item]-avg1)
        base2 += (inf2[item]-avg2)*(inf2[item]-avg2)
    if total <=0 or base1 == 0 or base2 ==0: return None
    sim = total/(math.sqrt(base1)*math.sqrt(base2))
    return [combine_value[0], combine_value[1], sim]

def build_user_list(users_dict, rated_list):
    res = list()
    for x in set(rated_list):
        res.append(users_dict[x])
    if len(res)<3:
        return None
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
        cur = str(index)+","+str(value_list[i-2])+str(value_list[i-1])
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

def cal_sim(base_dict, x1, x2):
    set1 = set(base_dict[x1].keys())
    set2 = set(base_dict[x2].keys())
    union = set1.union(set2)
    intersect = set1.intersection(set2)
    return len(intersect)/len(union)>=0.01
    

start = time.time()
inputfile = sys.argv[1]
outputfile = sys.argv[2]
cf_type = sys.argv[3]
sc = SparkContext(appName="inf553")

review = sc.textFile(inputfile).map(json.loads)
file = open(outputfile, 'w')
if cf_type == 'item_based':
    business_base = review.map(lambda x: [x['business_id'], (
        x['user_id'],x['stars'])]).groupByKey().mapValues(build_list).collect()
    base_dict = collections.defaultdict(dict)
    for item in business_base:
        base_dict[item[0]] = item[1]
    combine_value = review.map(lambda x: x['business_id']).distinct().filter(lambda x : len(base_dict[x])>=3)
    combine = combine_value.cartesian(combine_value).map(lambda x: sorted(x))
    model = combine.map(lambda x: build_model(base_dict, x)).collect()
    exist = collections.defaultdict(int)
    for item in model:
        if item != None and item[2] > 0 and exist[item[0]+","+item[1]] != 1:
            exist[item[0]+","+item[1]] = 1
            json.dump({'b1': item[0], 'b2': item[1], 'sim': item[2]}, file)
            file.write('\n')
else:
    business_list = review.map(lambda x: x['business_id']).distinct().collect()
    business_dict = collections.defaultdict(int)
    index =0 
    for x in business_list:
        business_dict[x] = index
        index += 1
    rated = review.map(lambda x: [x['user_id'], x['business_id']]).groupByKey().mapValues(
        lambda x: build_user_list(business_dict,x)).filter(lambda x: x[1] != None).sortByKey()
    
    m = len(business_list)
    min_hash_size = 480
    a_list = random.sample([i for i in range(m)], min_hash_size)
    b_list = random.sample([i for i in range(m)], min_hash_size)
    min_hash = rated.mapValues(lambda x: cal_min_hash(a_list,b_list, m, x))
    best_b = 240
    best_r = 2
    bands = min_hash.flatMapValues(lambda x: split_list(x, best_r)).map(
        lambda x: [x[1],x[0]]).groupByKey().filter(lambda x: len(x[1])>=2).map(lambda x: list(x[1]))
    
    base_dict = collections.defaultdict(dict)
    user_base = review.map(lambda x: [x['user_id'], (
        x['business_id'],x['stars'])]).groupByKey().mapValues(build_list).collect()
    for item in user_base:
        base_dict[item[0]] = item[1]
    candidates = bands.flatMap(lambda x: combination(x)).map(lambda x: tuple(x)).distinct().filter(
        lambda x: x[0]!= x[1])
        
    model = candidates.map(lambda x: build_model(base_dict, x)).filter(lambda x: x!=None).collect()
    for item in model:
        if item != None:
            json.dump({'u1': item[0], 'u2': item[1], 'sim': item[2]}, file)
            file.write('\n')


print('duration: ', time.time() - start)

