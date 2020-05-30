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

def gainwords(stopwords, text):
    total = " ".join(text)
    text = re.sub('[^A-Za-z]+', ' ', total).split()
    freq = collections.defaultdict(int)
    max_freq = 0
    for x in text:
        x = x.lower()
        if x not in stopwords:
            freq[x] += 1
            max_freq =max(max_freq, freq[x])
    ans = list()
    for x in freq.keys():
        if freq[x]>=len(text)*0.000001:
            ans.append([x, freq[x]/max_freq])
    return ans

def cal_TF_IDF(IDF_dict, tf):
    ans = list()
    for item in tf:
        ans.append([item[0], item[1]*IDF_dict[item[0]]])
    ans = sorted(ans, key = lambda x: x[1], reverse= True)[:min(200, len(ans))]
    res = list()
    for item in ans:
        res.append(item[0])
    return res

def build_profile(profile_dict, value_list):
    ans = list()
    freq = collections.defaultdict(int)
    for item in value_list:
        for word in profile_dict[item]:
            freq[word] +=1
    ans = sorted(list(freq.keys()), key = lambda x: freq[x], reverse = True)[:min(400, len(freq.keys()))]
    return ans

start = time.time()
inputfile = sys.argv[1]
outputfile = sys.argv[2]
stop = sys.argv[3]
sc = SparkContext(appName="inf553")
stopword = sc.textFile(stop).collect()
review = sc.textFile(inputfile).map(json.loads).map(lambda x: [x['user_id'], [x['business_id'], x['text']]])
user_business = review.map(lambda x: [x[0],x[1][0]]).groupByKey()
TF = review.map(lambda x: x[1]).groupByKey().mapValues(lambda x: gainwords(stopword, x))
total_business = TF.count()
IDF = TF.flatMap(lambda x: x[1]).map(lambda x: [x[0],1]).reduceByKey(add).map(
    lambda x: [x[0],math.log2(total_business/x[1])]).collect()
IDF_dict = dict()
for item in IDF:
    IDF_dict[item[0]] = item[1]
TF_IDF = TF.mapValues(lambda x: cal_TF_IDF(IDF_dict,x)).collect()
file = open(outputfile, 'w')
business_profile_dict = collections.defaultdict(set)
for item in TF_IDF:
    json.dump({'type': 'business', 'name': item[0], 'profile': item[1]}, file)
    business_profile_dict[item[0]] = item[1]
    file.write('\n')
user_profile = user_business.mapValues(lambda x: build_profile(business_profile_dict, x)).map(
        lambda item: json.dumps({'type': 'user', 'name': item[0], 'profile': item[1]})).collect()
for item in user_profile:
    file.write(item)
    file.write('\n')
print("duration: ", time.time()-start)
