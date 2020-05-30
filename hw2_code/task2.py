from pyspark import SparkContext
import collections
from itertools import combinations
import os
from operator import add
import sys
import time
import math

start_time = time.time()
# build itemset from frequent itemsets
def find_pair(itemsets, cur_len):
    ans = set()
    for s1 in range(1,len(itemsets)):
        for s2 in range(s1):
            one = sorted(list(set(itemsets[s1]+itemsets[s2])))
            if len(one) == cur_len :
                ans.add(','.join(one))
    ans = map(lambda x: x.split(','), ans)
    return ans

# calculate the frequent of an itemset
def Count_pair(item_set, item_dict):
    ans = item_dict[item_set[0]]
    for i in range(1, len(item_set)):
        ans = ans & item_dict[item_set[i]]
    return len(ans)

# A_priori Alogrithm
def A_Priori(iter,total,support):
    count = 0
    baskets = []
    item_dict = collections.defaultdict(set)
    index = 0
    for item in iter:
        baskets.append(list(item))
        for i in item:
            item_dict[i].add(index)
        index += 1
        count += 1
    items = item_dict.keys()
    cur_support = math.ceil(count*support/total)
    # counter = {} 
    # find the frequent item
    # for item in items:
    #     if item in counter.keys(): counter[item] += 1
    #     else: counter[item] = 1
    dp = []
    dp.append([])
    for item in set(items):
        if len(item_dict[item])>= cur_support:
            dp[0].append([item])
    # for key in counter.keys():
    #     if(counter[key] >= cur_support):
    #         dp[0].append([key])
    # find frequent itemsets based on frequent item
    while len(dp[-1]) > 0:
        possible_pair = find_pair(dp[-1], len(dp)+1)
        dp.append([])
        for item_set in possible_pair:
            if Count_pair(item_set, item_dict) >= cur_support:
                 dp[-1].append(item_set)
    return dp[:-1]

# cal the frequent of candidates in all baskets
def cal_total(basket, candidates_set):
    count = []
    for item in candidates_set:
        if set(item) == set(item) & set(basket):
            count.append([item,1])
    return count

# main 
k_filter = int(sys.argv[1])
support = int(sys.argv[2])
inputfile = sys.argv[3]
outputfile = sys.argv[4]

sc = SparkContext(appName="inf553")
# read file, skip first line
review_rdd = sc.textFile(inputfile).zipWithIndex().filter(lambda x: x[1]!= 0).map(lambda x: x[0].split(','))
# build basket
basket = review_rdd.groupByKey().map(lambda b: list(set(b[1]))).filter(lambda x: len(x) > k_filter)
basket_num = basket.count()
# run son alogrithm, use each chunk as a sample
candidates = basket.mapPartitions(lambda p: A_Priori(p, basket_num, support)).flatMap(
    lambda x: x).map(lambda x: ",".join(x)).distinct().map(lambda x: x.split(',')).sortBy(lambda x: x).sortBy(lambda x: len(x))
# output = candidates.map(lambda item: ",".join(item)).distinct().count()
# print(output)
candidates_set = candidates.collect()
# gain frequent itemsets
fequent = basket.flatMap(lambda x : cal_total(x, candidates_set)).map(lambda x: [','.join(x[0]), x[1]]).reduceByKey(add).filter(
    lambda x: x[1] >= support).sortByKey().map(lambda x: x[0].split(',')).sortBy(len)
#  write results into file
file = open(outputfile,'w')
file.write('Candidates:\n')

for i in range(len(candidates_set)):
    file.write("('"+"', '".join(candidates_set[i])+"')")
    if i == len(candidates_set)-1 or len(candidates_set[i]) != len(candidates_set[i+1]): file.write('\n\n')
    else: file.write(',')

file.write('Frequent Itemsets:\n')
frequent_set = fequent.collect()
for i in range(len(frequent_set)):
    file.write("('"+"', '".join(frequent_set[i])+"')")
    if i < len(frequent_set)-1 and len(frequent_set[i]) != len(frequent_set[i+1]): file.write('\n\n')
    elif i != len(frequent_set)-1 : file.write(',')
file.flush()
file.close()
print("Duration: %s" % int(time.time() - start_time))