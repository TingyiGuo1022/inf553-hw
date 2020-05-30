from functools import reduce
from pyspark import SparkContext
import sys
import time
from itertools import combinations
from operator import add
import math
import random
import collections
from copy import deepcopy

class Node:
    def __init__(self, value, level):
        self.val = value
        self.level = level
        self.childrens = list()
        self.short_path_num = 0
# cal credit
def cal_credit(node, tree):
    if len(tree[node].childrens) == 0: return 1
    sum_credit = 1
    for child in tree[node].childrens:
        child_credit = cal_credit(child, tree)*(tree[node].short_path_num/tree[child].short_path_num)
        sum_credit += child_credit
    return sum_credit
# cal betweenness 
def cal_bet(cur_edges, node):
    q = [node]
    level = 0
    tree = {node: Node(node, level)}
    tree[node].short_path_num = 1
    while q:
        cur = q.pop(0)
        for n in cur_edges[cur]:
            if n not in tree:
                q.append(n)
                child = Node(n, tree[cur].level+1)
                child.short_path_num += tree[cur].short_path_num
                tree[cur].childrens.append(n)
                tree[n] = child
            elif n in tree and tree[cur].level == tree[n].level-1:
                tree[n].short_path_num += tree[cur].short_path_num
                tree[cur].childrens.append(n)
    ans = []
    for item in tree.keys():
        for c in tree[item].childrens:
            c_credit = cal_credit(c, tree)*(tree[item].short_path_num/tree[c].short_path_num)
            btw = [tuple(sorted([item, c])), c_credit]
            ans.append(btw)
    
    return ans

# build commuities based on current edges
def build_commuity(x):
    commuities = []
    users = set(x.keys())
    visited = set()
    while len(users) != 0:
        cur_node = users.pop()
        commuity = set()
        q = [cur_node]
        while q:
            prev = q.pop(0)
            commuity.add(prev)
            for n in x[prev]:
                if n not in visited: 
                    q.append(n)
                    commuity.add(n)
                    visited.add(n)
        commuities.append(commuity)
        users = users - commuity
    return commuities
    
def cal_modularity(cur_edges, m, edges, count_dict):
    commuities = build_commuity(cur_edges)
    G = 0
    for commuity in commuities:
        if len(commuity) == 1:
            continue
        g = 0
        for i in commuity:
            for j in commuity:
                if i != j:
                    ki, kj = count_dict[i], count_dict[j]
                    if i in edges[j]: Aij = 1
                    else: Aij = 0
                    g += (Aij - (ki*kj)/(2*m))
        G += g
    
    return G/(2*m)



start = time.time()
threshold = int(sys.argv[1])
inputfile = sys.argv[2]
outputfile_1 = sys.argv[3]
outputfile_2 = sys.argv[4]
sc = SparkContext()
sc.setLogLevel("WARN")
data = sc.textFile(inputfile).map(lambda x: x.split(',')).filter(lambda x: x[0] != 'user_id')
user_pairs = data.map(lambda x: [x[1],x[0]]).groupByKey().mapValues(sorted).mapValues(lambda x: combinations(x, 2)).flatMap(lambda x: x[1]).map(
    lambda x: [x, 1]).reduceByKey(add).map(lambda x: [x[0][0],x[0][1], x[1]]).filter(lambda x: x[2]>=threshold)
users = user_pairs.flatMap(lambda x: x[:2]).distinct()
edges_rdd = user_pairs.collect()
edges = collections.defaultdict(list)
m = 0
for item in edges_rdd:
    m += 1
    edges[item[0]].append(item[1])
    edges[item[1]].append(item[0])
count_dict= {}
for item in edges.keys(): count_dict[item] = len(edges[item])
# cal betweenness
betweenness = users.flatMap(lambda x: cal_bet(edges, x)).reduceByKey(add).mapValues(lambda x: x/2).sortByKey().sortBy(lambda x : -x[1]).collect()
file = open(outputfile_1, 'w')
for item in betweenness: file.write("("+"'"+item[0][0]+"'"+", "+"'"+item[0][1]+"'"+")"+", "+str(item[1])+"\n")
file.flush()
file.close()
#cal modularity
cur_edges = deepcopy(edges)
cur_G= float("-inf")
max_edges, max_G = edges, cur_G
count = m
while count> 0 :
    betweenness = users.flatMap(lambda x: cal_bet(cur_edges, x)).reduceByKey(add).mapValues(lambda x: x/2).sortByKey().sortBy(lambda x : -x[1])
    drop_edge = betweenness.take(1)[0]
    cur_edges[drop_edge[0][0]].remove(drop_edge[0][1])
    cur_edges[drop_edge[0][1]].remove(drop_edge[0][0])
    cur_G = cal_modularity(cur_edges, m, edges, count_dict)
    if cur_G > max_G:
        max_G, max_edges = cur_G, deepcopy(cur_edges)
    count -= 1


commuities = build_commuity(max_edges)
commuities = map(lambda x: sorted(x), commuities)
commuities = sorted(commuities, key = lambda x: [len(x), x[0]])

file_2 = open(outputfile_2, 'w')
for item in commuities:
    file_2.write("'"+"', '".join(item)+"'")
    file_2.write('\n')
print("Duration:", time.time()-start)
