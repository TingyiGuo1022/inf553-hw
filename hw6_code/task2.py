from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
from datetime import datetime
import json
import random
import csv
import math
import collections
import os

def cal_hash_result(x_str, length):
    ans = []
    for i in range(0, 100):
        cur_sum = 0
        for j in range(len(x_str)):
            cur_sum += ord(x_str[j])*(i+j)
        bin_res = "{0:b}".format(cur_sum%pow(2,math.floor(math.log(length, 2))))
        index = len(bin_res)-1
        zero_count = 0
        while bin_res[index] == '0' and index >= 0:
            zero_count += 1
            index -=1
        ans.append((i,zero_count))
    return ans
    

def cal_result(rdd, output_file):
    
    file = open(output_file, "a")
    wr = csv.writer(file)
    Time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_len = rdd.count()
    ground_truth = rdd.distinct().count()
    hash_result = rdd.filter(lambda x: x!= "").flatMap(lambda x: cal_hash_result(x,total_len)).groupByKey().mapValues(
        lambda x: pow(2, max(x)-min(x))).map(lambda x: x[1]).collect()
    hash_result = sorted(list(hash_result))[1:-1]
    estimation = math.ceil(sum(hash_result)/len(hash_result))
    accurate = 1-abs(ground_truth - estimation)/ground_truth
    ans = [Time, ground_truth, estimation, accurate]
    wr.writerow(ans)
    file.close()
    


if __name__ == "__main__":
  
    port = int(sys.argv[1])
    output_file = sys.argv[2]
    
    file = open(output_file, 'w')
    wr = csv.writer(file)
    wr.writerow(["Time", "Ground Truth", "Estimation"])
    file.close()
    window_duration = 30
    slide_duration = 10
    sc = SparkContext()
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)
    batchs = ssc.socketTextStream("localhost", port)
    data = batchs.window(windowDuration=window_duration, slideDuration=slide_duration)
    cur_data = data.map(json.loads).map(lambda y: y.get("city", ""))
    cur_data.foreachRDD(lambda x : cal_result(x, output_file))
    cur_data.pprint(num= 1)


    ssc.start()
    ssc.awaitTermination(timeout= 700)
