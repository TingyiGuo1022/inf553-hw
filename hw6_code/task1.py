from pyspark import SparkContext
import sys
import time
import json
import random
import binascii
import csv

def cal_hash_res(a_list, b_list, m, num):
    ans =[]
    for i in range(len(a_list)):
        ans.append((a_list[i]*num + b_list[i])%m)
    return ans

def predict(a_list, b_list, m, num, bit_array):
    if num == "": return 0
    num = int(binascii.hexlify(num.encode("utf8")), 16)
    hash_res = cal_hash_res(a_list, b_list, m, num)
    for index in hash_res:
        if bit_array[index] != 1: return 0
    return 1

if __name__ == "__main__":
    start_time = time.time()
    train_file = sys.argv[1]
    predict_file = sys.argv[2]
    output_file = sys.argv[3]
    sc= SparkContext()
    sc.setLogLevel("WARN")
    
    train_city = sc.textFile(train_file).map(json.loads).map(lambda x: "" if "city" not in x else x["city"]).filter(lambda x: x != "").distinct().map(
        lambda s: int(binascii.hexlify(s.encode("utf8")), 16))
    test_city = sc.textFile(predict_file).map(json.loads).map(lambda x: "" if "city" not in x else x["city"])
    hash_func_size = 5
    m = train_city.count()*50
    a_list = random.sample([i for i in range(m)], hash_func_size)
    b_list = random.sample([i for i in range(m)], hash_func_size)
    bit_array = [0]*m
    train_res = train_city.flatMap(lambda x: cal_hash_res(a_list, b_list, m, x)).distinct().collect()
    for num in train_res: bit_array[num] = 1
    predict_res =  test_city.map(lambda x: predict(a_list, b_list, m, x, bit_array)).collect()
    file = open(output_file, "w")
    print(len(predict_res))
    wr = csv.writer(file, delimiter=' ')
    wr.writerow(predict_res)
    print("Total Duration: ", time.time()-start_time)
