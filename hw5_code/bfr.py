import os
# os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3.6'
# os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3.6'
import math
import time
from copy import deepcopy
import random
from pyspark import SparkContext
import types
import csv
import sys
import json

def cal_E_distance(point1, point2):
    sum_res= 0
    for i in range(len(point1)):
        sum_res += (point1[i]-point2[i])**2
    return math.sqrt(sum_res)

def cal_nearest_cluster(point, point_location, centroids):
    min_distance = float("inf")
    for center in centroids:
        center_location = center[1]
        distance =cal_E_distance(center_location,point_location)
        if min_distance > distance:
            nearest_cluster = center[0]
            min_distance = distance
    return (nearest_cluster, point)

def select_centroids(dataset, cluster_num, location_dict):
    count = 0
    sample_set = dataset.collect()
    centroids = [(count,location_dict[random.choice(sample_set)])]
    max_min_distance = {}
    for point in sample_set:
        max_min_distance[point] = cal_E_distance(location_dict[point], centroids[0][1])
    while count < cluster_num - 1:
        count += 1

        next_sample = max(max_min_distance, key= max_min_distance.get)
        next_sample = location_dict[next_sample]
        centroids.append((count, next_sample))
        for point in sample_set:
            next_distance = cal_E_distance(next_sample, location_dict[point])
            max_min_distance[point] = min(max_min_distance[point],next_distance)
    return centroids

def cal_new_centroids(cluster, location_dict, d):
    new_centroids = [0]*d
    cluster_size = len(cluster)
    for point in cluster:
        location = location_dict[point]
        for i in range(d):
            new_centroids[i] += location[i]/cluster_size
    return new_centroids

def K_means(dataset, cluster_num, location_dict):
    cluster_dict = {}
    point_inf = dataset.collect()
    for point in point_inf:
        cluster_dict[point] = point
    centroids = select_centroids(dataset, cluster_num, location_dict)
    d = len(centroids[0][1])
    change = float("inf")
    point_num = dataset.count()
    times = 0
    while change > point_num/50 and change > 100 and times < 10:
        change = 0
        cluster = dataset.map(lambda x: cal_nearest_cluster(x, location_dict[x], centroids))
        centroids = cluster.groupByKey().mapValues(lambda x: cal_new_centroids(x,location_dict, d)).collect()
        for point in cluster.collect():
            if cluster_dict[point[1]] != point[0]: 
                change += 1
            cluster_dict[point[1]] = point[0]
        times += 1
    cluster = cluster.groupByKey().map(lambda x: list(x[1])).collect()

    return cluster

def cal_std(cluster_inf):
    d = len(cluster_inf[1])
    N, SUM, SUMSQ = cluster_inf[0],cluster_inf[1],cluster_inf[2]
    dev = []
    for i in range(d):
        dev.append(math.sqrt(max(1e-10, (SUMSQ[i]/N) - (SUM[i]/N)**2)))
    return dev

def cal_statistics(cluster, location_dict, d):
    SUM = [0]* d
    SUMSQ = [0] * d
    N = 0
    for point in cluster:
        location = location_dict[point]
        for i in range(len(location)):
            SUM[i] += location[i]
            SUMSQ[i] += location[i]**2
        N+=1
    
    return (N, SUM, SUMSQ)

def cal_Mahalanobis_distace(point_location, clusters_inf, M_threshold):
    belong = -1
    min_distance = float("inf")
    for i in range(len(clusters_inf)):
        if clusters_inf[i][0] != 1:
            center = list(map(lambda x: x/clusters_inf[i][0], clusters_inf[i][1]))
            dev = cal_std(clusters_inf[i])
            distance = 0
            for j in range(len(dev)):
                distance += ((point_location[j] - center[j])/dev[j])**2
            distance = math.sqrt(distance)
            if distance < M_threshold and distance < min_distance:
                min_distance = distance
                belong = i
    return belong

def update_statistics(point, inf):
    point_location = point
    N = inf[0] + 1
    SUM = inf[1]
    SUMSQ = inf[2]
    for i in range(len(inf[1])):
        SUM[i] += point_location[i]
        SUMSQ[i] += (point_location[i]**2)

    return (N, SUM, SUMSQ)

if __name__ == "__main__":
    
    start_time = time.time()
    path = sys.argv[1]
    c_num = int(sys.argv[2])
    cluster_res= sys.argv[3]
    intermediate = sys.argv[4]
    file_list = sorted(os.listdir(path))
    sc = SparkContext()
    sc.setLogLevel("WARN")
    inter_data = []
    DS_inf, CS_inf = {}, {}
    CS_cluster, RS_cluster = {}, {}
    RS_points = []
    DS_count, RS_count, CS_count = 0, 0, 0
    DS_cluster_num, CS_cluster_num = 0, 0     
    res_dict = {}
    d, M_threshold = 0, 0 

    cur_index = 1
    for file in file_list:
        print(cur_index, time.time()- start_time)
        data = sc.textFile(path+file).map(lambda x: x.split(",")).map(lambda x: [int(x[0]), list(map(float, x[1:]))])
        location = data.collect()
        d = len(location[0][1])
        for item in location:
            res_dict[str(item[0])] = "-1"
        
        if cur_index == 1:
            cur_location_dict = {}
            for item in location:
                cur_location_dict[item[0]] = item[1]
            data = data.map(lambda x: x[0])
            sample_points = data.sample(False, 0.1)
            remian_points = data.subtract(sample_points)
            K_means_res = K_means(sample_points, c_num, cur_location_dict)
            for cluster in K_means_res:
                DS_inf[DS_cluster_num] = cal_statistics(cluster, cur_location_dict, d)
                DS_count += len(cluster)
                for point in cluster:
                    res_dict[str(point)] = str(DS_cluster_num)
                DS_cluster_num += 1
            remian_cluster = K_means(remian_points, c_num*5, cur_location_dict)
            for cluster in remian_cluster:
                if len(cluster) > 1:
                    CS_inf[CS_cluster_num] = cal_statistics(cluster, cur_location_dict, d)
                    CS_cluster[CS_cluster_num] = cluster
                    CS_cluster_num += 1
                    CS_count += len(cluster)
                else:
                    RS_cluster[cluster[0]] = cur_location_dict[cluster[0]]
                    RS_points.append(cluster[0])
                    RS_count += 1
            
            
        else:
            points_inf = data.collect()
            M_threshold = 2*math.sqrt(d)
            for point in points_inf:
                cluster_num = cal_Mahalanobis_distace(point[1], DS_inf, M_threshold)
                if cluster_num != -1:
                    DS_count += 1
                    res_dict[str(point[0])] = str(cluster_num)
                    DS_inf[cluster_num] = update_statistics(point[1], DS_inf[cluster_num])
                else:
                    cluster_num = cal_Mahalanobis_distace(point[1], CS_inf, M_threshold)
                    if cluster_num != -1:
                        CS_count += 1
                        CS_cluster[cluster_num].append(point[0])
                        CS_inf[cluster_num] = update_statistics(point[1], CS_inf[cluster_num])
                    else:
                        RS_cluster[point[0]] = point[1]
                        RS_points.append(point[0])
                        RS_count += 1
            if len(RS_points) > c_num*50 and len(RS_points) > len(res_dict)/100:
                RS_rdd = sc.parallelize(RS_points)
                K_means_on_RS = K_means(RS_rdd, c_num*5, RS_cluster)
                new_RS_cluster = {}
                new_RS_point = []
                
                for cluster in K_means_on_RS:
                    if len(cluster) >1 :
                        cluster_centroids = cal_new_centroids(cluster, RS_cluster, d)
                        cluster_num = cal_Mahalanobis_distace(cluster_centroids, CS_inf, M_threshold)
                        CS_count += len(cluster)
                        if cluster_num == -1:
                            CS_cluster[CS_cluster_num] = cluster
                            CS_inf[CS_cluster_num] = cal_statistics(cluster, RS_cluster, d)
                            CS_cluster_num += 1
                        else:
                            CS_cluster[cluster_num] += cluster
                            for point in cluster:
                                CS_inf[cluster_num] = update_statistics(RS_cluster[point], CS_inf[cluster_num])

                    else:
                        new_RS_point += cluster
                        for point in cluster:
                            new_RS_cluster[point] = RS_cluster[point]
                RS_cluster = new_RS_cluster
                RS_points = new_RS_point
                RS_count = len(RS_points)
        inter_data.append([cur_index, DS_cluster_num, DS_count, CS_cluster_num, CS_count, RS_count])
        cur_index += 1

    for cluster_index in range(len(CS_cluster)):
        cs_cluster_inf = CS_inf[cluster_index]
        cs_center = list(map(lambda x: x/cs_cluster_inf[0], cs_cluster_inf[1]))
        merage_cluster_num = cal_Mahalanobis_distace(cs_center, DS_inf, M_threshold)
        if merage_cluster_num == -1:
            for point in CS_cluster[cluster_index]:
                res_dict[str(point)] = str(DS_cluster_num)
            DS_cluster_num += 1
        else:
            for point in CS_cluster[cluster_index]:
                res_dict[str(point)] = str(merage_cluster_num)
    for point in RS_points:
        res_dict[str(point)] = '-1'
    output =  json.dumps(res_dict)

    file_1 = open(cluster_res, 'w')
    file_1.write(output)

    file_2 = open(intermediate, "w")
    wr = csv.writer(file_2)
    wr.writerow(["round_id", "nof_cluster_discard", "nof_point_discard", "nof_cluster_compression", "nof_point_compression", "nof_point_retained"])
    wr.writerows(inter_data)

    print("Duration: ", time.time()- start_time)
                




            
            


