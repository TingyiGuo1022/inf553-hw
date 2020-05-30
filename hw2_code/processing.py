from pyspark import SparkContext
import json
import os
import csv

sc = SparkContext(appName="inf553")

review_rdd = sc.textFile('review.json').map(json.loads).map(lambda item: [item['business_id'], item['user_id']])
business_rdd = sc.textFile('business.json').map(json.loads).map(lambda item: [item['business_id'], item['state']]).filter(
    lambda x: x[1] == 'NV')

user_business = review_rdd.join(business_rdd).map(lambda item: item[1][0]+","+item[0]).distinct().map(lambda x: x.split(',')).sortByKey()

with open("user_business.csv","w") as csvfile: 
    writer = csv.writer(csvfile)
    writer.writerow(["user_id","business_id"])
    writer.writerows(user_business.collect())


