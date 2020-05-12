import os
from pyspark import SparkContext
import sys
import json
import time
import copy
import itertools
from collections import Counter
import binascii

# spark-submit task1.py <first_json_path> <second_json_path> <output_file_path>
# spark-submit task1.py business_first.json business_second.json task1.csv

start_time = time.time()
first_json_path = sys.argv[1]
second_json_path = sys.argv[2]
output_file_path = sys.argv[3]

sc = SparkContext('local[*]', 'task1')
sc.setLogLevel('ERROR')

# f = open(first_json_path,'r')
# cityList = []
# for line in f:
#     try:
#         city = json.loads(line)['city']
#         if len(city) > 0:
#             cityList.append(city)
#     except:
#         pass
#
# citySet = set(cityList)
# print(len(cityList))
# print(len(citySet))
#
#
# f = open(second_json_path,'r')
# results = []
# for line in f:
#     try:
#         city = json.loads(line)['city']
#         if len(city) > 0:
#             if city in citySet:
#                 results.append(1)
#             else:
#                 results.append(0)
#         else:
#             results.append(0)
#     except:
#         results.append(0)


prime_numbers = [77743, 92479, 117373, 127081, 139487, 154127, 170141, 194933, 213859, 214433, 228023, 230239, 228539,
                 470551]

m = 214433


def h1(x):
    return (77734 * x + 92479) % m


def h2(x):
    return ((117373 * x + 154127) % 470551) % m


def h3(x):
    return (194933 * x + 230239) % m


def h4(x):
    return (127081 * x + 170141) % m


def digits1(x):
    resultset = set()
    resultset.add(h1(x))
    resultset.add(h2(x))
    resultset.add(h3(x))
    resultset.add(h4(x))
    return resultset


def combinesets(seta, setb):
    setc = seta.copy()
    setc.update(setb)
    return setc

filterList = [0] * m

rdd0 = sc.textFile(first_json_path)
digits = rdd0.map(lambda x: json.loads(x)['city'])\
    .filter(lambda x: len(x) > 0)\
    .map(lambda x: int(binascii.hexlify(x.encode('utf8')), 16))\
    .map(lambda x: (1, digits1(x)))\
    .reduceByKey(lambda a, b: combinesets(a, b))\
    .map(lambda x: x[1])\
    .collect()

for d in digits[0]:
    filterList[d] = 1


output = open(output_file_path, 'w')

secondf = open(second_json_path, 'r')

# results2 = []
for line in secondf:
    try:
        city = json.loads(line)['city']
        cityint = int(binascii.hexlify(city.encode('utf8')), 16)
        digits = digits1(cityint)
        appear = 1
        for d in digits:
            if filterList[d] == 1:
                continue
            else:
                appear = 0
                output.write('0 ')
                # results2.append(0)
                break
        if appear == 1:
            output.write('1 ')
            # results2.append(1)
    except:
        output.write('0 ')
        # results2.append(0)


# def perf_measure(y_actual, y_hat):
#     TP = 0
#     FP = 0
#     TN = 0
#     FN = 0
#
#     for i in range(len(y_hat)):
#         if y_actual[i]==y_hat[i]==1:
#            TP += 1
#         if y_hat[i]==1 and y_actual[i]!=y_hat[i]:
#            FP += 1
#         if y_actual[i]==y_hat[i]==0:
#            TN += 1
#         if y_hat[i]==0 and y_actual[i]!=y_hat[i]:
#            FN += 1
#
#     return(TP, FP, TN, FN)
#
#
# print(perf_measure(results, results2))

print('Duration: ' + str(time.time() - start_time))
