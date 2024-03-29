#python 3
from random import uniform, randint
from itertools import groupby
import os
import math

if not os.path.exists('input'):
    os.makedirs('input')

SUM_OUT = 'output_sum'
MIN_OUT = 'output_min'
MAX_OUT = 'output_max'
AVG_OUT = 'output_avg'
NUM_OF_PARTITIONS = 10
NUM_OF_ELEMENTS = 5000
NUM_OF_ELEMENTS_PER_PARTITION = int(NUM_OF_ELEMENTS / NUM_OF_PARTITIONS)

for d in [SUM_OUT, MIN_OUT, MAX_OUT, AVG_OUT]:
    if not os.path.exists(d):
        os.makedirs(d)

input_data = [(randint(1, 100), round(uniform(-100, 5000), 6)) for n in range(NUM_OF_ELEMENTS)]
for n in range(NUM_OF_PARTITIONS):
    with open('input/input_{0}.txt'.format(n), 'w') as file:
        for m in range(NUM_OF_ELEMENTS_PER_PARTITION):
            o = input_data[n * NUM_OF_ELEMENTS_PER_PARTITION + m]
            value = '%.6f' % o[1]
            file.write("{0} {1}\n".format(o[0], value))

i = 0
result_sum = {}
result_min = {}
result_max = {}
result_avg = {}
for key, group in groupby(sorted(input_data), lambda x: x[0]):
    g = [x[1] for x in group]
    result_sum[key] = sum(g)
    result_min[key] = min(g)
    result_max[key] = max(g)
    result_avg[key] = sum(g) / float(len(g))


def write_output(dir_name, data):
    keys = list(sorted(data.keys()))
    NUM_OF_ELEMENTS_PER_PARTITION = int(math.ceil(float(len(keys)) / NUM_OF_PARTITIONS))
    for n in range(NUM_OF_PARTITIONS):
        with open('{0}/output_{1}.txt'.format(dir_name, n), 'w') as file:
            for m in range(NUM_OF_ELEMENTS_PER_PARTITION):
                index = n * NUM_OF_ELEMENTS_PER_PARTITION + m
                if index >= len(keys):
                    return
                value = '%.6f' % data[keys[index]]
                file.write("{0} {1}\n".format(keys[index], value))

write_output(SUM_OUT, result_sum)
write_output(MIN_OUT, result_min)
write_output(MAX_OUT, result_max)
write_output(AVG_OUT, result_avg)
