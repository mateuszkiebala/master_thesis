from random import randint
from itertools import groupby
import os
import math

input_R = 'input_R'
input_T = 'input_T'
for d in [input_R, input_T]:
    if not os.path.exists(d):
        os.makedirs(d)

if not os.path.exists('output'):
    os.makedirs('output')

NUM_OF_PARTITIONS = 10
NUM_OF_R_ELEMENTS = 7000
NUM_OF_T_ELEMENTS = 3000

def write_input(dir_name, data, num_of_elements):
    for n in range(NUM_OF_PARTITIONS):
        with open('{0}/input_{1}.txt'.format(dir_name, n), 'w') as file:
            for m in range(num_of_elements):
                o = data[n * num_of_elements + m]
                file.write("{0} {1}\n".format(o[0], o[1]))


def write_output(dir_name, data):
    data = list(sorted(data))
    elems_on_part = int(math.ceil(len(data) / NUM_OF_PARTITIONS))
    for n in range(NUM_OF_PARTITIONS):
        with open('{0}/output_{1}.txt'.format(dir_name, n), 'w') as file:
            for m in range(elems_on_part):
                index = n * elems_on_part + m
                if index >= len(data):
                    return
                o = data[index]
                file.write("{0} {1}\n".format(o[0], o[1]))


data_R = [(randint(1, 100), randint(-100, 5000)) for n in range(NUM_OF_R_ELEMENTS)]
data_T = [(randint(50, 150), randint(-100, 5000)) for n in range(NUM_OF_T_ELEMENTS)]
write_input(input_R, data_R, int(NUM_OF_R_ELEMENTS / NUM_OF_PARTITIONS))
write_input(input_T, data_T, int(NUM_OF_T_ELEMENTS / NUM_OF_PARTITIONS))

keys_T = set(map(lambda p: p[0], data_T))
result = [(k, v) for k, v in data_R if k in keys_T]
write_output('output', result)
