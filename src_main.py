#!/usr/bin/env python
# coding=utf-8
import codecs
import json
import os
import argparse
import random

from pyspark import SparkConf, SparkContext


def singleLine2KeyValue(line):
    new2origin_header_map = new2origin_header_map_broadcast.value
    parts = line.split('\t')
    unique_key = int(parts[0][1:])
    origin_header = new2origin_header_map[str(unique_key)]
    # add a random number 0~255, 8 bit
    #random_num = random.randint(0, 255)
    #unique_key += '$' + str(random_num)
    return (unique_key, '>' + origin_header + '#' + parts[1] + '#')

def add_prefix_partition(index, line_list):
    line_list = list(line_list)
    #print("partition=%d, line list=" % index, line_list)
    for i in range(len(line_list)):
        line_list[i] = str(index) + line_list[i]
    return line_list

def hash_code(s):
    h = 0
    if len(s) > 0:
        for item in s:
            h = 31 * h + ord(item)
        return h
    else:
        return 0


if __name__ == '__main__':
    # 1.parse args
    parser = argparse.ArgumentParser(description='run Spark RepeatClassifier...')
    parser.add_argument('-f', metavar='fasta path',
                        help='input fasta path')
    parser.add_argument('-o', metavar='output dir',
                        help='input output dir')
    parser.add_argument('--partition_num', metavar='Spark partition number',
                        help='Input spark partition number')
    parser.add_argument('-p', metavar='repeatclassifier path',
                        help='input repeatclassifier path')
    args = parser.parse_args()

    fasta_path = args.f
    output_dir = args.o
    partition_num = int(args.partition_num)
    program_path = args.p

    (dir, filename) = os.path.split(fasta_path)
    new2origin_header_map_path = output_dir + '/' + filename + '.header_map.json'
    new2origin_header_map = json.load(open(new2origin_header_map_path, 'r'))

    # 2.configure SparkConf
    conf = SparkConf().setAppName("Spark repeat classifier")
    conf.set("spark.driver.maxResultSize", "20g")
    conf.set("spark.shuffle.reduceLocality.enabled", False)

    sc = SparkContext(conf=conf)

    fasta_file = sc.textFile(fasta_path)

    # broadcast new2origin_header_map
    new2origin_header_map_broadcast = sc.broadcast(new2origin_header_map)

    ## ---------------------part01: divided file into sub file
    reads_fa = fasta_file.filter(lambda line: line.startswith(">"))\
        .map(lambda line: singleLine2KeyValue(line)).partitionBy(partition_num, lambda key: int(key))
        #.coalesce(partition_num, shuffle=True)#.repartition(partition_num)

    reads_fa = reads_fa.values().mapPartitionsWithIndex(add_prefix_partition)

    command = "python " + os.getcwd() + "/save_and_classifier.py" + " -o " + output_dir + " -f " + filename + " -p " + program_path
    pipeRDD = reads_fa.pipe(command)


    contigNames = []
    contigs = {}
    for line in pipeRDD.collect():
        parts = line.split('$')
        if len(parts) > 1:
            contigName = parts[0][1:]
            contig = parts[1]
            contigNames.append(contigName)
            contigs[contigName] = contig

    #headers = sorted(headers, reverse=False)

    final_classified_path = output_dir + '/' + filename + '.spark.classified'
    if os.path.exists(final_classified_path):
        os.remove(final_classified_path)

    with open(final_classified_path, "w") as f_save:
        for contigName in contigNames:
            contig = contigs[contigName]
            f_save.write(">" + contigName + "\n")
            f_save.write(contig + "\n")
    f_save.close()

    sc.stop()