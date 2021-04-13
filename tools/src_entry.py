import argparse
import codecs
import json
import os
import time

def read_fasta(fasta_path):
    contigs = {}
    contigNames = []
    with open(fasta_path, "r") as f_r:
        contigName = ''
        contigseq = ''
        for line in f_r:
            if line.startswith('>'):
                if contigName != '' and contigseq != '':
                    contigs[contigName] = contigseq
                    contigNames.append(contigName)
                contigName = line.strip()[1:]
                contigseq = ''
            else:
                contigseq += line.strip().upper()
        contigs[contigName] = contigseq
        contigNames.append(contigName)
    f_r.close()
    return contigNames, contigs

def read_classified_fasta(fasta_path):
    unclass_class_names_map = {}
    contigs = {}
    contigNames = []
    with open(fasta_path, "r") as f_r:
        contigName = ''
        contigseq = ''
        node_name = ''
        class_name = ''
        node_info = ''
        for line in f_r:
            if line.startswith('>'):
                if contigName != '' and contigseq != '':
                    contigs[contigName] = contigseq
                    contigNames.append(contigName)
                contigName = line.strip()[1:]

                classified_index = contigName.find('#')
                blank_index = contigName.find(' ')
                node_name = contigName[0:classified_index]
                if blank_index != -1:
                    class_name = contigName[classified_index+1: blank_index]
                    node_info = ' ' + contigName[blank_index+1:]
                else:
                    class_name = contigName[classified_index + 1: ]
                    node_info = ''
                unclass_name = node_name + node_info
                unclass_class_names_map[unclass_name] = contigName

                contigseq = ''
            else:
                contigseq += line.strip().upper()
        contigs[contigName] = contigseq
        contigNames.append(contigName)
    f_r.close()
    return (unclass_class_names_map, contigNames, contigs)

def transform2SingleLine(tmp_fasta_path, contigs, partition_num):
    # sort contigs by length
    items_array = contigs.items()
    sorted_items_array = sorted(items_array, key=lambda x: len(x[1]), reverse=True)

    # replace header and record
    origin2new_header_map = {}
    new2origin_header_map = {}
    node_index = 0
    with open(tmp_fasta_path, 'w') as f_w:
        read_from_start = True
        read_from_end = False
        i = 0
        j = len(sorted_items_array) - 1
        while i <= j:
            # read from file start
            if read_from_start:
                origin_header = sorted_items_array[i][0]
                i += 1
            if read_from_end:
                origin_header = sorted_items_array[j][0]
                j -= 1

            new_header = str(node_index)
            node_index += 1
            origin2new_header_map[origin_header] = new_header
            new2origin_header_map[new_header] = origin_header
            contig = contigs[origin_header]
            line = '>' + new_header + '\t' + contig + '\n'
            f_w.write(line)
            if node_index % partition_num == 0:
                # reverse
                read_from_end = bool(1-read_from_end)
                read_from_start = bool(1-read_from_start)

        # for item in sorted_items_array:
        #     origin_header = item[0]
        #     new_header = str(node_index)
        #     node_index += 1
        #     origin2new_header_map[origin_header] = new_header
        #     new2origin_header_map[new_header] = origin_header
        #     contig = contigs[origin_header]
        #     line = '>' + new_header + '\t' + contig + '\n'
        #     f_w.write(line)
    f_w.close()
    return tmp_fasta_path, new2origin_header_map

def transform2SingleLine_direct(tmp_fasta_path, contigs, partition_num):
    # sort contigs by length
    items_array = contigs.items()
    sorted_items_array = sorted(items_array, key=lambda x: len(x[1]), reverse=True)

    # replace header and record
    new2origin_header_map = {}
    node_index = 0
    with open(tmp_fasta_path, 'w') as f_w:
        i = 0
        j = len(sorted_items_array) - 1
        while i <= j:
            if i % partition_num == 0 and i != 0:
                node_index += 1
            origin_header = sorted_items_array[i][0]
            new_header = str(node_index)
            new2origin_header_map[new_header] = origin_header
            contig = contigs[origin_header]
            line = '>' + new_header + '\t' + contig + '\n'
            f_w.write(line)
            i += 1
    f_w.close()
    return tmp_fasta_path, new2origin_header_map


def check_program_installation(param):
    hadoop_home = param['hadoop_home']
    spark_home = param['spark_home']
    repeatclassifier_path = param['repeatclassifier_path']
    hadoop_path = hadoop_home + '/bin/hadoop'
    spark_path = spark_home + '/bin/spark-submit'
    if not os.path.exists(hadoop_path):
        raise SystemExit('Hadoop path not exist: ' + hadoop_path)
    if not os.path.exists(spark_path):
        raise SystemExit('Spark path not exist: ' + spark_path)
    if not os.path.exists(repeatclassifier_path):
        raise SystemExit('RepeatClassifier path not exist: ' + repeatclassifier_path)



if __name__ == '__main__':
    # 1.parse args
    parser = argparse.ArgumentParser(description='run Spark RepeatClassifier ...')
    parser.add_argument('-f', metavar='fasta path',
                        help='input fasta path')
    parser.add_argument('-o', metavar='output dir',
                        help='input output dir')
    args = parser.parse_args()

    fasta_path = args.f
    output_dir = args.o

    file_removed = []
    # program_path = '/public/home/hpc194701009/repeat_detect_tools/RepeatModeler-2.0.1/RepeatClassifier'
    # main_dir = '/public/home/hpc194701009/Liao_Results/classifier'
    # output_dir = main_dir
    # species = ['RepeatLib_ant.fa', 'RepeatLib_dmel.fa', 'RepeatLib_Gallus.fa', 'RepeatLib_Soybean.fa', 'RepeatLib_Mouse.fa', 'RepeatLib_hg38.fa']
    # #species = ['RepeatLib_Mouse.fa', 'RepeatLib_hg38.fa']
    # total_cores = ['240']
    # #total_cores = ['15', '30', '60', '120', '240']
    # for specie in species:
    #     for total_executor_cores in total_cores:
    #         file_removed = []
    #         fasta_path = main_dir + '/' + specie

    ## ---------------------run spark classifier program -----------------------------
    print("Start Running Spark RepeatClassifier")
    starttime = time.time()
    param_config_path = os.getcwd() + "/config/ParamConfig.json"

    # read param config
    with open(param_config_path, 'r') as load_f:
        param = json.load(load_f)
    load_f.close()

    check_program_installation(param)

    total_executor_cores = param['total_executor_cores']
    # 1.1 transform fasta to single line
    tmp_fasta_path = fasta_path + ".tmp"
    contigNames, contigs = read_fasta(fasta_path)
    #fasta_path, new2origin_header_map = transform2SingleLine_direct(tmp_fasta_path, contigs, int(total_executor_cores))
    fasta_path, new2origin_header_map = transform2SingleLine(tmp_fasta_path, contigs, int(total_executor_cores))
    (dir, filename) = os.path.split(fasta_path)
    new2origin_header_map_path = output_dir + '/' + filename + '.header_map.json'
    with codecs.open(new2origin_header_map_path, 'w', 'utf-8') as outf:
        json.dump(new2origin_header_map, outf, ensure_ascii=False)
        outf.write('\n')
    file_removed.append(new2origin_header_map_path)

    # 1.2 upload fasta file to hdfs
    hadoop_home = param['hadoop_home']
    hdfs_dir = '/data/spark_classifier/RepeatScout/'
    tmp_rm_command = hadoop_home + '/bin/hadoop fs -rm -r ' + hdfs_dir + '/' + os.path.basename(fasta_path)
    print(tmp_rm_command)
    os.system(tmp_rm_command)

    upload_command = hadoop_home + '/bin/hadoop fs -put ' + fasta_path + ' ' + hdfs_dir
    print(upload_command)
    os.system(upload_command)
    fasta_path = hdfs_dir + filename

    # spark param
    spark_home = param['spark_home']
    master = param['master']
    driver_memory = param['driver_memory']

    executor_memory = param['executor_memory']
    # program param
    classifier_program = os.getcwd() + "/src_main.py"
    program_path = param['repeatclassifier_path']

    spark_command = spark_home + '/bin/spark-submit --master ' + master + ' --driver-memory ' + driver_memory \
                    + ' --total-executor-cores ' + total_executor_cores + ' --executor-memory ' + executor_memory \
                    + ' ' + classifier_program + ' -f ' + fasta_path + ' -o  ' + output_dir + ' --partition_num ' + str(int(total_executor_cores)) + ' -p ' + program_path
    # spark_command = spark_home + '/bin/spark-submit --master ' + master + ' --driver-memory ' + driver_memory + ' --total-executor-cores ' + total_executor_cores + ' --executor-memory ' + executor_memory + ' ' + kmer_count_program + ' -f ' + hdfs_file + ' -o ' + hdfs_output_file + ' -m ' + Hifi_reads_mode + ' -k ' + k_num + ' -g ' + allowed_gap_size + ' --min_contig_length ' + str(min_contig_length) + ' --partition_num ' + str(20 * int(total_executor_cores))
    #print(spark_command)
    os.system(spark_command)
    endtime = time.time()
    dtime = endtime - starttime
    print("total_executor_cores: %s, Spark classifier running time: %.8s s" % (total_executor_cores, dtime))
    #print("species: %s, total_executor_cores: %s, Spark classifier running time: %.8s s" % (specie, total_executor_cores, dtime))

    for f in file_removed:
        os.remove(f)



