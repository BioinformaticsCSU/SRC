import codecs
import json

import os
import re

from pip._vendor.distlib.compat import raw_input


def valid_path(path):
    if path == '' or path is None or not os.path.exists(path):
        return False
    else:
        return True


if __name__ == '__main__':
    default_hdfs_dir = 'hdfs://node1002:9000/data/repeat_detection/'
    default_master = 'spark://master_ip:7077'
    default_driver_memory = '10g'
    default_total_executor_cores = '240'
    default_executor_memory = '10g'

    param_config_path = os.getcwd() + "/config/ParamConfig.json"

    # 1.Hadoop Home
    hadoop_home = raw_input("Please enter Hadoop Home: ")
    hadoop_path = hadoop_home + '/bin/hadoop'
    while not valid_path(hadoop_path):
        hadoop_home = raw_input("can not detect Hadoop program: " + hadoop_path + ".\n Please re-enter Hadoop Home: ")
        hadoop_path = hadoop_home + '/bin/hadoop'
    hdfs_dir = raw_input("Please enter HDFS work directory(default, hdfs://master_ip:9000/data/repeat_detection/): ")
    hdfs_pattern = re.compile(r'^hdfs://.*:\d+/+')
    m = hdfs_pattern.match(hdfs_dir)
    if m is None:
        hdfs_dir = default_hdfs_dir

    # 2.Spark Home
    spark_home = raw_input("Please enter Spark Home: ")
    spark_path = spark_home + '/bin/spark-submit'
    while not valid_path(spark_path):
        spark_home = raw_input("can not detect Spark program: " + spark_path + ".\n Please re-enter Spark Home: ")
        spark_path = spark_home + '/bin/spark-submit'

    master = raw_input("Please enter Spark master address(default, spark://master_ip:7077): ")
    driver_memory = raw_input("Please enter Spark driver memory size(default, 10g): ")
    total_executor_cores = raw_input("Please enter Spark total executor cores(default, 240): ")
    executor_memory = raw_input("Please enter Spark executor memory size(default, 10g): ")

    master_pattern = re.compile(r'^spark://.*:\d+$')
    m = master_pattern.match(master)
    if m is None:
        master = default_master

    number_pattern = re.compile(r'^\d+$')
    m = number_pattern.match(total_executor_cores)
    if m is None:
        total_executor_cores = default_total_executor_cores

    memory_pattern = re.compile(r'^\d+g$')
    m = memory_pattern.match(driver_memory)
    if m is None:
        driver_memory = default_driver_memory

    m = memory_pattern.match(executor_memory)
    if m is None:
        executor_memory = default_executor_memory



    # 3. RepeatClassifier
    repeatclassifier_path = os.getcwd() + "/RepeatClassifier-2.0.1/RepeatClassifier"

    # 4. write back to ParamConfig
    new_param = {}
    new_param['hadoop_home'] = hadoop_path
    new_param['hdfs_dir'] = hdfs_dir

    new_param['spark_home'] = spark_home
    new_param['master'] = master
    new_param['driver_memory'] = driver_memory
    new_param['total_executor_cores'] = total_executor_cores
    new_param['executor_memory'] = executor_memory

    new_param['repeatclassifier_path'] = repeatclassifier_path

    with codecs.open(param_config_path, 'w', 'utf-8') as outf:
        json.dump(new_param, outf, ensure_ascii=False)
        outf.write('\n')

    # 5. repeatmasker_home & rmblast_home
    repeatmasker_home = raw_input("Please enter RepeatMasker Home: ")
    repeatmasker_path = repeatmasker_home + '/RepeatMasker'
    while not valid_path(repeatmasker_path):
        repeatmasker_home = raw_input("can not detect RepeatMasker program: " + repeatmasker_path + ".\n Please re-enter RepeatMasker Home: ")
        repeatmasker_path = repeatmasker_home + '/RepeatMasker'

    rmblast_home = raw_input("Please enter RMBLAST Home: ")
    rmblast_path = rmblast_home + '/rmblastn'
    while not valid_path(rmblast_path):
        rmblast_home = raw_input(
            "can not detect RMBLAST program: " + rmblast_path + ".\n Please re-enter RMBLAST Home: ")
        rmblast_path = rmblast_home + '/rmblastn'

    # 6. modify RepModelConfig.pm in RepeatClassifier directory
    RepModelConfig_path = os.getcwd() + "/RepeatClassifier-2.0.1/RepModelConfig.pm"
    RepModelConfig_lines = []

    modifing_repeatmasker = False
    modifing_rmblast = False
    modified_repeatmasker = False
    modified_rmblast = False

    with open(RepModelConfig_path, 'r') as f_r:
        for line in f_r.readlines():
            if not modified_repeatmasker:
                if not modifing_repeatmasker:
                    if line.split('=>')[0].strip() == "'REPEATMASKER_DIR'":
                        modifing_repeatmasker = True
                else:
                    if line.split('=>')[0].strip() == "'value'":
                        # modify
                        new_line = line.split('=>')[0] + "=> '" + repeatmasker_home + "'\n"
                        modified_repeatmasker = True
                        line = new_line

            if not modified_rmblast:
                if not modifing_rmblast:
                    if line.split('=>')[0].strip() == "'RMBLAST_DIR'":
                        modifing_rmblast = True
                else:
                    if line.split('=>')[0].strip() == "'value'":
                        # modify
                        new_line = line.split('=>')[0] + "=> '" + rmblast_home + "'\n"
                        modified_rmblast = True
                        line = new_line

            RepModelConfig_lines.append(line)

    with open(RepModelConfig_path, 'w') as f_w:
        f_w.writelines(RepModelConfig_lines)
