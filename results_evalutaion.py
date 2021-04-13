import argparse
import os
import random


def read_fasta(fasta_path):
    contignames = []
    contigs = {}
    with open(fasta_path, 'r') as rf:
        contigname = ''
        contigseq = ''
        for line in rf:
            if line.startswith('>'):
                if contigname != '' and contigseq != '':
                    contigs[contigname] = contigseq
                    contignames.append(contigname)
                contigname = line.strip()[1:].split(' ')[0]
                contigseq = ''
            else:
                contigseq += line.strip().upper()
        contigs[contigname] = contigseq
        contignames.append(contigname)
    rf.close()
    return contignames, contigs

def get_classifier_name(contigname):
    main_class = ''
    sub_class = ''
    parts = contigname.split("#")
    class_name = parts[1].split(" ")[0]
    c_parts = class_name.split("/")
    if len(c_parts) > 0:
        main_class = c_parts[0]
        if len(c_parts) > 1:
            sub_class = c_parts[1]
    return parts[0], main_class, sub_class

def getReverseSequence(sequence):
    base_map = {'A': 'T', 'T': 'A', 'C': 'G', 'G': 'C'}
    res = ''
    length = len(sequence)
    i = length - 1
    while i >= 0:
        base = sequence[i]
        if base not in base_map.keys():
            base = 'N'
        else:
            base = base_map[base]
        res += base
        i -= 1
    return res

if __name__ == '__main__':
    # w_fasta_path = '/public/home/hpc194701009/Liao_Results/classifier_multi/repeatscout_ant_r_3.fa'
    # fasta_path = '/public/home/hpc194701009/Liao_Results/classifier_multi/repeatscout_ant.fa'
    # contignames, contigs = read_fasta(fasta_path)

    # w_contignames, w_contigs = read_fasta(w_fasta_path)
    #
    # if len(w_contignames) != len(contignames):
    #     print("error")
    #
    # for name in contignames:
    #     if contigs[name] != w_contigs[name]:
    #         print("error")


    # count = 0
    # added_contignames = []
    # total_len = len(contignames)
    # with open(w_fasta_path, 'w') as f_save:
    #     while count < total_len:
    #         remain_names = list(set(contignames).difference(set(added_contignames)))
    #         remain_len = len(remain_names)
    #         index = random.randint(0, remain_len-1)
    #         #print(remain_len, index)
    #         header = '>' + remain_names[index]
    #         f_save.write(header + '\n')
    #         f_save.write(contigs[remain_names[index]] + '\n')
    #
    #         count += 1
    #         added_contignames.append(remain_names[index])


    # 1.parse args
    parser = argparse.ArgumentParser(description='run reference coverage calculation...')
    parser.add_argument('-o', metavar='origin repeat classified path',
                        help='input origin repeat classified path')
    parser.add_argument('-s', metavar='spark repeat classified path',
                        help='input spark repeat classified path')
    args = parser.parse_args()

    origin_classified_path = args.o
    spark_classified_path = args.s


    o_map = {}
    o_contignames, o_contigs = read_fasta(origin_classified_path)
    s_map = {}
    s_contignames, s_contigs = read_fasta(spark_classified_path)
    # r_o_map = {}
    # r_s_map = {}
    # r_o_contigs = {}
    # r_s_contigs = {}
    # for name in o_contigs.keys():
    #     contig = o_contigs[name]
    #     r_contig = getReverseSequence(contig)
    #     contig = contig if contig < r_contig else r_contig
    #     r_o_contigs[contig] = name
    #
    # for name in s_contigs.keys():
    #     contig = s_contigs[name]
    #     r_contig = getReverseSequence(contig)
    #     contig = contig if contig < r_contig else r_contig
    #     r_s_contigs[contig] = name
    #
    # for contig in r_o_contigs.keys():
    #     contig_name = r_o_contigs[contig]
    #     repeat_id, main_class, sub_class = get_classifier_name(contig_name)
    #     r_o_map[contig] = (main_class, sub_class)
    #
    # for contig in r_s_contigs.keys():
    #     contig_name = r_s_contigs[contig]
    #     repeat_id, main_class, sub_class = get_classifier_name(contig_name)
    #     r_s_map[contig] = (main_class, sub_class)
    #
    #
    # total_same_num = 0
    # total_diff_num = 0
    # sub_class_diff_num = 0
    #
    # if len(o_map.keys()) != len(s_map.keys()):
    #     print("origin file length is not equal to spark file")
    # else:
    #     total_diff_id = []
    #     for contig in r_o_map.keys():
    #         (main_class1, sub_class1) = r_o_map[contig]
    #         (main_class2, sub_class2) = r_s_map[contig]
    #         if main_class1 != main_class2:
    #             total_diff_num += 1
    #             total_diff_id.append(contig)
    #         elif sub_class1 != sub_class2:
    #             sub_class_diff_num += 1
    #         else:
    #             total_same_num += 1
    #
    #     print("total_same_num=%d, total_diff_num=%d, sub_class_diff_num=%d, sum=%d" %(total_same_num, total_diff_num, sub_class_diff_num, total_same_num + total_diff_num + sub_class_diff_num))
    #     print("all sequence number=%d" % len(r_o_map.keys()))
    #     print(total_diff_id[0:10])


    for name in o_contignames:
        repeat_id, main_class, sub_class = get_classifier_name(name)
        o_map[repeat_id] = (main_class, sub_class)

    for name in s_contignames:
        repeat_id, main_class, sub_class = get_classifier_name(name)
        s_map[repeat_id] = (main_class, sub_class)


    total_same_num = 0
    total_diff_num = 0
    sub_class_diff_num = 0

    if len(o_map.keys()) != len(s_map.keys()):
        print("origin file length is not equal to spark file")
    else:
        total_diff_id = []
        for repeat_id in o_map.keys():
            (main_class1, sub_class1) = o_map[repeat_id]
            (main_class2, sub_class2) = s_map[repeat_id]
            if main_class1 != main_class2:
                total_diff_num += 1
                total_diff_id.append(repeat_id)
            elif sub_class1 != sub_class2:
                sub_class_diff_num += 1
            else:
                total_same_num += 1

        print("total_same_num=%d, total_diff_num=%d, sub_class_diff_num=%d, sum=%d" %(total_same_num, total_diff_num, sub_class_diff_num, total_same_num + total_diff_num + sub_class_diff_num))
        print("all sequence number=%d" %len(o_map.keys()))
        #print(total_diff_id[0:10])

        # last_repeat_ids = []
        # spark_sub_dir = '/public/home/hpc194701009/Liao_Results/classifier/'
        # sub_ids = []
        # for i in range(0, 345):
        #     p = spark_sub_dir + str(i) + '/RepeatLib_dmel.fa.tmp.classified'
        #     tmp_contignames, tmp_contigs = read_fasta(p)
        #     last_contig_name = tmp_contignames[len(tmp_contignames)-1]
        #     last_repeat_id, main_class, sub_class = get_classifier_name(last_contig_name)
        #     last_repeat_ids.append(last_repeat_id)
        #
        # # total_diff_id = sorted(total_diff_id, key=lambda x: x.split('_')[1], reverse=False)
        # # last_repeat_ids = sorted(last_repeat_ids, key=lambda x: x.split('_')[1], reverse=False)
        # diff_id_num = 0
        # for diff_id in total_diff_id:
        #     if not diff_id in last_repeat_ids:
        #         print("diff id=%s not in last_repeat_ids" %diff_id)
        #         diff_id_num += 1
        # print("number of diff id not in last_repeat_ids = %d" %diff_id_num)

        # print(total_diff_id)
        # print(last_repeat_ids)

        # last_sequence_unknow_num = 0
        # for item in last_repeat_ids:
        #     last_repeat_id = item[0]
        #     main_class = item[1]
        #     sub_class = item[0]
        #     if main_class == 'Unknown':
        #         last_sequence_unknow_num += 1
        #
        #     if not last_repeat_id in total_diff_id:
        #         print("last_repeat_id=%s not in total_diff_ids" %last_repeat_id)
        #
        # print("last_repeat_ids_num=%d, last_sequence_unknow_num=%d" %(len(last_repeat_ids), last_sequence_unknow_num))





