import argparse
import shutil
import sys
import os

if __name__ == '__main__':
    # 1.parse args
    parser = argparse.ArgumentParser(description='run Spark contiguous repeat kmer sequence find...')
    parser.add_argument('-o', metavar='output dir',
                        help='input output dir')
    parser.add_argument('-f', metavar='filename',
                        help='input filename')
    parser.add_argument('-p', metavar='repeatclassifier path',
                        help='input repeatclassifier path')

    args = parser.parse_args()
    output_dir = args.o
    filename = args.f
    program_path = args.p

    removed_dirs = []

    #part01: save sub files
    last_partition_index = '-1'

    contigNames = []
    contigs = {}
    try:
        for line in sys.stdin:
            parts = line.split(">")
            partition_index = parts[0]

            if last_partition_index != '-1' and last_partition_index != partition_index:
                print('Pipe parse partition index error, One partition has multiple partition index')
                break

            seq_parts = parts[1].split("#")
            contigName = seq_parts[0]
            contig = seq_parts[1]
            contigNames.append(contigName)
            contigs[contigName] = contig

            last_partition_index = partition_index
    except:
        pass

    if last_partition_index != '-1':
        tmp_dir = output_dir + '/' + last_partition_index
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        removed_dirs.append(tmp_dir)
        unclassified_file_path = tmp_dir + '/' + filename

        with open(unclassified_file_path, "w") as f_save:
            for contigName in contigNames:
                contig = contigs[contigName]
                f_save.write(">" + contigName + "\n")
                f_save.write(contig + "\n")
            #write last sequence twice
            last_seq_name = contigNames[len(contigNames)-1]
            last_seq = contigs[last_seq_name]
            f_save.write(">" + last_seq_name + "\n")
            f_save.write(last_seq + "\n")
        f_save.close()


        #part02: invoke RepeatClassifier

        classifier_command = program_path + ' -consensi '
        (dir, filename) = os.path.split(unclassified_file_path)
        command = 'cd ' + dir + ' && ' + classifier_command + unclassified_file_path
        os.system(command)

        classified_path = unclassified_file_path + '.classified'
        #classified_path = unclassified_file_path
        if not os.path.exists(classified_path):
            print("%s is not exist, please check" % classified_path)

        # part03: write back to pipe
        # 1. read classified_file
        # important! since repeatclassifier has bug(discard last sequence),
        # we have to duplicate last sequence, and then discard last sequence
        contigs = {}
        contigNames = []
        with open(classified_path, "r") as f_r:
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
            # discard last sequence
            # contigs[contigName] = contigseq
            # contigNames.append(contigName)
        f_r.close()

        #write back to pipe
        for contigName in contigNames:
            contig = contigs[contigName]
            print(">" + contigName + "$" + contig + "$")

    for tmp_dir in removed_dirs:
        shutil.rmtree(tmp_dir, ignore_errors=True)