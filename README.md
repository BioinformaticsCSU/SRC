# SRC

Installation
------------
**Prerequisites**

  JDK - 
    We recommend using 1.8.0 or higher.
    
  Python - 
    We recommend using python3.

  Perl - 
    Available at http://www.perl.org/get.html. Developed and tested
    with version 5.8.8.

  RepeatMasker & Libraries - 
    Developed and tested with open-4.0.9. The program is available at 
    http://www.repeatmasker.org/RMDownload.html and is distributed with
    Dfam - an open database of transposable element families.

  RMBlast - Precompiled binaries and source can be found at
    http://www.repeatmasker.org/RMBlast.html.
    We recommend using 2.9.0-p2 or higher.
    
  Hadoop & Spark - 
    Configure a Hadoop and Spark platform. Hadoop is available at 
    https://archive.apache.org/dist/hadoop/common/ and Spark can be found at 
    https://spark.apache.org/downloads.html. 
    We use 'hadoop-2.7.6' and 'spark-3.0.1-bin-hadoop2.7' in benchmarkings.
    
**Clone**

    git clone https://github.com/BioinformaticsCSU/SRC.git
   
**Configure**

Automatic:

+ Run the "configure" script interactively with prompts for each setting:	
	         
		cd $SRC_HOME
	
		python configure.py
	 
	 

By Hand:

+ Edit the configuration file "config/ParamConfig.json" and "RepeatClassifier-2.0.1/RepModelConfig.pm"


Getting Started
-----------
**Command**

`python src_entry.py -f <fasta_path> -o <output_dir> `



**Demo**

we provide a demo to run and ensure your installation.

    cd $SRC_HOME

    python src_entry.py -f demo/RepeatLib_ant.fa -o demo/
