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
	 
	 A practical example of configuration：
	 
	 ![1632903344(1)](https://user-images.githubusercontent.com/22925278/135229940-33b2b92b-9ae5-44cd-8bd2-504564deea02.png)

	 Explanation：
	 * **Please enter Hadoop Home:**&emsp;&emsp;Input your Hadoop Home directory (the upper directory of bin directory). The Hadoop Home directory of the above example is */public/home/hpc194701009/spark/hadoop-2.7.6*
	 * **Please enter Spark Home:**&emsp;&emsp;Input your Spark Home directory (the upper directory of bin directory). The Spark Home directory of the above example is */public/home/hpc194701009/spark/spark-3.0.1-bin-hadoop2.7/*
	 * **Please enter Spark master address(e.g., spark://master_ip:7077):** &emsp;&emsp;You must first know the IP address and port of the master node. The IP address and the port of the above example is *node1014* and *7077*.
	 * **Please enter Spark driver memory size(e.g., 10g):**&emsp;&emsp;This is a more personalized parameter, which can be set appropriately according to your cluster memory space. The driver memory size of the above example is *10g*.
	 * **Please enter Spark total executor cores(e.g., 240):**&emsp;&emsp;This is parameter can be set appropriately according to your cluster cores. Suppose you have five nodes and the number of cores of each node is 48, then this parameter value should be less than or equal to 5 * 48 = 240.
	 * **Please enter Spark executor memory size(e.g., 10g):**&emsp;&emsp;This is also a personalized parameter, which can be set appropriately according to your cluster memory space. The driver memory size of the above example is *10g*.
	 * **Please enter RepeatMasker Home:**&emsp;&emsp;Input your RepeatMasker Home directory (the upper directory of the executable program *RepeatMasker*). The RepeatMasker Home directory of the above example is */public/home/hpc194701009/repeat_detect_tools/RepeatMasker-4.1.2/RepeatMasker*
	 * **Please enter RMBLAST Home:**&emsp;&emsp;Input your RMBLAST Home directory (the upper directory of bin directory). The RMBLAST Home directory of the above example is */public/home/hpc194701009/repeat_detect_tools/rmblast-2.9.0-p2*
	 


Getting Started
-----------
**Command**

`python src_entry.py -f <fasta_path> -o <output_dir> `



**Demo**

we provide a demo to run and ensure your installation.

    cd $SRC_HOME

    python src_entry.py -f demo/RepeatLib_ant.fa -o demo/

