# SRC

Installation
------------
**Prerequisites**

  Perl
    Available at http://www.perl.org/get.html. Developed and tested
    with version 5.8.8.

  RepeatMasker & Libraries
    Developed and tested with open-4.0.9. The program is available at 
    http://www.repeatmasker.org/RMDownload.html and is distributed with
    Dfam - an open database of transposable element families.

  RMBlast - Precompiled binaries and source can be found at
    http://www.repeatmasker.org/RMBlast.html
    We recommend using 2.9.0-p2 or higher.
    
**Installation**

   git clone https://github.com/BioinformaticsCSU/SRC.git

Getting Started
-----------
`python src_entry.py -f <fasta_path (absolute path)> -o <output_dir (absolute path)> `

ensure you use absolute path for fasta path and output directory.


we provide a demo to run and ensure your installation.

`python src_entry.py -f /home/.../SRC/demo/RepeatLib_ant.fa -o /home/.../SRC/demo/`
