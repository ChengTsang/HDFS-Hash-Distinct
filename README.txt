### realize hash distinct based on HDFS
![](https://img.shields.io/badge/language-C++-red.svg) 
![](https://img.shields.io/badge/license-MIT-000000.svg)

The project is to distinct based on hash. I implemented it on HBase and HDFS systems.
It may help you get familiar with HBase and HDFS.

In the first, you need to read data from HDFS.The format of the file is text,each row
of which is a relational record, each colume is seperated from each coliumn.

for example:
1|AMERICA|hs use ironic, even requests. s|
zeroth column: 1
first column: AMERICA
Second columns: HS use ironic, even requests. S

Some basic command below is helpful:
$ hdfs dfs –help
-copyFromLocal [-f] [-p] [-l] <localsrc> ... <dst> :
Identical to the -put command.
-copyToLocal [-p] [-ignoreCrc] [-crc] <src> ... <localdst> :
Identical to the -get command.
-cat [-ignoreCrc] <src> ... :
Fetch all files that match the file pattern <src> and display
their content on stdout.
-ls [-d] [-h] [-R] [<path> ...] :
list contents

You could also read HDFSTest.java to know how to read data from HDFS. The HBaseTest.java
may help you know how to use HBase.

The main idea for hash distinct is below:
1. Build a hash table
2. Key= distinct all key (for example：R2,R3,R5)
3. Value= None
4. Put the input in hash table once and only once, and finally scan out all the items
   in hash table.

You can use command like this to use my code:
java Hw1Grp4 R=/hw1/part.tbl select:R7,gt,1800 distinct:R3,R4,R5


