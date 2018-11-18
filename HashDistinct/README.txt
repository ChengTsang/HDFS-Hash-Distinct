--------------------------------------------------------------------------------
                README
--------------------------------------------------------------------------------

1. start hdfs and hbase

$ start-dfs.sh
$ start-hbase.sh

2. stop hdfs and hbase

$ stop-hbase.sh
$ stop-dfs.sh

3. hdfs directory is ~/work/hdfs

4. To compile your java code MyCode.java (implementing class MyCode)
$ javac MyCode

then to run it
$ java MyCode <args>

5. compile and run HDFSTest.java

$  javac HDFSTest.java
$  java HDFSTest

6. compile and run HBaseTest.java

$  javac HBaseTest.java 
$  java HBaseTest


check if we have successfully create mytable and put the new row
start hbase shell and run command in hbase shell

$ hbase shell

hbase(main):001:0> scan 'mytable'
ROW                                                  COLUMN+CELL                                                                                                                                             
 abc                                                 column=mycf:a, timestamp=1428459927307, value=789                                                                                                       
1 row(s) in 1.8950 seconds

hbase(main):002:0> disable 'mytable'
0 row(s) in 1.9050 seconds

hbase(main):003:0> drop 'mytable'
0 row(s) in 1.2320 seconds

hbase(main):004:0> exit

-------------------------------------------------------------------------------
