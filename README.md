One of classic map and reduce case we might need to use during our work
1. Input:
h,masterId,branchId,branchName
d,1,1,Chengdu
d,1,2,Chongqing
d,2,1,Shanghai
d,2,2,Beijing
d,2,3,Guangzhou
d,2,4,Shenzheng
d,3,1,Other
2. Ouput:
h,masterId,branchId,branchName
d,1,1||2,Chengdu||Chongqing
d,1,2,Chongqing
d,2,1||2||3||4,Shanghai||Beijing||Guangzhou||Shenzheng
d,3,1,Other

## Submit spark job
--driver-memory indicate the driver programe's memory, default 1g, in case of there are large dataset doing the aggregation (reduce, reduce by key), we need to give more memory for it. 
./bin/spark-submit --master local[2] --total-executor-cores 20 --driver-memory 2g --class example.Case1 /Users/aihua/work/spark-mapreduce/target/scala-2.11/spark-mapreduce_2.11-0.1.0-SNAPSHOT.jar /Users/aihua/work/spark-mapreduce/src/test/resources/spark_case1.csv
