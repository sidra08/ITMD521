Saved commands 

hadoop fs -ls -h  /user/controller/ncdc-orig




# to run jobs on remote cluster 
spark-submit --verbose --name siq-read-write-parquet-2011.py --master yarn --deploy-mode cluster siq-read-write-parquet-2011.py