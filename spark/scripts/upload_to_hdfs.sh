#!/bin/bash

# Upload the retail CSV to HDFS
docker exec retail_namenode hdfs dfs \
 -mkdir -p /data/raw \
 -put -f /data/Online_Retail.csv /data/raw
