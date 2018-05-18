# Hadoop-Essentials

## HDFS Commands
ls - listing
mkdir - make directory
cp -copy
mv - move
rm - remove or delete


### copy  
to loacal
hadoop fs -copyToLocal path/filname.csv
to HDFS  
hadoop fs -copyfromLocal path/filname.csv
  
### Replication factor
hadoop fs -ls filename
hadoop fs -Ddfs.replication=2 -cp filname


### permissions  
hadoop fs -chmod 777 filepath
### file system check
sudo -u hdfs hdfs fsck /filepath -files -block -block -locations    
  
