# 临时注册，重启后失效
hive> CREATE TEMPORARY FUNCTION UDFArea AS 'com.example.UDFArea' USING JAR '/root/UDFArea-1.0.jar';

# 永久注册
hadoop fs -put /root/UDFArea-1.0.jar /user/hive/warehouse
hive> CREATE FUNCTION UDFArea AS 'com.example.UDFArea' USING JAR 'hdfs:///user/hive/warehouse/UDFArea-1.0.jar';