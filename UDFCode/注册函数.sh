# 临时注册，重启后失效
hive> CREATE TEMPORARY FUNCTION UDFCode AS 'com.example.UDFCode' USING JAR '/root/UDFCode-1.0.jar';

# 永久注册
hadoop fs -put /root/UDFCode-1.0.jar /user/hive/warehouse
hive> CREATE FUNCTION UDFCode AS 'com.example.UDFCode' USING JAR 'hdfs:///user/hive/warehouse/UDFCode-1.0.jar';