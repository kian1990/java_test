# 临时注册，重启后失效
hive> CREATE TEMPORARY FUNCTION UDFAge AS 'com.example.UDFAge' USING JAR '/root/UDFAge-1.0.jar';

# 永久注册
hadoop fs -put /root/UDFAge-1.0.jar /user/hive/warehouse
hive> CREATE FUNCTION UDFAge AS 'com.example.UDFAge' USING JAR 'hdfs:///user/hive/warehouse/UDFAge-1.0.jar';