# 临时注册，重启后失效
hive> CREATE TEMPORARY FUNCTION UDFAge AS 'UDFAge' USING JAR '/opt/hive/UDF/UDFAge.jar';

# 永久注册
hadoop fs -put /opt/hive/UDF/UDFAge.jar /user/hive/warehouse
hive> CREATE FUNCTION UDFAge AS 'UDFAge' USING JAR 'hdfs:///user/hive/warehouse/UDFAge.jar';