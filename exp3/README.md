# 实验三：HBase Java API 编程实验
> 实现 HBase 建表、插入、扫描、修改、删除操作，三张表：student、course、sc

## 实验环境
- Ubuntu 22.04
- Hadoop 3.x
- HBase 2.4.9
- JDK 1.8
- Maven

## 功能说明
1. 创建 student、course、sc 三张表（列族：info）
2. Put 写入数据
3. Scan 扫描读取列族数据
4. Put 覆盖方式实现数据修改（HBase无单独update）
5. Delete 删除整行
6. hbase shell 验证结果

## 运行前置条件
Hadoop、HBase 正常启动，hbase shell 可以正常执行 list

## 运行命令
```bash
mvn clean compile
mvn exec:java -Dexec.mainClass="hbase_exp.HBaseUtil"
