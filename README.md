# redis-aof-parser

使用**重写**后的aof文件对redis中的数据进行内存分析。

#步骤

- 解析aof文件，将key的信息保存到sqlite数据库

- 计算key的内存信息，主要参考[rdb-tools](https://github.com/sripathikrishnan/redis-rdb-tools)
