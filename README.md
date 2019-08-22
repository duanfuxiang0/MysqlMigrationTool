# mysql 数据迁移工具

write in golang

## 场景
当线上mysql需要迁移大量数据的时候，在不影响线上数据访问的情况下，将数据迁移到目标数据表，
不太适合要求数据强一致的分布式环境，事务很难保证；比较适合在原来的数据库内进行分表或者加密
相关字段的需求。
<p> 工具分成两个脚本，全量迁移脚本和增量迁移脚本，再配合业务代码的双写，可以实现线上数据
的无感知迁移，具体的迁移流程如下图：
![](https://github.com/fuxiangduan/MysqlMigrationTool/blob/master/%E5%8F%8C%E5%86%99%E6%96%B9%E6%A1%88.jpg?raw=true)

## 1. 全量迁移script, full_data_migration.go

全量迁移，采用查线上从库数据，然后插入到目标数据表，查从库是为了不影响线上数据查询。
<p> 查询（分页查询）不能使用类似

```mysql
SELECT * from table1 LIMIT count OFFSET offset;
```

的sql语句，查询到千万的时候执行一个 sql 要30s了，越到后面，sql执行速度越慢, 应该使用
结合索引与limit的方式，

```mysql
SELECT * FROM table1 where index_key > start_key order by index_key asc LIMIT count
```

全表扫的时候，可以每次记住上一次查询的 last_index_key, 当作本次查询的start_key
<p>批量插入的也不适合直接使用insert, 可以使用类似 insert …on duplicate key update sql的
语句，可以做到在不存在主键或唯一键的情况下，执行insert 操作，否则执行 update 操作，支持多次重试。
<p>注：在 mysql 可重复读隔离级别的情况下，表结构中有主键和唯一键的情况下，并发执行insert …on duplicate key update 存在死锁问题，可以设置 session 为 读提交隔离级别，初始化 sql：

```mysql
SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
```

## 2. 增量迁移script, incremental_data-migration.go

增量迁移的数据源是线上主库的binlog
用go-mysql来处理 binlog，使用binlog中的row格式
解析insert, update, delete 事件, 更新数据.

