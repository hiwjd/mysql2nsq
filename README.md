# mysql2nsq

> produce mysql binlog to nsq

mysql -> binlog -> mysql2nsq -> nsq -> build external index|maintain cache|statistics

## Getting started

1. Create slave acount for mysql2nsq
2. Prepare table definition file, will describe below
3. Start mysql2nsq `(use ./mysql2nsq -h to see the whole option list)`
4. Subscribe from nsq

> note that mysql2nsq use schema name as nsq topic

The table definition file is in json format, it help mysql2nsq to know the column name. Here is an example,

assume we have a table `user` in database `db1`:

```sql
CREATE TABLE `user` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(30) NOT NULL DEFAULT '',
  `score` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

the definition file:

```json
{
  "db1-user": {
    "Cols": [
      {"Name":"id"},
      {"Name":"name"},
      {"Name":"score"},
    ]
  }
}
```

`db1-user` is schema name combine with table name. `Cols` represents table column, and becareful it should have the same order with table.

Let`s perform some sql action to see what will we subscribed.

```json
INSERT INTO `user`(name, score) VALUES('hiwjd', 80);

UPDATE `user` SET score = 85 where name = 'hiwjd';
```

We get data below:

```json
{"Schema":"db1","Table":"user","Action":"INSERT","Rows":[{"id":1,"name":"hiwjd","score":80}]}

{"Schema":"db1","Table":"user","Action":"UPDATE","Rows":[{"id":1,"name":"hiwjd","score":80},{"id":1,"name":"hiwjd","score":85}]}
```
