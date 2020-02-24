# mysql2nsq

> produce mysql binlog to nsq

mysql -> binlog -> mysql2nsq -> nsq -> build external index|maintain cache|statistics

## Getting started

1. Create slave acount for mysql2nsq
2. Start mysql2nsq `(use ./mysql2nsq -h to see the whole option list)`
3. Subscribe from nsq

> note that mysql2nsq use schema name as nsq topic
