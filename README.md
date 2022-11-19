# flink-connector-kudu
```sql
CREATE TABLE t_dim_user_auth_kd(
	username string,
	download_filepath string,
	download_filename string,
	download_db string,
	
	PRIMARY KEY (username,download_filepath)  not enforced
)WITH(

	  'connector' = 'kudu'
   ,'masters' = 'localhost:7051' --kudu地址和端口
   ,'table' = 'impala::ods_db_activity.t_ods_user_auth_kd'--kudu表名
 );
 CREATE TABLE t_ods_ngx_log_dirty_kd(
    context_id string,
    id string,
	PROCTIME  AS PROCTIME(),
    PRIMARY KEY (context_id,id) not enforced
)WITH(

  'connector' = 'kudu'
  ,'masters' = 'localhost:7051' --kudu地址和端口
  ,'table' = 'impala::ods_db_activity.t_ods_ngx_log_dirty_kd'--kudu表名
  ,'sink.parallelism' ='4'
  ,'sink.buffer-flush.interval' ='10s'
  ,'sink.buffer-flush.max-rows' ='100'
);
 select * from t_ods_ngx_log_dirty_kd a
 LEFT JOIN t_dim_user_auth_kd FOR SYSTEM_TIME AS OF a.PROCTIME AS b 
 ON a.context_id = b.username
 ;
 ```
