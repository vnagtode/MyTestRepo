set mapred.job.queue.name = etl;
set hive.merge.mapfiles = true
set hive.merge.smallfiles.avgsize = 200000000;
set hive.exec.compress.output = true;
use bi;
INSERT OVERWRITE DIRECTORY '/user/hadoop/test/validate_reco/reguevents/logdate=${hiveconf:LOGDATE}'
--ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
select * from instrumentation where uri_query['accid'][0] is not null and uri_query['lid'][0] is not null 
and logdate = 
--20140109
${hiveconf:LOGDATE} 
;
