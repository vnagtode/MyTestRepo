set mapred.job.queue.name = etl;
set hive.merge.mapfiles = true
set hive.merge.smallfiles.avgsize = 200000000;
--set hive.exec.compress.output = true;
use reco;
INSERT OVERWRITE DIRECTORY '/user/hadoop/test/validate_reco/validationset_reguevents/logdate=${hiveconf:LOGDATE}'
select Visitor,accid,lid,logdate,
sum(case when ptnid="188" then 1 else 0 end) as SaveListing,
sum(case when ptnid="190" then 1 else 0 end) as SendToFriend,
sum(case when ptnid="70" and upper(page)="LDP" then 1 else 0 end) as LDP,
sum(case when ptnid in ("227","229","231","232","204","230","398","496","533","536")  then 1 else 0 end) as PHotoClicks
from Reco_ValidationRecords_Analytics
where ((ptnid ="70" and upper(page)="LDP") or (ptnid in ("188","190","227","229","231","232","204","230","398","496","533","536")))
and logdate = ${hiveconf:LOGDATE}
group by Visitor,accid,lid,logdate;
