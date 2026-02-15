circlenet = LOAD '/home/zhiyang/data/CircleNetPage.csv'
    USING PigStorage(',')
    AS (ID:int, NickName:chararray, JobTitle:chararray, RegionCode:int, hobby:chararray);
follows = LOAD '/home/zhiyang/data/Follows.csv'
    USING PigStorage(',')
    AS (ColRel:int, ID1:int, ID2:int, DateofRelation:int, Desc:chararray);

follows_grp_id1 = GROUP follows BY ID1;
follows_count = FOREACH follows_grp_id1
    GENERATE group AS ID1, COUNT(follows) AS count;
ave_follows = FOREACH (GROUP follows_count ALL) GENERATE AVG(follows_count.count) AS avg_count;
ids_above_ave = FILTER follows_count BY count > ave_follows.avg_count;

follow_info = JOIN ids_above_ave BY ID1, circlenet BY ID;
follow_info = FOREACH follow_info
    GENERATE circlenet::NickName AS NickName, ids_above_ave::count AS count;

STORE follow_info INTO '/home/zhiyang/output/project3f_pig'
    USING PigStorage(',');