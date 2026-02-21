
circlenet = LOAD '/home/zhiyang/data/CircleNetPage.csv'
    USING PigStorage(',')
    AS (ID:int, NickName:chararray, JobTitle:chararray, RegionCode:int, hobby:chararray);
follows = LOAD '/home/zhiyang/data/Follows.csv'
    USING PigStorage(',')
    AS (ColRel:int, ID1:int, ID2:int, DateofRelation:int, Desc:chararray);

follows_grp_id2 = GROUP follows BY ID2;
follows_count = FOREACH follows_grp_id2
    GENERATE group AS ID2, COUNT(follows) AS count;
follow_info = JOIN follows_count BY ID2, circlenet BY ID;
follow_info = FOREACH follow_info
    GENERATE follows_count::ID2 AS ID, circlenet::NickName AS NickName, follows_count::count AS count;

STORE follow_info INTO '/home/zhiyang/output/project3d_pig'
    USING PigStorage(',');
