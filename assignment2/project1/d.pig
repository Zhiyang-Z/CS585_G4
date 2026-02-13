circlenet = LOAD '/home/zhiyang/data/CircleNetPage.csv'
    USING PigStorage(',')
    AS (ID:int, NickName:chararray, JobTitle:chararray, RegionCode:int, hobby:chararray);
follows = LOAD '/home/zhiyang/data/Follows.csv'
    USING PigStorage(',')
    AS (ColRel:int, ID1:int, ID2:int, DateofRelation:int, Desc:chararray);

follows_grp_id1 = GROUP follows BY ID1;
follows_count = FOREACH follows_grp_id1
    GENERATE group AS ID1, COUNT(follows) AS count;
follow_info = JOIN follows_count BY ID1, circlenet BY ID;
follow_info = FOREACH follow_info
    GENERATE follows_count::ID1 AS ID, circlenet::NickName AS NickName, follows_count::count AS count;

STORE follow_info INTO '/home/zhiyang/output/project3d_pig'
    USING PigStorage(',');