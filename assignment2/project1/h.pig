circlenet = LOAD '/home/zhiyang/data/CircleNetPage.csv'
    USING PigStorage(',')
    AS (ID:int, NickName:chararray, JobTitle:chararray, RegionCode:int, hobby:chararray);
follows = LOAD '/home/zhiyang/data/Follows.csv'
    USING PigStorage(',')
    AS (ColRel:int, ID1:int, ID2:int, DateofRelation:int, Desc:chararray);

follows_info = FOREACH follows GENERATE ID2 AS follower, ID1 AS followee;
follows_info = JOIN follows_info BY follower, circlenet BY ID;
follows_info = FOREACH follows_info
    GENERATE follows_info::follower AS follower, follows_info::followee AS followee, circlenet::RegionCode AS follower_region;
follows_info = JOIN follows_info BY followee, circlenet BY ID;
follows_info = FOREACH follows_info
    GENERATE follows_info::follower AS follower, follows_info::followee AS followee, follows_info::follower_region AS follower_region, circlenet::RegionCode AS followee_region;

follows_info_same_region = FILTER follows_info BY follower_region == followee_region;
reversed = FOREACH follows_info
    GENERATE followee AS follower,
             follower AS followee;
joined = JOIN follows_info_same_region BY (follower, followee)
         LEFT OUTER,
         reversed BY (follower, followee);
follows_info_no_back = FILTER joined BY reversed::follower IS NULL;

res = JOIN follows_info_no_back BY follows_info_same_region::follower, circlenet BY ID;
res = FOREACH res
    GENERATE follows_info_no_back::follows_info_same_region::follower AS follower, circlenet::NickName AS NickName;
res = DISTINCT res;

STORE res INTO '/home/zhiyang/output/project3h_pig'
    USING PigStorage(',');