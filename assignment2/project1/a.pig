circlenet = LOAD '/home/zhiyang/data/CircleNetPage.csv'
    USING PigStorage(',')
    AS (ID:int, NickName:chararray, JobTitle:chararray, RegionCode:int, hobby:chararray);

circlenet_grp_hobby = GROUP circlenet BY hobby;

hobby_count = FOREACH circlenet_grp_hobby
    GENERATE group AS hobby, COUNT(circlenet) AS count;

STORE hobby_count INTO '/home/zhiyang/output/project3a_pig'
    USING PigStorage(',');
