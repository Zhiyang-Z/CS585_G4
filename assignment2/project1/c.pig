circlenet = LOAD '/home/zhiyang/data/CircleNetPage.csv'
    USING PigStorage(',')
    AS (ID:int, NickName:chararray, JobTitle:chararray, RegionCode:int, hobby:chararray);

-- select users whose hobby is photography
circlenet_photography = FILTER circlenet BY hobby == 'Photography';
circlenet_photography_info = FOREACH circlenet_photography
    GENERATE NickName, JobTitle;

STORE circlenet_photography_info INTO '/home/zhiyang/output/project3c_pig'
    USING PigStorage(',');
