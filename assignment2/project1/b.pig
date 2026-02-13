circlenet = LOAD '/home/zhiyang/data/CircleNetPage.csv'
    USING PigStorage(',')
    AS (ID:int, NickName:chararray, JobTitle:chararray, RegionCode:int, hobby:chararray);
activity = LOAD '/home/zhiyang/data/ActivityLog.csv'
    USING PigStorage(',')
    AS (ActionId:int, ByWho:int, WhatPage:int, ActionType:chararray, ActionTime:int);

-- find top 10 users.
activity_grp_whatpage = GROUP activity BY WhatPage;
activity_count = FOREACH activity_grp_whatpage
    GENERATE group AS WhatPage, COUNT(activity) AS count;
activity_order = ORDER activity_count BY count DESC;
activity_top10 = LIMIT activity_order 10;
-- find out top10 user's id, nickname, and jobtitle
top_users = JOIN activity_top10 BY WhatPage, circlenet BY ID;
top_users_info = FOREACH top_users
    GENERATE activity_top10::WhatPage AS WhatPage, circlenet::NickName AS NickName, circlenet::JobTitle AS JobTitle;

STORE top_users_info INTO '/home/zhiyang/output/project3b_pig'
    USING PigStorage(',');
