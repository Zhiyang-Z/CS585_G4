activity = LOAD '/home/zhiyang/data/ActivityLog.csv'
    USING PigStorage(',')
    AS (ActionId:int, ByWho:int, WhatPage:int, ActionType:chararray, ActionTime:int);

activity_grp_bywho = GROUP activity BY ByWho;
activity_count = FOREACH activity_grp_bywho
    GENERATE group AS ByWho, COUNT(activity) AS count;
activity_distinct_count = FOREACH activity_grp_bywho {
    distinct_whatpage = DISTINCT activity.WhatPage;
    GENERATE group AS ByWho, COUNT(distinct_whatpage) AS count;
}

activity_info = JOIN activity_count BY ByWho, activity_distinct_count BY ByWho;
activity_info = FOREACH activity_info
    GENERATE activity_count::ByWho AS Id, activity_count::count AS total_actions, activity_distinct_count::count AS distinct_pages;

STORE activity_info INTO '/home/zhiyang/output/project3e_pig'
    USING PigStorage(',');