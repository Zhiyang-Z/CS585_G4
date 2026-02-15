circlenet = LOAD '/home/zhiyang/data/CircleNetPage.csv'
    USING PigStorage(',')
    AS (ID:int, NickName:chararray, JobTitle:chararray, RegionCode:int, hobby:chararray);
activity = LOAD '/home/zhiyang/data/ActivityLog.csv'
    USING PigStorage(',')
    AS (ActionId:int, ByWho:int, WhatPage:int, ActionType:chararray, ActionTime:int);

activity_grp_bywho = GROUP activity BY ByWho;
latest_activity = FOREACH activity_grp_bywho GENERATE group AS id, MAX(activity.ActionTime) AS latest_time;
-- Assuming current timestamp is 1,000,000
inactive_users = FILTER latest_activity BY latest_time < (1000000 - 90); -- active in the last 90 days
inactive_users_info = JOIN inactive_users BY id, circlenet BY ID;
inactive_users_info = FOREACH inactive_users_info
    GENERATE inactive_users::id AS Id, circlenet::NickName AS NickName;

STORE inactive_users_info INTO '/home/zhiyang/output/project3g_pig'
    USING PigStorage(',');