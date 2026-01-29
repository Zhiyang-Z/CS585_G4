import csv
from faker import Faker
import random
from tqdm import tqdm
import numpy as np
random.seed(7)
np.random.seed(7)

############################################ 1. generate basic info ############################################
jobs = [
    "Data Analyst", "Cloud Engineer", "AI Researcher", "ML Engineer", "Backend Dev", "Frontend Dev", "Fullstack Dev", "DevOps Engr",
    "System Admin", "Network Engr", "Security Engr", "QA Engineer", "Test Engineer", "Product Owner", "Project Lead", "Tech Manager",
    "Data Engineer", "Model Trainer", "AI Specialist", "ML Specialist", "Game Designer", "Level Designer", "UX Designer", "UI Designer",
    "Graphic Artist", "Visual Artist", "Media Planner", "Brand Manager", "Content Editor", "SEO Specialist", "Market Analyst", "Sales Manager",
]
job_probs = [
    0.0207, 0.0413, 0.0189, 0.0286, 0.0375, 0.0241, 0.0338, 0.0302, 0.0219, 0.0394, 0.0267, 0.0349, 0.0273, 0.0321, 0.0408, 0.0226,
    0.0294, 0.0317, 0.0235, 0.0382, 0.0259, 0.0366, 0.0198, 0.0354, 0.0279, 0.0842, 0.0201, 0.0330, 0.0288, 0.0249, 0.0416, 0.0182
]
assert sum(job_probs) == 1
region_codes = [code for code in range(1, 51)]
region_probs = [
    0.0204, 0.0127, 0.0189, 0.0068, 0.0295, 0.0216, 0.0241, 0.0173, 0.0109, 0.0278, 0.0197, 0.0225, 0.0146, 0.0259, 0.0083, 0.0164,
    0.0232, 0.0118, 0.0286, 0.0139, 0.0209, 0.0075, 0.0267, 0.0157, 0.0096, 0.0248, 0.0181, 0.0122, 0.0291, 0.0103, 0.0213, 0.0179,
    0.0061, 0.0272, 0.0192, 0.0141, 0.0229, 0.0089, 0.0254, 0.0111, 0.0283, 0.0169, 0.0079, 0.0238, 0.0133, 0.0201, 0.1058, 0.0261,
    0.0152, 0.0187
]
assert sum(region_probs) == 1
hobbies = [
    "Photography", "Landscape Art", "Street Sketch", "Travel Vlogging", "Mountain Hiking", "Trail Running", "Rock Climbing", "Bird Watching",
    "Stargazing", "Night Sky Photos", "Drone Flying", "Film Making", "Short Film Edit", "Music Mixing", "Guitar Practice", "Piano Playing",
    "Song Writing", "Digital Painting", "Watercolor Art", "Oil Painting", "3D Modeling", "Game Designing", "Level Creation", "Indie Game Dev",
    "Coding Projects", "Robotics Build", "Arduino Tinkering", "DIY Crafts", "Wood Carving", "Furniture Making", "Home Gardening", "Plant Care",
]
hobby_probs = [
    0.0289, 0.0512, 0.0197, 0.0335, 0.0224, 0.0371, 0.0256, 0.0408, 0.0179, 0.0394, 0.0216, 0.0347, 0.0263, 0.0291, 0.0428, 0.0185,
    0.0314, 0.0279, 0.0208, 0.0362, 0.0241, 0.0486, 0.0169, 0.0454, 0.0233, 0.0661, 0.0192, 0.0326, 0.0281, 0.0329, 0.0419, 0.0151
]
assert sum(hobby_probs) == 1
data = [
    ["ID", "NickName", "JobTitle", "RegionCode", "FavoriteHobby"],
]
fake = Faker() # for name generation
print('Generating CircleNetPage.csv...')
for id in tqdm(range(1, 200001)):
    user = [id, fake.user_name(),\
            random.choices(jobs, weights=job_probs, k=1)[0], random.choices(region_codes, weights=region_probs, k=1)[0], random.choices(hobbies, weights=hobby_probs, k=1)[0]]
    data.append(user)
with open("CircleNetPage.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(data)
############################################ 2. generate Follows ############################################
print('Generating Follows.csv...')
dates = [date for date in range(1, 1000001)]
date_prob_before_norm = [p for p in range(10, 41)]
date_prob = np.array([random.choice(date_prob_before_norm) for _ in range(1000000)])
date_prob = np.array(date_prob) / date_prob.sum()
follow_desc_list = [
    "college friend","high school friend","family member","relative abroad","coworker","former coworker","project teammate",
    "research partner","online friend","gaming buddy","travel companion","fitness partner","study group mate","club member",
    "same hometown","celebrity interest","sports fan page","music artist fan","content creator","business contact"
]
follow_desc_prob = [
    0.0452, 0.0389, 0.0540, 0.0617, 0.0473, 0.0508, 0.0435, 0.0489, 0.0532, 0.0496,
    0.0461, 0.0614, 0.0427, 0.0643, 0.0479, 0.0481, 0.0492, 0.0504, 0.0438, 0.0530
]
assert sum(follow_desc_prob) == 1
users = np.array([id for id in range(1, 200001)])
follow_prob_before_norm = [p for p in range(10, 71)]
follow_prob = [random.choice(follow_prob_before_norm) for _ in range(200000)]
follow_prob = np.array(follow_prob) / sum(follow_prob)
follow_by_prob_before_norm = [p for p in range(10, 101)]
follow_by_prob = [random.choice(follow_by_prob_before_norm) for _ in range(200000)]
follow_by_prob = np.array(follow_by_prob) / sum(follow_by_prob)

colrels = [num for num in range(1, 20000001)]
id1s = np.random.choice(users, size=20000000, p=follow_prob)
id2s = np.random.choice(users, size=20000000, p=follow_by_prob)
mask = (id1s == id2s)
while mask.sum() > 0:
    id2s[mask] = np.random.choice(users, size=mask.sum(), p=follow_by_prob)
    mask = (id1s == id2s)
date_of_relations = np.random.choice(dates, size=20000000, p=date_prob).tolist()
follow_descs = random.choices(follow_desc_list, weights=follow_desc_prob, k=20000000)

data = []
for colrel, id1, id2, date_of_relation, follow_desc in tqdm(zip(colrels, id1s, id2s, date_of_relations, follow_descs)):
    data.append([colrel, id1, id2, date_of_relation, follow_desc])
assert len(data) == 20000000
data.insert(0, ["ColRel", "ID1", "ID2", "DateofRelation", "Desc"])
with open("Follows.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(data)
############################################ 3. generate Activity Log ############################################
print('Generating ActivityLog.csv...')
dates = [date for date in range(1, 1000001)]
date_prob_before_norm = [p for p in range(10, 41)]
date_prob = np.array([random.choice(date_prob_before_norm) for _ in range(1000000)])
date_prob = np.array(date_prob) / date_prob.sum()

users = np.array([id for id in range(1, 200001)])
active_prob_before_norm = [p for p in range(10, 101)]
active_prob = [random.choice(active_prob_before_norm) for _ in range(200000)]
active_prob = np.array(active_prob) / sum(active_prob)
view_by_prob_before_norm = [p for p in range(10, 101)]
viewed_prob = [random.choice(view_by_prob_before_norm) for _ in range(200000)]
viewed_prob = np.array(viewed_prob) / sum(viewed_prob)

activity = ['viewed', 'left a note', 'poked']
activity_prob = [0.7, 0.1, 0.2]
data = [
    ['ActionId', 'ByWho', 'WhatPage', 'ActionType', 'ActionTime'],
]
generated_record = 0
# view only
N = int(10000000*0.7)
view_only_bywho = np.random.choice(users, size=N, p=active_prob)
view_only_whatpage = np.random.choice(users, size=N, p=viewed_prob)
mask = (view_only_bywho == view_only_whatpage)
while mask.sum() > 0:
    view_only_whatpage[mask] = np.random.choice(users, size=mask.sum(), p=viewed_prob)
    mask = (view_only_bywho == view_only_whatpage)
action_time = np.random.choice(dates, size=N, p=date_prob).tolist()
for bywho, whatpage, time in tqdm(zip(view_only_bywho, view_only_whatpage, action_time)):
    data.append([generated_record+1, bywho, whatpage, 'viewed', time])
    generated_record += 1
# left a note only
N = int(10000000*0.1) // 2
left_note_only_bywho = np.random.choice(users, size=N, p=active_prob)
left_note_only_whatpage = np.random.choice(users, size=N, p=viewed_prob)
mask = (left_note_only_bywho == left_note_only_whatpage)
while mask.sum() > 0:
    left_note_only_whatpage[mask] = np.random.choice(users, size=mask.sum(), p=viewed_prob)
    mask = (left_note_only_bywho == left_note_only_whatpage)
action_time = np.random.choice(dates, size=N, p=date_prob).tolist()
for bywho, whatpage, time in tqdm(zip(left_note_only_bywho, left_note_only_whatpage, action_time)):
    data.append([generated_record+1, bywho, whatpage, 'viewed', time])
    data.append([generated_record+1, bywho, whatpage, 'left a note', time])
    generated_record += 2
# poked only
N = int(10000000*0.2) // 2
poked_only_bywho = np.random.choice(users, size=N, p=active_prob)
poked_only_whatpage = np.random.choice(users, size=N, p=viewed_prob)
mask = (poked_only_bywho == poked_only_whatpage)
while mask.sum() > 0:
    poked_only_whatpage[mask] = np.random.choice(users, size=mask.sum(), p=viewed_prob)
    mask = (poked_only_bywho == poked_only_whatpage)
action_time = np.random.choice(dates, size=N, p=date_prob).tolist()
for bywho, whatpage, time in tqdm(zip(poked_only_bywho, poked_only_whatpage, action_time)):
    data.append([generated_record+1, bywho, whatpage, 'viewed', time])
    data.append([generated_record+1, bywho, whatpage, 'poked', time])
    generated_record += 2
assert len(data) == 10000001
with open("ActivityLog.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(data)
