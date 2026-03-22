N = 1500000
connected_user_rate = 0.2

import numpy as np
from faker import Faker
from tqdm import tqdm
# fix random seed for reproducibility
np.random.seed(42)

# generate user ids.
print('generating user ids...')
ids = np.arange(0, N).reshape(-1, 1) # (N, 1)
# generate user names.
print('generating user names...')
fake = Faker() # for name generation
names = np.array([fake.name() for _ in tqdm(range(N))]).reshape(-1, 1) # (N, 1)
# generate user ages.
print('generating user ages...')
ages = np.random.randint(18, 65, size=(N, 1)) #
# generate email addresses.
print('generating email addresses...')
emails = np.array([fake.email() for _ in tqdm(range(N))]).reshape(-1, 1) # (N, 1)

def generate_points_numpy(n, max_val=15000):
    total = max_val * max_val
    assert n <= total, "Sample size exceeds total possible points"

    # Sample unique indices
    indices = np.random.choice(total, size=n, replace=False)

    # Convert to (x, y)
    x = indices // max_val + 1
    y = indices % max_val + 1

    # Stack into (n, 2) array
    points = np.column_stack((x, y))

    return points

# generate geo points (x, y) for each user.
print('generating geo points...')
points = generate_points_numpy(N)
connected_ids = np.random.choice(ids.flatten(), size=int(N*connected_user_rate), replace=False)
# combine all the generated data into a single array and write into a csv file.
import csv
print('writing data into csv files...')
with open('people.csv', mode='w', newline='') as file0:
    with open('connected.csv', mode='w', newline='') as file1:
        with open('people_with_handshake_info.csv', mode='w', newline='') as file2:
            writer0, writer1, writer2 = csv.writer(file0), csv.writer(file1), csv.writer(file2)
            for i in tqdm(range(N)):
                writer0.writerow([ids[i][0], names[i][0], ages[i][0], emails[i][0], points[i][0], points[i][1]])
                if ids[i][0] in connected_ids:
                    writer1.writerow([ids[i][0], names[i][0], ages[i][0], emails[i][0], points[i][0], points[i][1]])
                    writer2.writerow([ids[i][0], names[i][0], ages[i][0], emails[i][0], points[i][0], points[i][1], 'yes'])
                else:
                    writer2.writerow([ids[i][0], names[i][0], ages[i][0], emails[i][0], points[i][0], points[i][1], 'no'])
