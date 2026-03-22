from pyspark import SparkConf, SparkContext
distance_unit = 6

# ----------------------------------------
# 1. Create Spark Context and read data
# ----------------------------------------
conf = SparkConf().setAppName("AverageAgeRDD").setMaster("local[*]")
sc = SparkContext(conf=conf)

# people_RDD = sc.textFile("people.csv")
# connected_RDD = sc.textFile("connected.csv")
people_with_handshake_info_RDD = sc.textFile("people_with_handshake_info.csv")
# according to people_with_handshake_info_RDD, construct people_RDD and connected_RDD
people_RDD = people_with_handshake_info_RDD.map(lambda line: line.split(",")).map(lambda x: ",".join(x[0:6]))
connected_RDD = people_with_handshake_info_RDD.map(lambda line: line.split(",")).filter(lambda x: x[6] == "yes").map(lambda x: ",".join(x[0:6]))

### query 1 ###
# 1. connected RDD: map to ((x//distance_unit, y//distance_unit), (id, x, y)) and expand to neighboring grid cells
connected_RDD4join = (
    connected_RDD
    .map(lambda line: line.split(","))
    .map(lambda x: ((int(x[4]) // distance_unit, int(x[5]) // distance_unit), (x[0], (int(x[4]), int(x[5])))))  # ((x//distance_unit, y//distance_unit), (id, x, y))
    .flatMap(lambda x: [((x[0][0] + dx, x[0][1] + dy), x[1]) for dx in range(-1, 1) for dy in range(-1, 1)])  # expand to neighboring grid cells
)
# 2. people RDD: map to ((x//distance_unit, y//distance_unit), (id, x, y))
distance_RDD = (
    people_RDD
    .map(lambda line: line.split(","))
    .map(lambda x: (x[0], (int(x[4]), int(x[5]))))  # (id, (x, y))
    .map(lambda x: ((x[1][0] // distance_unit, x[1][1] // distance_unit), (x[0], (x[1][0], x[1][1]))))  # ((x//distance_unit, y//distance_unit), (id, x, y))
)
# 3. Join RDDs on grid cell key
joined_RDD = distance_RDD.join(connected_RDD4join)  # ((x//distance_unit, y//distance_unit), ((id1, (x1, y1)), (id2, (x2, y2))))
# 4. Calculate distance and filter pairs
distance_RDD = (
    joined_RDD
    .map(lambda x: ((x[1][0][0], x[1][1][0]), ((x[1][0][1][0] - x[1][1][1][0]) ** 2 + (x[1][0][1][1] - x[1][1][1][1]) ** 2) ** 0.5))  # ((id1, id2), distance)
    .filter(lambda x: x[1] < 6 and x[1] > 0)  # filter pairs with distance less than 6
    .map(lambda x: (x[0][1], x[0][0])) # map to (id2, id1)
)
# 5. count the total number of persons for each connect-i
count_RDD = distance_RDD.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

count_RDD_data = count_RDD.collect()
for record in count_RDD_data[0:5]:
    print(record)
print(f"Total persons with distance less than 6: {len(count_RDD_data)}")

# ----------------------------------------
# 5. Stop Spark Context
# ----------------------------------------
sc.stop()