from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Step2_SparkSQL").config("spark.driver.memory", "8g").getOrCreate()
# Task 2.1
books_df = spark.read.csv("./data/books_data.csv", header=True, inferSchema=True, multiLine=True)
ratings_df = spark.read.csv("./data/Books_rating.csv", header=True, inferSchema=True, multiLine=True)
print(books_df.printSchema())
print(ratings_df.printSchema())
books_df.createOrReplaceTempView("books")
ratings_df.createOrReplaceTempView("ratings")

# Task 2.2
spark.sql("drop table if exists T1")
spark.sql("""
          create table T1 as 
          select * from ratings 
          where try_cast(split(`review/score`, '/')[0] as double) >= 4.0
            and `review/text` is not null
            and Title is not null;
          """)
spark.sql("select * from ratings limit 4").show()

# Task 2.3
spark.sql("""
SELECT 
    `review/score` AS score,
    COUNT(*) AS num_reviews,
    AVG(LENGTH(`review/text`)) AS avg_text_len,
    MIN(LENGTH(`review/text`)) AS min_text_len,
    MAX(LENGTH(`review/text`)) AS max_text_len
FROM T1
GROUP BY `review/score`
ORDER BY score
""").show(truncate=False)

# Task 2.4
spark.sql("""
CREATE TABLE T3 AS
SELECT
    User_id AS user_id,
    COUNT(*) AS num_reviews,
    AVG(try_cast(split(`review/score`, '/')[0] as double)) AS avg_score,
    AVG(LENGTH(`review/text`)) AS avg_text_len
FROM T1
GROUP BY User_id
HAVING COUNT(*) >= 3
""")
spark.sql("select * from T3 limit 4").show()

# Task 2.5
spark.sql("""
CREATE OR REPLACE TEMP VIEW T4 AS
SELECT
    t1.User_id AS user_id,
    bd.categories AS category,
    COUNT(*) AS num_reviews_in_category,
    AVG(try_cast(split(t1.`review/score`, '/')[0] AS DOUBLE)) AS avg_score_in_category,
    AVG(try_cast(t1.price as double)) AS avg_price_in_category
FROM T1 t1 JOIN books bd ON t1.title = bd.title
GROUP BY t1.User_id, bd.categories
HAVING COUNT(*) >= 2
""")
spark.sql("select * from T4 limit 4").show()