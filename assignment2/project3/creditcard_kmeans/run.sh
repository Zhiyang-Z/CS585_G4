#!/bin/bash
# =============================================================
# CS585 Project 2 - Part 3d: Credit Card K-Means + Visualization
# =============================================================

JAR="target/creditcard-kmeans-1.0-jar-with-dependencies.jar"
HDFS_BASE="/user/$USER/project2/creditcard"

echo "[Step 1] Building JAR and downloading dependencies..."
mvn clean package -q
mvn dependency:copy-dependencies -DoutputDirectory=libs -q
echo "JFreeChart JAR location: $(pwd)/libs/jfreechart-1.5.4.jar"
echo "Use this path in Zeppelin Cell 0 z.load() call"

echo "[Step 2] Uploading data to HDFS..."
hdfs dfs -mkdir -p $HDFS_BASE/input
hdfs dfs -put -f creditcard_data.csv    $HDFS_BASE/input/creditcard_data.csv
hdfs dfs -put -f creditcard_seeds_k3.csv $HDFS_BASE/seeds_k3.csv

echo "[Step 3] Running K-Means (K=3, R=25, threshold=0.001)..."
hdfs dfs -rm -r -f $HDFS_BASE/output

hadoop jar $JAR cs585.creditcard.CreditCardKMeans \
    $HDFS_BASE/input/creditcard_data.csv \
    $HDFS_BASE/seeds_k3.csv \
    $HDFS_BASE/output \
    25 \
    0.001

echo "[Step 4] Verifying output..."
hdfs dfs -ls $HDFS_BASE/output/final_clustered_points/
hdfs dfs -cat $HDFS_BASE/output/final_clustered_points/part-m-00000 | head -5

echo ""
echo "Done! Now open Apache Zeppelin:"
echo "  1. Go to http://localhost:8080"
echo "  2. Import CS585_Part3d_Visualization.json as a new notebook"
echo "  3. Update HDFS_INPUT path in Cell 1 to: $HDFS_BASE/output/final_clustered_points/part-m-00000"
echo "  4. Run all cells in order"
echo "  5. In Cell 2 output, click the scatter chart icon to switch to scatter view"
