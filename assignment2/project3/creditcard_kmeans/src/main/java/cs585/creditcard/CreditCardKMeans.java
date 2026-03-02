package cs585.creditcard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * K-Means MapReduce for Credit Card Customer Data (Part 3 - Task d visualization).
 *
 * Runs K-Means with combiner optimization and early termination.
 * After convergence, runs a final map-only assignment job to produce
 * the full clustered output file needed for Zeppelin visualization.
 *
 * Input data columns (features only, IDs already stripped):
 *   avg_credit_limit, total_credit_cards, total_visits_bank,
 *   total_visits_online, total_calls_made
 *
 * Usage:
 *   hadoop jar creditcard_kmeans.jar cs585.creditcard.CreditCardKMeans \
 *     <input_data> <seeds_file> <output_base> <R> [threshold]
 */
public class CreditCardKMeans extends Configured implements Tool {

    private static final String CENTERS_KEY = "kmeans.centers";

    // -----------------------------------------------------------------------
    // Shared utility: load centers from HDFS
    // -----------------------------------------------------------------------
    private static Point5D[] loadCenters(Configuration conf, String path) throws Exception {
        List<Point5D> centers = new ArrayList<>();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (!line.isEmpty()) centers.add(Point5D.fromString(line));
        }
        reader.close();
        return centers.toArray(new Point5D[0]);
    }

    private static String serializeCenters(Point5D[] centers) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < centers.length; i++) {
            if (i > 0) sb.append("|");
            sb.append(centers[i].toString());
        }
        return sb.toString();
    }

    private static Point5D[] deserializeCenters(String s) {
        String[] parts = s.split("\\|");
        Point5D[] centers = new Point5D[parts.length];
        for (int i = 0; i < parts.length; i++) centers[i] = Point5D.fromString(parts[i]);
        return centers;
    }

    private static boolean hasConverged(Point5D[] oldC, Point5D[] newC, double threshold) {
        for (int i = 0; i < oldC.length; i++)
            if (!oldC[i].hasConverged(newC[i], threshold)) return false;
        return true;
    }

    // -----------------------------------------------------------------------
    // Phase 1 Mapper: assign each point to nearest center
    // Emits: (clusterIdx, "d0,d1,d2,d3,d4,1")
    // -----------------------------------------------------------------------
    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
        private Point5D[] centers;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            centers = deserializeCenters(context.getConfiguration().get(CENTERS_KEY));
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            Point5D point = Point5D.fromString(line);
            int nearest = point.nearestCenter(centers);
            context.write(new IntWritable(nearest), new Text(point.toString() + ",1"));
        }
    }

    // -----------------------------------------------------------------------
    // Phase 1 Combiner: local partial aggregation
    // Input:  (clusterIdx, ["d0,d1,d2,d3,d4,count", ...])
    // Output: (clusterIdx, "sumD0,sumD1,sumD2,sumD3,sumD4,totalCount")
    // -----------------------------------------------------------------------
    public static class KMeansCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double[] sum = new double[5];
            long count = 0;
            for (Text t : values) {
                String[] p = t.toString().split(",");
                for (int i = 0; i < 5; i++) sum[i] += Double.parseDouble(p[i]);
                count += Long.parseLong(p[5]);
            }
            context.write(key, new Text(String.format(
                "%.4f,%.4f,%.4f,%.4f,%.4f,%d",
                sum[0], sum[1], sum[2], sum[3], sum[4], count)));
        }
    }

    // -----------------------------------------------------------------------
    // Phase 1 Reducer: aggregate partial sums -> new centroid
    // Output: (clusterIdx, "newD0,newD1,newD2,newD3,newD4")
    // -----------------------------------------------------------------------
    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double[] sum = new double[5];
            long count = 0;
            for (Text t : values) {
                String[] p = t.toString().split(",");
                for (int i = 0; i < 5; i++) sum[i] += Double.parseDouble(p[i]);
                count += Long.parseLong(p[5]);
            }
            if (count == 0) return;
            Point5D newCenter = new Point5D(
                sum[0]/count, sum[1]/count, sum[2]/count, sum[3]/count, sum[4]/count);
            context.write(key, new Text(newCenter.toString()));
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2 Mapper: final assignment (map-only)
    // For each point, emit its cluster assignment + center coordinates
    // Output line: "clusterIdx,d0,d1,d2,d3,d4,centerD0,centerD1,centerD2,centerD3,centerD4"
    // -----------------------------------------------------------------------
    public static class AssignMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Point5D[] finalCenters;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            finalCenters = deserializeCenters(context.getConfiguration().get(CENTERS_KEY));
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            Point5D point = Point5D.fromString(line);
            int clusterIdx = point.nearestCenter(finalCenters);
            Point5D center = finalCenters[clusterIdx];
            // Output: clusterIdx, all point dims, all center dims
            String output = clusterIdx + "," + point.toString() + "," + center.toString();
            context.write(NullWritable.get(), new Text(output));
        }
    }

    // -----------------------------------------------------------------------
    // Driver
    // -----------------------------------------------------------------------
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: CreditCardKMeans <input> <seeds> <output_base> <R> [threshold]");
            return 1;
        }

        String inputPath  = args[0];
        String seedsPath  = args[1];
        String outputBase = args[2];
        int R             = Integer.parseInt(args[3]);
        double threshold  = args.length >= 5 ? Double.parseDouble(args[4]) : 0.001;

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        Point5D[] oldCenters = loadCenters(conf, seedsPath);
        int K = oldCenters.length;

        System.out.println("=== Credit Card K-Means (K=" + K + ", R=" + R + ", threshold=" + threshold + ") ===");

        String currentCentersPath = seedsPath;
        boolean converged = false;
        int round;

        // ---- Phase 1: Training ----
        for (round = 1; round <= R; round++) {
            System.out.println("\n--- Round " + round + " ---");
            oldCenters = loadCenters(conf, currentCentersPath);
            conf.set(CENTERS_KEY, serializeCenters(oldCenters));

            String roundOutput = outputBase + "/round_" + round;
            Path outPath = new Path(roundOutput);
            if (fs.exists(outPath)) fs.delete(outPath, true);

            Job job = Job.getInstance(conf, "CreditCard KMeans Round " + round);
            job.setJarByClass(CreditCardKMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, outPath);

            if (!job.waitForCompletion(true)) return 1;

            currentCentersPath = roundOutput + "/part-r-00000";
            Point5D[] newCenters = loadCenters(conf, currentCentersPath);

            if (hasConverged(oldCenters, newCenters, threshold)) {
                converged = true;
                System.out.println("Converged after round " + round + ".");
                break;
            }
            oldCenters = newCenters;
        }

        System.out.println("\n=== Training done. Converged: " + converged + " after " + round + " round(s). ===");

        // Load final centers
        Point5D[] finalCenters = loadCenters(conf, currentCentersPath);
        conf.set(CENTERS_KEY, serializeCenters(finalCenters));

        // Print final centers
        System.out.println("Final cluster centers:");
        for (int i = 0; i < finalCenters.length; i++) {
            System.out.println("  Cluster " + i + ": " + finalCenters[i].toString());
        }

        // ---- Phase 2: Final assignment (map-only for visualization) ----
        System.out.println("\nPhase 2: Assigning all points to final clusters...");
        String assignOutput = outputBase + "/final_clustered_points";
        Path assignOutPath = new Path(assignOutput);
        if (fs.exists(assignOutPath)) fs.delete(assignOutPath, true);

        Job assignJob = Job.getInstance(conf, "CreditCard KMeans Final Assignment");
        assignJob.setJarByClass(CreditCardKMeans.class);
        assignJob.setMapperClass(AssignMapper.class);
        assignJob.setNumReduceTasks(0);  // map-only
        assignJob.setOutputKeyClass(NullWritable.class);
        assignJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(assignJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(assignJob, assignOutPath);

        if (!assignJob.waitForCompletion(true)) return 1;

        System.out.println("Clustered points written to: " + assignOutput);
        System.out.println("Format: clusterIdx,d0,d1,d2,d3,d4,centerD0,centerD1,centerD2,centerD3,centerD4");
        System.out.println("\nNow open Apache Zeppelin and run the visualization notebook.");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new CreditCardKMeans(), args));
    }
}
