import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class JobDriver {

    // Parse CSV line into double array
    private static double[] parsePoint(String line) {
        String[] parts = line.trim().split(",");
        double[] point = new double[parts.length];
        for (int i = 0; i < parts.length; i++) point[i] = Double.parseDouble(parts[i]);
        return point;
    }

    // Mapper for K-Means iteration
    public static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {
        private List<double[]> centroids = new ArrayList<>();
        private int k = 0;
        private Text outK = new Text();
        private Text outV = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    Path path = new Path(uri.getPath());
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    String line;
                    while ((line = br.readLine()) != null) centroids.add(parsePoint(line));
                    br.close();
                }
                k = centroids.size();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double[] curPoint = parsePoint(value.toString());
            int closestIndex = -1;
            double minDist = Double.MAX_VALUE;
            for (int i = 0; i < k; i++) {
                double dist = 0;
                for (int j = 0; j < curPoint.length; j++) {
                    double diff = curPoint[j] - centroids.get(i)[j];
                    dist += diff * diff;
                }
                if (dist < minDist) {
                    minDist = dist;
                    closestIndex = i;
                }
            }
            outK.set(String.valueOf(closestIndex));
            outV.set(value);
            context.write(outK, outV);
        }
    }

    // Combiner for partial sums
    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        private Text outV = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double[] sum = null;
            int count = 0;
            for (Text value : values) {
                double[] point = parsePoint(value.toString());
                if (sum == null) sum = new double[point.length];
                for (int i = 0; i < point.length; i++) sum[i] += point[i];
                count++;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(count);
            for (double v : sum) sb.append(",").append(v);
            outV.set(sb.toString());
            context.write(key, outV);
        }
    }

    // Reducer to compute new centroids
    public static class JobReducer_All extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double[] sum = null;
            int totalCount = 0;
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                int count = Integer.parseInt(parts[0]);
                if (sum == null) sum = new double[parts.length - 1];
                for (int i = 1; i < parts.length; i++) sum[i - 1] += Double.parseDouble(parts[i]);
                totalCount += count;
            }
            for (int i = 0; i < sum.length; i++) sum[i] /= totalCount;

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < sum.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(sum[i]);
            }
            context.write(NullWritable.get(), new Text(sb.toString()));
        }
    }

    // Helper: read centroids from HDFS
    private static List<double[]> readCentroidsFromHDFS(Configuration conf, String pathStr) throws IOException {
        List<double[]> centroids = new ArrayList<>();
        Path path = new Path(pathStr);
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.trim().split(",");
            double[] point = new double[parts.length];
            for (int i = 0; i < parts.length; i++) point[i] = Double.parseDouble(parts[i]);
            centroids.add(point);
        }
        br.close();
        return centroids;
    }

    // Convergence check
    private static boolean hasConverged(List<double[]> oldC, List<double[]> newC, double epsilon) {
        for (int i = 0; i < oldC.size(); i++) {
            double dist = 0;
            for (int j = 0; j < oldC.get(i).length; j++) {
                double diff = oldC.get(i)[j] - newC.get(i)[j];
                dist += diff * diff;
            }
            if (Math.sqrt(dist) > epsilon) return false;
        }
        return true;
    }

    // Mapper for final assignment of points to clusters
    public static class AssignFinalClusterMapper extends Mapper<LongWritable, Text, Text, Text> {
        private List<double[]> centroids = new ArrayList<>();
        private Text outK = new Text();
        private Text outV = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    Path path = new Path(uri.getPath());
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    String line;
                    while ((line = br.readLine()) != null) centroids.add(parsePoint(line));
                    br.close();
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double[] point = parsePoint(value.toString());
            int closestIndex = -1;
            double minDist = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double dist = 0;
                for (int j = 0; j < point.length; j++) {
                    double diff = point[j] - centroids.get(i)[j];
                    dist += diff * diff;
                }
                if (dist < minDist) {
                    minDist = dist;
                    closestIndex = i;
                }
            }
            outK.set(String.valueOf(closestIndex));
            outV.set(value);
            context.write(outK, outV);
        }
    }

    // Reducer for final assignment output
    public static class AssignFinalClusterReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) context.write(key, v);
        }
    }

    // Main driver
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        int maxIter = 100;
        double epsilon = 0.001;
        boolean converged = false;
        int iteration = 0;
        List<double[]> oldCentroids = null;

        Configuration conf = new Configuration();
        String inputPath = "/home/zhiyang/data/kmeans_data.csv";
        String seedPath = "/home/zhiyang/data/kmeans_seeds.csv";
        String outputBase = "/home/zhiyang/output/kmeans_output/kmeans_output_iteration_";

        // --- Iterative K-Means ---
        while (!converged && iteration < maxIter) {
            Job job = Job.getInstance(conf, "KMeans Iteration " + iteration);

            if (iteration > 0)
                job.addCacheFile(new URI(outputBase + iteration + "/part-r-00000"));
            else
                job.addCacheFile(new URI(seedPath));

            job.setJarByClass(JobDriver.class);
            job.setMapperClass(JobMapper.class);
            job.setCombinerClass(Combiner.class);
            job.setReducerClass(JobReducer_All.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(conf);
            Path outPath = new Path(outputBase + (iteration + 1));
            if (fs.exists(outPath)) fs.delete(outPath, true);

            FileInputFormat.setInputPaths(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, outPath);

            boolean result = job.waitForCompletion(true);

            List<double[]> newCentroids = readCentroidsFromHDFS(conf, outPath + "/part-r-00000");
            if (oldCentroids != null) converged = hasConverged(oldCentroids, newCentroids, epsilon);
            oldCentroids = newCentroids;
            iteration++;
        }

        // --- Requirement (i): final centroids + convergence status ---
        System.out.println("KMeans finished after " + iteration + " iterations.");
        System.out.println("Converged: " + converged);
        System.out.println("Final centroids:");
        for (int i = 0; i < oldCentroids.size(); i++) {
            double[] c = oldCentroids.get(i);
            StringBuilder sb = new StringBuilder();
            sb.append("Centroid ").append(i).append(": ");
            for (int j = 0; j < c.length; j++) {
                if (j > 0) sb.append(",");
                sb.append(c[j]);
            }
            System.out.println(sb.toString());
        }

        // --- Requirement (ii): final cluster assignment ---
        Job finalJob = Job.getInstance(conf, "KMeans Final Assignment");
        finalJob.setJarByClass(JobDriver.class);
        finalJob.setMapperClass(AssignFinalClusterMapper.class);
        finalJob.setReducerClass(AssignFinalClusterReducer.class);

        finalJob.setMapOutputKeyClass(Text.class);
        finalJob.setMapOutputValueClass(Text.class);
        finalJob.setOutputKeyClass(Text.class);
        finalJob.setOutputValueClass(Text.class);

        finalJob.addCacheFile(new URI(outputBase + iteration + "/part-r-00000"));

        Path finalOut = new Path(outputBase + "final_result");
        if (FileSystem.get(conf).exists(finalOut)) FileSystem.get(conf).delete(finalOut, true);
        FileInputFormat.setInputPaths(finalJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(finalJob, finalOut);

        finalJob.waitForCompletion(true);
        System.out.println("Final clustered data points written to: " + finalOut.toString());
    }
}