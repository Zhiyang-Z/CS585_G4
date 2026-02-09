import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.List;
import java.util.ArrayList;

import java.io.IOException;

public class JobDriver {
    public static class JobMapper_Follows extends Mapper<LongWritable, Text, Text, Text> {

        private Text outK = new Text(); // Create a Text object for output key
        private Text outV = new Text("1");

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // 1. get a line at a time from the input data file
            String line = value.toString();
            // 2. divide the line we got from step 1 into words by specifying the text delimiter ","
            String[] records = line.split(",");
            if (key.get() > 0) { // Skip the header line
                outK.set(records[1]);
                context.write(outK, outV);
            }
        }
    }

    public static class JobMapper_NetPage extends Mapper<LongWritable, Text, Text, Text> {

        private Text outK = new Text(); // Create a Text object for output key
        private Text outV = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // 1. get a line at a time from the input data file
            String line = value.toString();
            // 2. divide the line we got from step 1 into words by specifying the text delimiter ","
            String[] records = line.split(",");
            if (key.get() > 0) { // Skip the header line
                outK.set(records[0]);
                outV.set(records[1]);
                context.write(outK, outV);
            }
        }
    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        private Text outV = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            String user_name = "";

            boolean counter = false;
            for (Text value : values) {
                if (value.toString().equals("1")) {
                    counter = true;
                    sum += Integer.parseInt(value.toString());
                } else {
                    user_name = "name," + value.toString();
                }
            }
            // write out according to mapper type
            if (!counter) {
                outV.set(user_name);
            } else {
                outV.set(String.valueOf(sum));
            }
            // System.out.println("debug: " + key + ", " + String.valueOf(sum) + ", " + user_name); // for debug!

            // write out the result (output (key, value) pair of reduce phase)
            context.write(key,outV);
        }
    }

    public static class JobReducer_All extends Reducer<Text, Text, Text, IntWritable> {
        private List<String> user_name_list = new ArrayList<>();
        private List<Integer> follow_list = new ArrayList<>();
        private Text outV = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            // get values length
            String user_name = "";
            int user_follows = 0;
            for (Text value : values) {
                if (value.toString().contains(",")) {
                    user_name = value.toString().split(",")[1];
                } else {
                    user_follows += Integer.parseInt(value.toString().trim());
                }
            }
            user_name_list.add(user_name);
            follow_list.add(user_follows);
            // System.out.println("debug: " + key + ", " + user_follows + ", " + user_name);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // calculate the mean value of all follows
            int total_follows = 0;
            for (int i = 0; i < follow_list.size(); i++) {
                total_follows += follow_list.get(i);
            }
            double mean_follows = (double) total_follows / follow_list.size();
            // only output users with follows greater than the mean
            for (int i = 0; i < user_name_list.size(); i++) {
                if (follow_list.get(i) > mean_follows) {
                    context.write(new Text(user_name_list.get(i)), new IntWritable(follow_list.get(i)));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1. create a job object
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. map the classes
        job.setJarByClass(JobDriver.class);
        MultipleInputs.addInputPath(job, new Path("/home/zhiyang/data/CircleNetPage.csv"), TextInputFormat.class, JobMapper_NetPage.class);
        MultipleInputs.addInputPath(job, new Path("/home/zhiyang/data/Follows.csv"), TextInputFormat.class, JobMapper_Follows.class); // second mapper
        // job.setMapperClass(JobMapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(JobReducer_All.class);

        // 3. set up the output key value data type class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4. Specify the input and output path
        // FileInputFormat.setInputPaths(job, new Path("/home/zhiyang/data/CircleNetPage.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/home/zhiyang/output/project3f/"));

        job.setNumReduceTasks(1); // important to have a single reducer for top 10 calculation

        // 5. submit job!
        boolean result = job.waitForCompletion(true);
        
        System.exit(result ? 0 : 1);
    }
}