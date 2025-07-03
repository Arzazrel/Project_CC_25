package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Class to implement a MapReduce word count, the simplest version.
 */
public class WordCount
{
    /**
     * Mapper class for implement the map logic for the word count
     */
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);  // var to set the value to associate with the found words
        private final Text word = new Text();       // var which will contain the word that will be used as the key

        // map function
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());  // split the given input text line into tokens
            // iterate over each token obtained from the line
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());      // set the var text with the current word (token)
                context.write(word, one);       // emit the pair (key,value)
            }
        }
    }

    /**
     * Reducer class to implement the reduce logic for the word count
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();   // var to set the value to associate with the found words

        // reduce function
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {
            int sum = 0;                            // var to the sum of the values associated to keys
            for (final IntWritable val : values) {
                sum += val.get();                   // update sum
            }
            result.set(sum);                // set the result with sum
            context.write(key, result);     // emit
        }
    }

    /**
     * function to format the time passed as parameter
     *
     * @param millis    time in milliseconds
     * @return          the time in the correct format
     */
    public static String formatDuration(long millis) {
        long hours = millis / (1000 * 60 * 60);
        millis %= (1000 * 60 * 60);

        long minutes = millis / (1000 * 60);
        millis %= (1000 * 60);

        long seconds = millis / 1000;
        millis %= 1000;

        return String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds, millis);
    }

    /**
     * main function of the wordcount application.
     *
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {

        // check the number of argument passed
        if (args.length < 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }
        long startTime, endTime, duration;              // var to take the effective execution time

        final Configuration conf = new Configuration(); // create configuration object
        final Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCount.class);

        job.setOutputKeyClass(Text.class);              // set the typer for the output key for reducer
        job.setOutputValueClass(IntWritable.class);     // set the typer for the output value for reducer

        job.setMapperClass(WordCountMapper.class);      // set mapper
        job.setReducerClass(WordCountReducer.class);    // set reducer

        FileInputFormat.addInputPath(job, new Path(args[0]));       // first argument is the input folder
        FileOutputFormat.setOutputPath(job, new Path(args[1]));     // second argument is the output folder

        startTime = System.currentTimeMillis();         // start time

        boolean success = job.waitForCompletion(true);      // wait the end of the job

        endTime = System.currentTimeMillis();           // end time
        duration = endTime - startTime;                 // time in milliseconds

        // take some statistics and visualize to console
        if (success)    // if job ended with success
        {
            // get some statistics
            Counters counters = job.getCounters();
            long mapInputRecords = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
            long mapOutputRecords = counters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
            long reduceInputRecords = counters.findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue();
            long reduceOutputRecords = counters.findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
            long spilledRecords = counters.findCounter(TaskCounter.SPILLED_RECORDS).getValue();

            // print to console statistcs
            System.out.println("=== Job Statistics ===");
            System.out.println("Job Name: " + job.getJobName());
            System.out.println("Job ID: " + job.getJobID());
            System.out.println("Tracking URL: " + job.getTrackingURL());
            System.out.println("Map Input Records: " + mapInputRecords);
            System.out.println("Map Output Records: " + mapOutputRecords);
            System.out.println("Reduce Input Records: " + reduceInputRecords);
            System.out.println("Reduce Output Records: " + reduceOutputRecords);
            System.out.println("Spilled Records: " + spilledRecords);
            System.out.println("Application time: " + formatDuration(duration));

            // write in a file txt -- see Note 0
            try (PrintWriter writer = new PrintWriter(new FileWriter("job_stats.txt", true))) {
                writer.println("=== Job Statistics ===");
                writer.println("Job Name: " + job.getJobName());
                writer.println("Job ID: " + job.getJobID());
                writer.println("Tracking URL: " + job.getTrackingURL());
                writer.println("Map Input Records: " + mapInputRecords);
                writer.println("Map Output Records: " + mapOutputRecords);
                writer.println("Reduce Input Records: " + reduceInputRecords);
                writer.println("Reduce Output Records: " + reduceOutputRecords);
                writer.println("Spilled Records: " + spilledRecords);
                writer.println("Application time: " + formatDuration(duration));
                writer.println("------------------------------------------");
            }
        }

        System.exit(success ? 0 : 1);   // exit
    }
}
/*
NOTE 0:
    The job_stats.txt file is created in the same directory where you run the Java command (not in HDFS).
    If the file already exists, the data is appended to the end (FileWriter with true for append mode).
    The "Tracking URL" field shows you the address to monitor the job from the browser, useful if Hadoop is running in pseudo-distributed mode or on a real cluster.


 */
