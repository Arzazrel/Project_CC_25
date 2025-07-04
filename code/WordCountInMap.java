package it.unipi.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Class to implement a MapReduce word count, the in-mapping combining version.
 * In this version we will do local aggregation in the mapper, there will be an associative array that will be used to
 * keep the current occurrence for each word found so as to update the name at each iteration without emitting a key
 * and value each time (with value = 1). In practice you have to be careful about memory management, in this
 * implementation there will be control that 80% of usage is not exceeded.
 */
public class WordCountInMap
{
    /**
     * Mapper class for implement the map logic for the word count
     */
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private HashMap<String, Integer> wordCountMap;              // hash map to contain word occurrences
        private static final double MEMORY_THRESHOLD = 0.8;         // maximum usable memory threshold (80%)
        private final IntWritable countWritable = new IntWritable();// var to set the value to associate with the found words
        private final Text word = new Text();                       // var which will contain the word that will be used as the key

        // to initialize the data structures used fo in mapping
        protected void setup(Context context) throws IOException, InterruptedException {
            wordCountMap = new HashMap<>();     // set the hash map
        }

        // map function
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString());  // split the given input text line into tokens
            // iterate over each token obtained from the line
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();     // take the word
                wordCountMap.put(token, wordCountMap.getOrDefault(token, 0) + 1);
            }

            if (isMemoryThresholdExceeded())    // check the used memory
            {
                flush(context);     // emit and flush memory
            }
        }

        // to close the data structures used fo in mapping
        protected void cleanup(Context context) throws IOException, InterruptedException {
            flush(context);         // emit and flush memory
        }

        // ---------------------- start: utility functions for in-combining ----------------------
        // function to check the current used memory
        private boolean isMemoryThresholdExceeded() {
            long maxMemory = Runtime.getRuntime().maxMemory();      // get maximum memory the JVM can use (imposed limit).
            long totalMemory = Runtime.getRuntime().totalMemory();  // get current total memory allocated to the JVM.
            long freeMemory = Runtime.getRuntime().freeMemory();    // get Free memory inside totalMemory()

            long usedMemory = totalMemory - freeMemory;                 // calculate the current used memory
            long memoryLimit = (long) (maxMemory * MEMORY_THRESHOLD);   // calculate the threshold

            return usedMemory >= memoryLimit;       // check if the current used memory exceeds the threshold
        }

        // function to emit the data collected and flus the memory
        private void flush(Context context) throws IOException, InterruptedException {
            // I loop through the entire contents of the hasmap and output the key-values (word-occurrence) stored inside and then free the hashmap.
            for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
                word.set(entry.getKey());                   // set the word
                countWritable.set(entry.getValue());        // set the occurrence
                context.write(word, countWritable);         // emit
            }
            wordCountMap.clear();                           // clean hash map
        }
        // ---------------------- end: utility functions for in-combining ----------------------
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

    // ---------------------- start: utility functions ----------------------
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
     * function to retrieve the current date and time in the format "dd-MM-yyyy HH:mm:ss"
     *
     * @return  current date and time in this format "dd-MM-yyyy HH:mm:ss"
     */
    public static String getCurrentDateTime()
    {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");
        return now.format(formatter);
    }

    // ---------------------- end: utility functions ----------------------

    /**
     * main function of the wordcount application.
     * It repeats the same job multiple times and will collect metrics each time and then show an average of some,
     * useful for testing. In the output data folder a different subfolder will be generated for each job run executed,
     * at the end of the name there will be the number related to the job to distinguish the different outputs.
     *
     * @param args          <input path> <output base path> [numRuns]
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {

        String jobName = "WordCountInMap";       // name for the job
        // check the number of argument passed
        if (args.length < 2) {
            System.err.println("Usage: WordCountInMap <input path> <output base path> [numRuns]");
            System.exit(-1);
        }
        String inputPath = args[0];         // take the input folder
        String outputBasePath = args[1];    // take base output folder

        int numRunsRequested = 1;           // default value for the run
        if (args.length >= 3)               // check if the user entered the third argument
        {
            numRunsRequested = Integer.parseInt(args[2]);   // take the number of runs to do
        }
        // var to manage the runs
        int successfulRuns = 0;             // run successfully completed
        long totalTime = 0;                 // total time used to perform all the runs of the job
        int attempt = 0;                    // run's attempt

        // while to perform the severals runs of the application
        while (successfulRuns < numRunsRequested)
        {
            long startTime, endTime, duration;              // var to take the effective execution time

            final Configuration conf = new Configuration(); // create configuration object
            final Job job = new Job(conf, jobName + "_run_" + successfulRuns);
            job.setJarByClass(WordCountInMap.class);

            job.setOutputKeyClass(Text.class);              // set the typer for the output key for reducer
            job.setOutputValueClass(IntWritable.class);     // set the typer for the output value for reducer

            job.setMapperClass(WordCountMapper.class);      // set mapper
            //job.setCombinerClass(WordCountReducer.class);   // set combiner -> See NOTE 1
            job.setReducerClass(WordCountReducer.class);    // set reducer

            FileInputFormat.addInputPath(job, new Path(inputPath));     // first argument is the input folder

            // Output folder specific for each successful run
            String outputPath = outputBasePath + "/output_run_" + successfulRuns;   // set sub-path for the output
            FileOutputFormat.setOutputPath(job, new Path(outputPath));              // give the output path to the job
            System.out.println("\n--- Starting Job Attempt " + attempt + " ---");

            startTime = System.currentTimeMillis();         // start time
            boolean success = job.waitForCompletion(true);  // wait the end of the job
            endTime = System.currentTimeMillis();           // end time

            // take some statistics and visualize to console
            if (success)    // if job ended with success
            {
                duration = endTime - startTime;             // time in milliseconds
                totalTime += duration;                      // update total time
                successfulRuns++;                           // update the number of the succesfully terminated job
                System.out.println("Job " + successfulRuns + " completed successfully in " + formatDuration(duration));

                // get some statistics
                Counters counters = job.getCounters();
                long mapInputRecords = counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
                long mapOutputRecords = counters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue();
                long reduceInputRecords = counters.findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue();
                long reduceOutputRecords = counters.findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue();
                long spilledRecords = counters.findCounter(TaskCounter.SPILLED_RECORDS).getValue();

                // print to console statistcs
                System.out.println("=== Job Statistics ===");
                System.out.println("Date: " + getCurrentDateTime());
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
                    writer.println("------------------------------------------");
                    writer.println("=== Job Statistics ===");
                    writer.println("Date: " + getCurrentDateTime());
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
            else    // job failed
            {
                System.out.println("Job attempt " + attempt + " failed. Retrying...");
            }
            attempt++;      // update the attempt
        }       // -- end - while --

        double averageTime = totalTime / (double) successfulRuns;       // calculate the average execution time
        System.out.println("\n=== All " + successfulRuns + " jobs completed successfully ===");
        System.out.println("Average execution time: " + formatDuration((long)averageTime) + " ms");

        System.exit(successfulRuns == numRunsRequested ? 0 : 1);   // exit, 0: all ok , 1: error
    }
}
/*
NOTE 0:
    The job_stats.txt file is created in the same directory where you run the Java command (not in HDFS).
    If the file already exists, the data is appended to the end (FileWriter with true for append mode).
    The "Tracking URL" field shows you the address to monitor the job from the browser, useful if Hadoop is running in
    pseudo-distributed mode or on a real cluster.
NOTE 1:
    the combiner is normal combiner isn't in-mapper combiner. Hadoop can execute the combiner or not (is optional).
    If you don't want to set the combiner just comment out the whole line.
 */
