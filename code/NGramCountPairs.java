package it.unipi.hadoop;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskCounter;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Class to implement a MapReduce CoOccurrence count, the pairs version.
 *
 */
public class NGramCountPairs
{
    /**
     * Mapper class for implement the map logic for the CoOccurrence count
     */
    public static class NGramCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final static IntWritable ONE = new IntWritable(1);  // the value for each pairs
        private Text nGram = new Text();                            // var to contain the key (word1,word2,...,wordN-1)
        private int window;                                         // var to contain the window size


        // to take the window size from configuration
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();    // retrieve configuration object
            window = conf.getInt("window",2);                   // get the configuration to contain the N of N-Gram (default value is 2)
        }

        // map function
        protected void map(final LongWritable key,final Text value,final Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString().toLowerCase()
                    .replaceAll("[^\\p{L}0-9\\s'-]", " ")           // removes unwanted characters, keeps only letters, numbers and ',-
                    .replaceAll("(?<=\\s)['-]+|[\'-]+(?=\\s)", " ") // removes isolated '-' or ''' between spaces
                    .replaceAll("(^|\\s)['-]+", " ")                // removes '-' or ''' at the beginning of the word
                    .replaceAll("[-']+(\\s|$)", " ")                // removes '-' or ''' at the end of the word
                    .trim()                                         // removes leading and trailing whitespace
                    .split("\\s+");                                 // splits the string into an array of words, using one or more consecutive whitespace as separators.
            boolean errorWord = false;      // indicates if empty words were found in the current window, you should move to the next iteration without issuing anything

            if (words.length < window)      // check to see if the record has length less than the window
                return;

            for (int i = 0; i < (words.length - (window - 1)); i++)          // iterate over each word obtained from the line
            {
                StringBuilder wordsKey = new StringBuilder();   // the builder for the key formed by the union of the various words of the n-gram
                errorWord = false;                              // reset var
                
                for (int j = 0; j < window; j++)        // iteration to retrieve all the word in the current window
                {
                    if (words[i + j].isEmpty())     // check if at least one word is empty
                    {
                        errorWord = true;               // set var
                        break;                          // exit to the creation of the key string
                    }
                    wordsKey.append(words[i + j]);      // add word
                    if (j != (window-1))            // check if is not the last iteration
                        wordsKey.append(",");  // add ','
                }

                if (errorWord)   // control check for the key
                    continue;                   // go to next iteration

                nGram.set(wordsKey);            // create the key = (current word,next word)
                context.write(nGram, ONE);      // emit key-value pairs
            }
        }
    }

    /**
     * Reducer class to implement the reduce logic for the CoOccurrence count
     */
    public static class NGramCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        LocalDateTime now = LocalDateTime.now();        // get the current date
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss");   // format the date
        return now.format(formatter);
    }

    // ---------------------- end: utility functions ----------------------

    /**
     * Main function of the n-gram count application.
     * It repeats the same job multiple times and will collect metrics each time and then show an average of some,
     * useful for testing. In the output data folder a different subfolder will be generated for each job run executed,
     * at the end of the name there will be the number related to the job to distinguish the different outputs.
     *
     * @param args          <input path> <output base path> [window(N)] [numRuns] [useCombiner(true|false)] [numReducer]
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {

        String jobName = "NGramCountPairs";     // name for the job
        String statsFileName = "job_stats.txt"; // name for the file containing the printed statistics of the jobs
        // check the number of argument passed
        if (args.length < 2) {
            System.err.println("Usage: " + jobName + " <input path> <output base path> [window(N)] [numRuns] [useCombiner(true|false)] [numReducer]");
            System.exit(-1);
        }
        // ---- take all the argument ----
        String inputPath = args[0];         // take the input folder
        String outputBasePath = args[1];    // take base output folder
        // -- get window size --
        int window = 2;                     // default value for the window (2-gram)
        if (args.length >= 3)               // check if the user entered the fourth argument
        {
            try {
                window = Integer.parseInt(args[2]); // get the number indicating the size of the window
            } catch (NumberFormatException e) {
                System.err.println("Invalid window size: " + args[2]);
                System.exit(-1);
            }

            if (window < 2)                         // check for the window size
            {
                System.err.println("Window size must be >= 2");
                System.exit(-1);
            }
        }
        // -- get te number of runs to do --
        int numRunsRequested = 1;           // default value for the run
        if (args.length >= 4)               // check if the user entered the fifth argument
        {
            try {
            numRunsRequested = Integer.parseInt(args[3]);   // take the number of runs to do
            } catch (NumberFormatException e) {
                System.err.println("Invalid num of runs: " + args[3]);
                System.exit(-1);
            }

            if (numRunsRequested <= 0)              // check for the num of the runs to do
            {
                System.err.println("Num of runs to do must be >= 1");
                System.exit(-1);
            }
        }
        // -- get the combiner choice --
        boolean useCombiner = false;
        if (args.length >= 5)           // check if the user entered the sixth argument
            useCombiner = Boolean.parseBoolean(args[4]);

        // -- get the number of reducer --
        int numReducer = 1;             // default value for the num of the reducer
        if (args.length >= 6)           // check if the user entered the seventh argument
        {
            try {
                numReducer = Integer.parseInt(args[5]);     // take the number of reducer task to execute
            } catch (NumberFormatException e) {
                System.err.println("Invalid num of reducer: " + args[5]);
                System.exit(-1);
            }

            if (numReducer <= 0)                            // check for the num of the runs to do
            {
                System.err.println("Num of reducer to do must be >= 0");
                System.exit(-1);
            }
        }

        // ---- instantiates and executes jobs ----
        // var to manage the runs
        int successfulRuns = 0;             // run successfully completed
        long totalTime = 0;                 // total time used to perform all the runs of the job
        int attempt = 0;                    // run's attempt

        // while to perform the severals runs of the application
        while (successfulRuns < numRunsRequested)
        {
            long startTime, endTime, duration;              // var to take the effective execution time

            final Configuration conf = new Configuration(); // create configuration object
            conf.setInt("window",window);                   // set the configuration to contain the N of N-Gram
            final Job job = Job.getInstance(conf, jobName + "_run_" + successfulRuns + "_w" + window + "_comb_" + useCombiner + "_red_" + numReducer);
            job.setJarByClass(NGramCountPairs.class);

            job.setOutputKeyClass(Text.class);              // set the typer for the output key for reducer
            job.setOutputValueClass(IntWritable.class);     // set the typer for the output value for reducer

            job.setMapperClass(NGramCountMapper.class);         // set mapper
            if (useCombiner)
                job.setCombinerClass(NGramCountReducer.class);  // set combiner -> See NOTE 1
            job.setReducerClass(NGramCountReducer.class);       // set reducer
            job.setNumReduceTasks(numReducer);                  // to set the number of the reducer task

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
                try (PrintWriter writer = new PrintWriter(new FileWriter(statsFileName, true))) {
                    writer.println("------------------------------------------");
                    writer.println("=== Job Statistics ===");
                    writer.println("Identifiers:");
                    writer.println("Date: " + getCurrentDateTime());
                    writer.println("Job Name: " + job.getJobName());
                    writer.println("Job ID: " + job.getJobID());
                    writer.println("Tracking URL: " + job.getTrackingURL());
                    writer.println("Parameters:");
                    writer.println("Input Path: " + inputPath);
                    writer.println("Output Path: " + outputPath);
                    writer.println("Window Size: " + window);
                    writer.println("Use Combiner: " + useCombiner);
                    writer.println("Num Reducers: " + numReducer);
                    writer.println("Run Attempt: " + attempt);
                    writer.println("Data:");
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
        System.out.println("Average execution time: " + formatDuration((long)averageTime));
        System.out.println("Statistics written to: " + new File(statsFileName).getAbsolutePath());

        // write in a file txt -- see Note 0
        try (PrintWriter writer = new PrintWriter(new FileWriter(statsFileName, true))) {
            writer.println("------------------------------------------");
            writer.println("=== Final Recap of Runs for " + jobName + " ===");
            writer.println("Run:");
            writer.println("Requested run: " + numRunsRequested);
            writer.println("Successfull run: " + successfulRuns);
            writer.println("Total attempt run: " + attempt);
            writer.println("Failed run: " + (attempt - successfulRuns));
            writer.println("Time:");
            writer.println("Total execution time: " + formatDuration(totalTime));
            writer.println("Average execution time: " + formatDuration((long)averageTime));
            writer.println("------------------------------------------");
        }

        System.exit(successfulRuns == numRunsRequested ? 0 : 1);   // exit, 0: all ok , 1: error
    }
}
/*
NOTE 0:
    The job_stats.txt file is created in the same directory where you run the Java command (not in HDFS).
    If the file already exists, the data is appended to the end (FileWriter with true for append mode).
    The "Tracking URL" field shows you the address to monitor the job from the browser, useful if Hadoop is running in
    pseudo-distributed mode or on a real cluster.
    Designed for testing purposes to obtain the most important statistics for each job in a pre-formatted, easily
    accessible file. In non-test applications or on distributed clusters, it's best to comment out the entire section
    related to writing to this file and take the information printed on the command line.
NOTE 1:
    The combiner is normal combiner isn't in-mapper combiner. Hadoop can execute the combiner or not (is optional).
 */
