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
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Class to implement a MapReduce CoOccurrence count, the stripes version.
 *
 */
public class CoOccurrenceStripesInMap
{
    /**
     * Mapper class for implement the map logic for the CoOccurrence count
     */
    public static class CoOccurrenceMapper extends Mapper<LongWritable, Text, Text, MapWritable>
    {
        HashMap<String, MapWritable> wordSeenMap;               // hash map to contain the words already seen
        private static final double MEMORY_THRESHOLD = 0.8;     // maximum usable memory threshold (80%)

        // to initialize the data structures used fo in mapping
        protected void setup(Context context) throws IOException, InterruptedException {
            wordSeenMap = new HashMap<>();          // set the hash map
        }

        // map function
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = value.toString()
                                  .toLowerCase()
                                  .replaceAll("[^a-zA-Z0-9\\s]", "")    // removes punctuation
                                  .split("\\s+");                       // split the given input text line into words

            if (words.length < 2)   // check for record withs size less than the window of N-gram (in this case 2-gram)
                return;

            for (int i = 0; i < words.length - 1; i++)      // iterate over each word obtained from the line
            {
                String w1 = words[i];                           // take the current word
                String w2 = words[i + 1];                       // take the next word
                if (w1.isEmpty() || w2.isEmpty())               // control check for the key
                    continue;

                if (!wordSeenMap.containsKey(w1))   // check if the current word has it already been seen or not
                    wordSeenMap.put(w1, new MapWritable());     // set the stripes for the new word

                // update the value for the co-occurrence
                MapWritable stripe = wordSeenMap.get(w1);       // get stripe for the current word
                Text neighbor = new Text(w2);                   // neighbour of the current word

                if (stripe.containsKey(neighbor))       // neighbor already seen, value must be updated
                {
                    IntWritable count = (IntWritable) stripe.get(neighbor); // get old occurrence
                    count.set(count.get() + 1);                             // update occurrence
                }
                else                                    // new neighbor
                {
                    stripe.put(neighbor, new IntWritable(1));
                }
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
            // now emit all the word and associated stripes
            for (Map.Entry<String, MapWritable> entry : wordSeenMap.entrySet()) {
                Text word = new Text(entry.getKey());              // get key (word)
                MapWritable stripe = entry.getValue();             // get stripe
                context.write(word, stripe);                       // emit (word, stripe)
            }
            wordSeenMap.clear();                                    // clean hash map
        }
        // ---------------------- end: utility functions for in-combining ----------------------
    }

    /**
     * Reducer class to implement the reduce logic for the CoOccurrence count
     */
    public static class CoOccurrenceReducer extends Reducer<Text, MapWritable, Text, Text> {
        // reduce function
        protected void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            MapWritable aggregateMap = new MapWritable();       // define aggregate map for

            for (MapWritable map : values)              // for each stripes associated to a word
            {
                for (Writable k : map.keySet())         // for each neighbour in the stripe
                {
                    IntWritable count = (IntWritable) map.get(k);               // get local occurrence
                    IntWritable existing = (IntWritable) aggregateMap.get(k);   // get total occurrence

                    if (existing != null)           // this pair of words has been found before
                        existing.set(existing.get() + count.get());             // update total occurrence
                    else                            // this pair of words has not been found before
                        aggregateMap.put(k, new IntWritable(count.get()));      // set (first time) the total occurrence
                }
            }

            context.write(key, new Text(mapWritableToString(aggregateMap)));  // emit word and aggregate stripe associated
        }

        // utility function to format stripes into a readable string
        public static String mapWritableToString(MapWritable map)
        {
            StringBuilder sb = new StringBuilder();     // the builder for the output string

            for (Map.Entry<Writable, Writable> entry : map.entrySet())      // get all the word and occurrence
            {
                Text key = (Text) entry.getKey();                           // get the neighbour
                IntWritable value = (IntWritable) entry.getValue();         // get the occurrence
                sb.append(key.toString()).append(":").append(value.get()).append(", "); // format the string
            }

            return sb.toString();                       // return the formatted string
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

        String jobName = "CoOccurrenceStripesInMap";        // name for the job
        // check the number of argument passed
        if (args.length < 2) {
            System.err.println("Usage: CoOccurrenceStripesInMap <input path> <output base path> [numRuns]");
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
            final Job job = Job.getInstance(conf, jobName + "_run_" + successfulRuns);
            job.setJarByClass(CoOccurrenceStripesInMap.class);

            job.setMapOutputKeyClass(Text.class);           // set the typer for the output key for mapper
            job.setMapOutputValueClass(MapWritable.class);  // set the typer for the output value for mapper

            job.setOutputKeyClass(Text.class);              // set the typer for the output key for reducer
            job.setOutputValueClass(Text.class);            // set the typer for the output value for reducer

            job.setMapperClass(CoOccurrenceMapper.class);       // set mapper
            //job.setCombinerClass(CoOccurrenceReducer.class);  // set combiner -> See NOTE 1
            job.setReducerClass(CoOccurrenceReducer.class);     // set reducer

            //job.setNumReduceTasks(2);                           // to set the number of the reducer task

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
