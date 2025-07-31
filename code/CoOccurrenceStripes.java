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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

/**
 * Class to implement a MapReduce CoOccurrence count, the stripes version.
 *
 */
public class CoOccurrenceStripes
{
    /**
     * Mapper class for implement the map logic for the CoOccurrence count (vers. stripes)
     */
    public static class CoOccurrenceMapper extends Mapper<LongWritable, Text, Text, MapWritable>
    {
        // map function
        protected void map(final LongWritable key,final Text value,final Context context)
                throws IOException, InterruptedException {
            HashMap<String, MapWritable> wordSeenMap = new HashMap<>(); // hash map to contain the words already seen
            String[] words = value.toString().toLowerCase()
                    .replaceAll("[^\\p{L}0-9\\s'-]", " ")           // removes unwanted characters, keeps only letters, numbers and ',-
                    .replaceAll("(?<=\\s)['-]+|[\'-]+(?=\\s)", " ") // removes isolated '-' or ''' between spaces
                    .replaceAll("(^|\\s)['-]+", " ")                // removes '-' or ''' at the beginning of the word
                    .replaceAll("[-']+(\\s|$)", " ")                // removes '-' or ''' at the end of the word
                    .trim()                                         // removes leading and trailing whitespace
                    .split("\\s+");                                 // splits the string into an array of words, using one or more consecutive whitespace as separators.

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
                    IntWritable countWritable = (IntWritable) stripe.get(neighbor); // get old occurrence
                    int count = countWritable.get();
                    stripe.put(neighbor, new IntWritable(count + 1));               // update occurrence
                }
                else                                    // new neighbor
                {
                    stripe.put(neighbor, new IntWritable(1));
                }
            }

            // now emit all the word and associated stripes
            for (Map.Entry<String, MapWritable> entry : wordSeenMap.entrySet()) {
                Text word = new Text(entry.getKey());              // get key (word)
                MapWritable stripe = entry.getValue();             // get stripe
                context.write(word, new MapWritable(stripe));      // emit (word, stripe)
            }
        }
    }

    /**
     * Combiner class to implement the reduce logic for the CoOccurrence count (vers. stripes)
     * A purpose-built combiner is created and the reducer is not used because otherwise the combiner's output would not
     * be in the same format as the reducer's input.
     * This is because the reducer outputs (text, text) pairs instead of (text, MapWritable) for greater readability.
     * To maintain the MapWritable's text transformation in the reducer, a combiner must be created that implements the
     * same logic as the reducer but that does not perform this transformation and outputs (Text, MapWritable) pairs.
     */
    public static class CoOccurrenceCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
        protected void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            MapWritable combinedStripe = new MapWritable(); // define aggregate map for

            for (MapWritable map : values)          // for each stripes associated to a word
            {
                for (Writable k : map.keySet())     // for each neighbour in the stripe
                {
                    IntWritable count = (IntWritable) map.get(k);               // get local occurrence
                    IntWritable existing = (IntWritable) combinedStripe.get(k); // get total occurrence

                    if (existing != null)       // this pair of words has been found before
                    {
                        int updatedCount = existing.get() + count.get();        // create updated count
                        combinedStripe.put(k, new IntWritable(updatedCount));   // update total occurrence
                    } else                      // this pair of words has not been found before
                        combinedStripe.put(k, new IntWritable(count.get()));    // set (first time) the total occurrence
                }
            }

            context.write(key, combinedStripe);         // emit word and aggregate stripe associated
        }
    }

    /**
     * Reducer class to implement the reduce logic for the CoOccurrence count (vers. stripes)
     */
    public static class CoOccurrenceReducer extends Reducer<Text, MapWritable, Text, Text> {
        // reduce function
        protected void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {
            MapWritable aggregateMap = new MapWritable();       // define aggregate map

            for (MapWritable map : values)              // for each stripes associated to a word
            {
                for (Writable k : map.keySet())         // for each neighbour in the stripe
                {
                    IntWritable count = (IntWritable) map.get(k);               // get local occurrence
                    IntWritable existing = (IntWritable) aggregateMap.get(k);   // get total occurrence

                    if (existing != null)           // this pair of words has been found before
                    {
                        int updatedCount = existing.get() + count.get();        // create updated count
                        aggregateMap.put(k, new IntWritable(updatedCount));     // update total occurrence
                    }
                    else                            // this pair of words has not been found before
                        aggregateMap.put(k, new IntWritable(count.get()));      // set (first time) the total occurrence
                }
            }

            context.write(key, new Text(mapWritableToString(aggregateMap)));    // emit word and aggregate stripe associated
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

            if (sb.length() > 2)
                sb.setLength(sb.length() - 2);          // removes the last comma and space

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
     * main function of the CoOccurrence word count application (vers. stripes).
     * It repeats the same job multiple times and will collect metrics each time and then show an average of some,
     * useful for testing. In the output data folder a different subfolder will be generated for each job run executed,
     * at the end of the name there will be the number related to the job to distinguish the different outputs.
     *
     * @param args          <input path> <output base path> [numRuns] [useCombiner(true|false)] [numReducer]
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {

        String jobName = "CoOccurrenceStripes";     // name for the job
        String statsFileName = "job_stats.txt";     // name for the file containing the printed statistics of the jobs
        // check the number of argument passed
        if (args.length < 2) {
            System.err.println("Usage: " + jobName + " <input path> <output base path> [numRuns] [useCombiner(true|false)] [numReducer]");
            System.exit(-1);
        }
        // ---- take all the argument ----
        String inputPath = args[0];         // take the input folder
        String outputBasePath = args[1];    // take base output folder
        // -- get te number of runs to do --
        int numRunsRequested = 1;           // default value for the run
        if (args.length >= 3)               // check if the user entered the third argument
        {
            try {
                numRunsRequested = Integer.parseInt(args[2]);   // take the number of runs to do
            } catch (NumberFormatException e) {
                System.err.println("Invalid num of runs: " + args[2]);
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
        if (args.length >= 4) {
            useCombiner = Boolean.parseBoolean(args[3]);
        }
        // -- get the number of reducer --
        int numReducer = 1;             // default value for the num of the reducer
        if (args.length >= 5)           // check if the user entered the third argument
        {
            try {
                numReducer = Integer.parseInt(args[4]);     // take the number of reducer task to execute
            } catch (NumberFormatException e) {
                System.err.println("Invalid num of reducer: " + args[4]);
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
        int successfulRuns = 0;                 // run successfully completed
        long totalTime = 0;                     // total time used to perform all the runs of the job
        int attempt = 0;                        // run's attempt
        long totalBytesRead = 0;                // Total number of bytes read by the job from input files
        long totalMapOutputBytes = 0;           // Size in bytes of all mapper output
        long totalSpilledRecords = 0;           // Number of records temporarily written to disk 8spilling phase in mapper)
        long totalCombineInputRecords = 0;      // Records read by a combiner (if used)
        long totalCombineOutputRecords = 0;     // Records output by a combiner (if used)
        long totalReduceInputGroups = 0;        // Number of unique keys received by the reducer
        long totalReduceInputRecords = 0;       // Total records sent to the reducer (across all keys)
        long totalReduceOutputRecords = 0;      // Records emitted by the reducer as final output
        long totalPhysicalMemory = 0;           // Total physical memory usage (RAM)
        long totalPeakMapPhysicalMemory = 0;    // Maximum physical memory used by any single map task
        long totalPeakReducePhysicalMemory = 0; // Maximum physical memory used by any reduce task

        // while to perform the severals runs of the application
        while (successfulRuns < numRunsRequested)
        {
            long startTime, endTime, duration;              // var to take the effective execution time

            final Configuration conf = new Configuration(); // create configuration object
            final Job job = Job.getInstance(conf, jobName + "_run_" + successfulRuns + "_comb_" + useCombiner + "_red_" + numReducer);
            job.setJarByClass(CoOccurrenceStripes.class);

            job.setMapOutputKeyClass(Text.class);           // set the typer for the output key for mapper
            job.setMapOutputValueClass(MapWritable.class);  // set the typer for the output value for mapper

            job.setOutputKeyClass(Text.class);              // set the typer for the output key for reducer
            job.setOutputValueClass(Text.class);            // set the typer for the output value for reducer

            job.setMapperClass(CoOccurrenceMapper.class);           // set mapper
            if (useCombiner)
                job.setCombinerClass(CoOccurrenceCombiner.class);   // set combiner -> See NOTE 1
            job.setReducerClass(CoOccurrenceReducer.class);         // set reducer
            job.setNumReduceTasks(numReducer);                      // to set the number of the reducer task

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
                long bytesRead = counters.findCounter("File Input Format Counters", "Bytes Read").getValue();
                long mapOutputBytes = counters.findCounter("Map-Reduce Framework", "Map output bytes").getValue();
                long combineInputRecords = counters.findCounter("Map-Reduce Framework", "Combine input records").getValue();
                long combineOutputRecords = counters.findCounter("Map-Reduce Framework", "Combine output records").getValue();
                long reduceInputGroups = counters.findCounter("Map-Reduce Framework", "Reduce input groups").getValue();
                long physicalMemory = counters.findCounter("Map-Reduce Framework", "Physical memory (bytes) snapshot").getValue();
                long peakMapPhysicalMemory = counters.findCounter("Map-Reduce Framework", "Peak Map Physical memory (bytes)").getValue();
                long peakReducePhysicalMemory = counters.findCounter("Map-Reduce Framework", "Peak Reduce Physical memory (bytes)").getValue();

                // update value to calculate the mean of the interesting fields
                totalBytesRead += bytesRead;
                totalMapOutputBytes += mapOutputBytes;
                totalSpilledRecords += spilledRecords;
                totalCombineInputRecords += combineInputRecords;
                totalCombineOutputRecords += combineOutputRecords;
                totalReduceInputGroups += reduceInputGroups;
                totalReduceInputRecords += reduceInputRecords;
                totalReduceOutputRecords += reduceOutputRecords;
                totalPhysicalMemory += physicalMemory;
                totalPeakMapPhysicalMemory += peakMapPhysicalMemory;
                totalPeakReducePhysicalMemory += peakReducePhysicalMemory;
                
                // write in a file txt -- see Note 0
                try (PrintWriter writer = new PrintWriter(new FileWriter(statsFileName, true))) {
                    writer.println("------------------------------------------");
                    writer.println("=== Job Statistics ===");
                    writer.println("Identifiers:");
                    writer.println("Date: " + getCurrentDateTime());
                    writer.println("Job Name: " + job.getJobName());
                    writer.println("Job ID: " + job.getJobID());
                    writer.println("Tracking URL: " + trackingUrl);
                    writer.println("Parameters:");
                    writer.println("Input Path: " + inputPath);
                    writer.println("Output Path: " + outputPath);
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

        // calculate the average values
        double averageTime = totalTime / (double) successfulRuns;       // calculate the average execution time
        long avgBytesRead = totalBytesRead / successfulRuns;
        long avgMapOutputBytes = totalMapOutputBytes / successfulRuns;
        long avgSpilledRecords = totalSpilledRecords / successfulRuns;
        long avgCombineInputRecords = totalCombineInputRecords / successfulRuns;
        long avgCombineOutputRecords = totalCombineOutputRecords / successfulRuns;
        long avgReduceInputGroups = totalReduceInputGroups / successfulRuns;
        long avgReduceInputRecords = totalReduceInputRecords / successfulRuns;
        long avgReduceOutputRecords = totalReduceOutputRecords / successfulRuns;
        long avgPhysicalMemory = totalPhysicalMemory / successfulRuns;
        long avgPeakMapPhysicalMemory = totalPeakMapPhysicalMemory / successfulRuns;
        long avgPeakReducePhysicalMemory = totalPeakReducePhysicalMemory / successfulRuns;

        // print average values in the cmd
        System.out.println("\n=== All " + successfulRuns + " jobs completed successfully ===");
        System.out.println("Average execution time: " + formatDuration((long)averageTime));
        System.out.println("Statistics written to: " + new File(statsFileName).getAbsolutePath());
        System.out.println("\n=== Averages over " + successfulRuns + " successful runs ===");
        System.out.println("-- Data --");
        System.out.println("Avg Bytes Read: " + avgBytesRead);
        System.out.println("Avg Map Output Bytes: " + avgMapOutputBytes);
        System.out.println("Avg Spilled Records: " + avgSpilledRecords);
        System.out.println("Avg Combine Input Records: " + avgCombineInputRecords);
        System.out.println("Avg Combine Output Records: " + avgCombineOutputRecords);
        System.out.println("Avg Reduce Input Groups: " + avgReduceInputGroups);
        System.out.println("Avg Reduce Input Records: " + avgReduceInputRecords);
        System.out.println("Avg Reduce Output Records: " + avgReduceOutputRecords);
        System.out.println("-- Memory --");
        System.out.println("Avg Physical Memory Snapshot: " + avgPhysicalMemory);
        System.out.println("Avg Peak Map Physical Memory: " + avgPeakMapPhysicalMemory);
        System.out.println("Avg Peak Reduce Physical Memory: " + avgPeakReducePhysicalMemory);

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
            writer.println("=== Averages over " + successfulRuns + " successful runs ===");
            writer.println("-- Data --");
            writer.println("Avg Bytes Read: " + avgBytesRead);
            writer.println("Avg Map Output Bytes: " + avgMapOutputBytes);
            writer.println("Avg Spilled Records: " + avgSpilledRecords);
            writer.println("Avg Combine Input Records: " + avgCombineInputRecords);
            writer.println("Avg Combine Output Records: " + avgCombineOutputRecords);
            writer.println("Avg Reduce Input Groups: " + avgReduceInputGroups);
            writer.println("Avg Reduce Input Records: " + avgReduceInputRecords);
            writer.println("Avg Reduce Output Records: " + avgReduceOutputRecords);
            writer.println("-- Memory --");
            writer.println("Avg Physical Memory Snapshot: " + avgPhysicalMemory);
            writer.println("Avg Peak Map Physical Memory: " + avgPeakMapPhysicalMemory);
            writer.println("Avg Peak Reduce Physical Memory: " + avgPeakReducePhysicalMemory);
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
NOTE 1:
    the combiner is normal combiner isn't in-mapper combiner. Hadoop can execute the combiner or not (is optional).
    If you don't want to set the combiner just comment out the whole line.
 */
