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
     * main function of the wordcount application.
     *
     * @param args
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        final Configuration conf = new Configuration();     // create configuration object
        final Job job = new Job(conf, "wordcount");
        job.setJarByClass(WordCount.class);

        job.setOutputKeyClass(Text.class);              // set the typer for the output key for reducer
        job.setOutputValueClass(IntWritable.class);     // set the typer for the output value for reducer

        job.setMapperClass(WordCountMapper.class);      // set mapper
        job.setReducerClass(WordCountReducer.class);    // set reducer

        FileInputFormat.addInputPath(job, new Path(args[0]));       // first argument is the input folder
        FileOutputFormat.setOutputPath(job, new Path(args[1]));     // second argument is the output folder

        System.exit(job.waitForCompletion(true) ? 0 : 1);   // wait the end of the job before to exit
    }
}
