package fr.eurecom.dsg.mapreduce.OrderInversion;

import fr.eurecom.dsg.mapreduce.Pairs.Pair;
import fr.eurecom.dsg.mapreduce.Pairs.TextPair;
import org.apache.commons.collections.IterableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class OrderInversion extends Configured implements Tool {

    private final static String ASTERISK = "\0";

    public static class PartitionerTextPair extends
            Partitioner<TextPair, IntWritable> {
        @Override
        public int getPartition(TextPair key, IntWritable value,
                                int numPartitions) {
            // TODO: implement getPartition such that pairs with the same first element
            //       will go to the same reducer. You can use toUnsighed as utility.
            return toUnsigned(key.getFirst().hashCode()) % numPartitions;
        }

        /**
         * toUnsigned(10) = 10
         * toUnsigned(-1) = 2147483647
         *
         * @param val Value to convert
         * @return the unsigned number with the same bits of val
         * */
        public static int toUnsigned(int val) {
            return val & Integer.MAX_VALUE;
        }
    }

    public static class PairMapper extends
            Mapper<LongWritable, Text, TextPair, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            // TODO: implement the map method

            String[] words = value.toString().split(" ");

            for(int i = 0; i < words.length; i++) {

                IntWritable count = new IntWritable(0);
                if(words[i].length() > 0) {

                    for (int j = 0; j < words.length; j++) {

                        if(!words[i].equals(words[j]) && words[j].length() > 0) {
                            context.write(new TextPair(words[i], words[j]), new IntWritable(1));
                            count.set(count.get() + 1);
                        }
                    }
                }
                context.write(new TextPair(words[i], ASTERISK), count);
            }
        }
    }

    public static class PairReducer extends
            Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {


        long count = 0L;
        DoubleWritable avg = new DoubleWritable();

        @Override
        public void reduce(TextPair pair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // Get the count for each word
            long result = 0L;
            for(IntWritable value : values) {
                result += (long) value.get();
            }

            // The pairs are storted and the pair with asterisk should be first
            if(pair.getSecond().toString().equals(new Text(ASTERISK))) {
                count = result;
            } else {
                avg.set(result / count);
                context.write(pair, avg);
            }

        }

    }

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = new Job(conf, "group26-OrderInversion");  // TODO: define new job instead of null using conf e setting a name

        // TODO: set job input format

        job.setInputFormatClass(TextInputFormat.class);

        // TODO: set map class and the map output key and value classes

        job.setMapperClass(OrderInversion.PairMapper.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        // TODO: set reduce class and the reduce output key and value classes

        job.setReducerClass(OrderInversion.PairReducer.class);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(DoubleWritable.class);

        // TODO: set job output format

        job.setOutputFormatClass(TextOutputFormat.class);

        // TODO: add the input file as job input (from HDFS) to the variable inputFile

        FileInputFormat.addInputPath(job, this.inputPath);

        // TODO: set the output path for the job results (to HDFS) to the variable outputPath

        FileOutputFormat.setOutputPath(job, this.outputDir);

        // TODO: set the number of reducers using variable numberReducers

        job.setNumReduceTasks(this.numReducers);

        // TODO: set the jar class

        job.setJarByClass(OrderInversion.class);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    OrderInversion(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: OrderInversion <num_reducers> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrderInversion(args), args);
        System.exit(res);
    }
}
