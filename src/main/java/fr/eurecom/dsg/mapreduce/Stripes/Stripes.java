package fr.eurecom.dsg.mapreduce.Stripes;

import java.io.IOException;

import fr.eurecom.dsg.mapreduce.Pairs.Pair;
import fr.eurecom.dsg.mapreduce.Pairs.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Stripes extends Configured implements Tool {

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = this.getConf();
    Job job = new Job(conf, "group26-Stripes");  // TODO: define new job instead of null using conf e setting a name

    // TODO: set job input format

    job.setInputFormatClass(TextInputFormat.class);

    // TODO: set map class and the map output key and value classes

    job.setMapperClass(StripesMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    // TODO: set reduce class and the reduce output key and value classes

    job.setReducerClass(StripesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    // TODO: set job output format

    job.setOutputFormatClass(TextOutputFormat.class);

    // TODO: add the input file as job input (from HDFS) to the variable inputFile

    FileInputFormat.addInputPath(job, this.inputPath);

    // TODO: set the output path for the job results (to HDFS) to the variable outputPath

    FileOutputFormat.setOutputPath(job, this.outputDir);

    // TODO: set the number of reducers using variable numberReducers

    job.setNumReduceTasks(this.numReducers);

    // TODO: set the jar class

    job.setJarByClass(Stripes.class);


    return job.waitForCompletion(true) ? 0 : 1;
  }

  public Stripes (String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: Stripes <num_reducers> <input_path> <output_path>");
      System.exit(0);
    }
    this.numReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
    System.exit(res);
  }
}

class StripesMapper
        extends Mapper<LongWritable,   // TODO: change Object to input key type
        Text,   // TODO: change Object to input value type
        Text,   // TODO: change Object to output key type
        StringToIntMapWritable> { // TODO: change Object to output value type

  @Override
  public void map(LongWritable key, // TODO: change Object to input key type
                  Text value, // TODO: change Object to input value type
                  Context context)
          throws java.io.IOException, InterruptedException {

    // TODO: implement map method
  }
}

class StripesReducer
        extends Reducer<Text,   // TODO: change Object to input key type
        StringToIntMapWritable,   // TODO: change Object to input value type
        Text,   // TODO: change Object to output key type
        StringToIntMapWritable> { // TODO: change Object to output value type
  @Override
  public void reduce(Text key, // TODO: change Object to input key type
                     Iterable<StringToIntMapWritable> values, // TODO: change Object to input value type
                     Context context) throws IOException, InterruptedException {

    // TODO: implement the reduce method
  }
}