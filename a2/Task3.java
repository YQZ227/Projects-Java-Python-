import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {

  // add code here
  public static class ratingPerUserMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final static IntWritable count = new IntWritable(1);
    private IntWritable user = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",", -1);

      for (int i = 1; i < tokens.length; ++i) {
        if (!tokens[i].equals("")) {
          user.set(i);
          context.write(user, count);
        } else {
          continue;
        }           
      }
    }
  }

  public static class ratingPerUserReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable reduced = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable v : values) {
        count += v.get();
      }
      reduced.set(count);
      context.write(key, reduced);
    }
  }

    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task3");
    job.setJarByClass(Task3.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapperClass(ratingPerUserMapper.class);
    job.setCombinerClass(ratingPerUserReducer.class);
    job.setReducerClass(ratingPerUserReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(1);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
