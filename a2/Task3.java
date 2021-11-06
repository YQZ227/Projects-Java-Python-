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
  public static class ratingPerUserMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable count = new IntWritable(1);
    private Text user = new Text();

    public void map(Object key, org.w3c.dom.Text value, Context context) throws IOException, InterruptedException {
      String[] tokens = value.toString().split(",", -1);

      for (int i = 1; i < tokens.length; ++i) {
        String token = tokens[i];
        if (!token.equals("")) {
          count.set(Integer.valueOf(tokens[i]));
          user.set(String.valueOf(i));
          context.write(user, count);
        }            
      }
    }
  }

  public static class ratingPerUserReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable reduced = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      for (IntWritable v : values) {
        count += v.get();
      }
      reduced.set(count);
      context.write(key, result);
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
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
