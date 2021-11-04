import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task1 {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
    
    private Text filmname = new Text();
    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {
      System.out.println("Value: "+ value );
      String[] tokens = value.toString().split(",");
      filmname.set(tokens[0]);

      //find the highest rating
      int maxrating = 1;
      for (int i = 1; i < tokens.length; i++) {
        if(tokens[i].equals("")) {
          continue;
        }
        int rating = Integer.parseInt(tokens[i]);
        if(rating == 5 ) {
          maxrating = 5;
          break;
        } else if(rating > maxrating) {
          maxrating = rating;
        }
      }
      StringBuffer result = new StringBuffer();
      for (int i = 1; i < tokens.length; i++) {
        if(tokens[i].equals(String.valueOf(maxrating))) {
            result.append("," + i);
        }
      }
      result.deleteCharAt(0);
      Text re = new Text(result.toString());
      context.write(filmname,re);
    }
  }

  public static class IntSumReducer 
      extends Reducer<Text,IntWritable,Text,Text> {

    public void reduce(Text key, Text values, 
                      Context context
                      ) throws IOException, InterruptedException {
      
      context.write(key, values);
    }
  }
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    // add code here

    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }

        // one of the following two lines is often needed
    // to avoid ClassNotFoundException
    job.setJarByClass(Task1.class);
    //job.setJar("HadoopWC.jar");

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    //job.setNumReduceTasks(0);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class IntArrayWritable extends ArrayWritable {
    public IntArrayWritable() {
      super(IntWritable.class);
    }
    public IntArrayWritable(IntWritable[] values) {
      super(IntWritable.class, values) ;
    }
  }
}
