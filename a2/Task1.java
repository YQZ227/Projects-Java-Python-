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

public class Task1 {

  // add code here
  public static class SplitRating extends Mapper <Object, Text, Text, Text> {
    private Text name = new Text();
    private Text rating = new Text();

    public void map (Object key, org.w3c.dom.Text value, Context context) throws IOException, InterruptedException {
      String[] token = value.toString().split(",");
      name.set(token[0]);
      StringBuilder sb = new StringBuilder();
      int max = Integer.MIN_VALUE;
      for (int i = 0; i < token.length; ++i) {
        String t = token[i];
        if (token[i].equals("")){
          continue;
        }
        int rating = Integer.parseInt(token[i]);      
        if (rating > max) {
          sb = new StringBuilder();
          sb.append(String.valueOf(i));
          max = rating;
        }
        if (rating == max) {
          sb.append("," + String.valueOf(i));
        }
        rating.set(sb.toString());
        context.write(name,rating);
      }

    }

    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task1");
    job.setJarByClass(Task1.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    
    // add code here

    job.setMapperClass(Task1.SplitRating.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
}
