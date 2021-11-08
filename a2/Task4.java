import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.*;  

public class Task4 {

  public static class MovieSimilarityMapper extends Mapper<Object, Text, Text, NullWritable> {
    private HashMap<String, Byte[]> ratingMap = new HashMap<String, Byte[]>();
    private Text output = new Text();

    public void setup(Context context) throws IOException, InterruptedException {
      Path cachePath = context.getLocalCacheFiles()[0];
      BufferedReader br = new BufferedReader(new FileReader(cachePath.toString())); 
      String l;

      while ((l = br.readLine()) != null){
        String[] tokens = l.split(",", -1);
        String title = tokens[0];
        Byte[] ratings = new Byte[tokens.length - 1];

        for (int i = 0; i < ratings.length; ++i) {
          if(tokens[i + 1].equals("")) {
            ratings[i] = 0;
          } else {
            ratings[i] = Byte.parseByte(tokens[i + 1]);
          }
        }
        ratingMap.put(title, ratings);
      }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String first_movie = value.toString().split(",", 2)[0];
      Byte[] first_ratings = ratingMap.get(first_movie);
      for (String second_movie : ratingMap.keySet()) {
        if (second_movie.compareTo(first_movie)> 0) {
          Byte[] second_ratings = ratingMap.get(second_movie);
          int count = 0;
          for (int i = 0; i < Math.min(first_ratings.length,second_ratings.length); ++i) {
            if (first_ratings[i] != 0 && first_ratings[i] == second_ratings[i]) {
              count++;
            }
          }
          output.set(first_movie + "," + second_movie + "," + count);
          context.write(output, NullWritable.get());
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    Job job = new Job(conf, "Task IV: similarity between movies");
    
    job.setJarByClass(Task4.class);
    job.setMapperClass(MovieSimilarityMapper.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    // distributed cache
    job.addCacheFile(new URI(otherArgs[0]));
    job.setNumReduceTasks(0);

    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}




