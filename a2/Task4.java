import java.io.IOException;
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
import java.util.HashMap;
import java.net.URI;
import java.io.FileReader;
import java.io.BufferedReader;


public class Task4 {

  // add code here
  public static class sameMovieRating extends Mapper<Object, Text, Text, NullWritable> {
    private HashMap<String, Byte[]> ratingMap = new HashMap<String, Byte[]>();
    private Text movie_name = new Text();

    @Override
        protected void setup(Context context) throws IOException,InterruptedException {
          Path[] filelist = DistributedCache.getLocalCacheFiles(context.getConfiguration());
          for(Path findlist:filelist) {
              if(findlist.getName().toString().trim().equals("mapmainfile.dat")) {
                fetchvalue(findlist,context);
              }
          }     
        }
        public void fetchvalue(Path realfile,Context context) throws NumberFormatException, IOException {
          BufferedReader buff = new BufferedReader(new FileReader(realfile.toString()));
            //some operations with the file
          String l;
          while((l = buff.readLine()) != null) {
            String[] tokens = line.split(",", -1);
            Byte[] ratings = new Byte[tokens.length - 1];
              
            for (int i = 0; i < ratings.length; ++i) {
              if(tokens[i + 1].equals("")) {
                ratings[i] = 0;
              } else {
                ratings[i] = Byte.parseByte(tokens[i + 1]);
              }
            }
            ratingMap.put(movie_name,ratings);
          }
        }
    }   
    public void map(Object key, Text value, Context contexT) throws IOException, InterruptedException {
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
          context.write(output,NullWritable.get());
        }
      }
    }
  

    
    
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    
    Job job = Job.getInstance(conf, "Task4");
    job.setJarByClass(Task4.class);

    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // add code here
    job.setMapperClass(sameMovieRating.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    // Set up distributed cache
    DistributedCache.addCacheFile(new URI("/user/hduser/test/mapmainfile.dat"),conf);

    //job.addCacheFile(new URI(otherArgs[0]));
    // This is a mapper-only hadoop job
    job.setNumReduceTasks(0);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
