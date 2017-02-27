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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AccessCount {
 
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    public void map(Object key, Text value, Context context) throws IOException, 
    InterruptedException {
    	Text record = new Text();
    	Text traffic = new Text();
    	
    	String line = value.toString();
    	String[] items = line.split("\\s");
    	if(items[0].equals("time")){
    		return;
    	}
    	String userId = items[2];
    	String spName = items[5];
    	
    	String upTraffic = items[6];
    	String downTraffic = items[7];
    	
    	record.set(userId + " " + spName);
    	traffic.set(upTraffic + " " + downTraffic);
    	context.write(record, traffic);

    }
  }

  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
    InterruptedException {
      int downTraffic = 0;
      int upTraffic = 0;
      int count = 0;
      Text outvalue = new Text();
      String items[], cnt, dt, ut;
      for (Text t : values) {
        items = t.toString().split("\\s");
        downTraffic += Integer.parseInt(items[0]);
        upTraffic += Integer.parseInt(items[1]);
        count++;
      }
      cnt = String.valueOf(count);
      dt = String.valueOf(downTraffic);
      ut = String.valueOf(upTraffic);
      outvalue.set(cnt + " " + dt + " " + ut);
      context.write(key, outvalue);
    }
  }

  public static void main(String[] args) throws Exception { 

    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: access count <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "Access Count");
    job.setJarByClass(AccessCount.class);
    job.setMapperClass(TokenizerMapper.class);  
    job.setReducerClass(IntSumReducer.class); 

    job.setMapOutputKeyClass(Text.class);  
    job.setMapOutputValueClass(Text.class); 

    job.setOutputKeyClass(Text.class); 
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
//    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 

    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}