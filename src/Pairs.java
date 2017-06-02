import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Pairs {

	public static class MapClass extends Mapper<Object, Text, Text, IntWritable>{
		private String line;
		private String[] items;
		private String spName;
		private String userId;
		private static  final IntWritable one = new IntWritable(1);
		private Text outkey =new Text(), outvalue = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			line = value.toString();
			items = line.split("\\s");
			
			if(items[0].equals("time")){
				return ;
			}
			userId = items[2];
			spName = items[5];
			
			outkey.set(userId + " " + spName);
			
			context.write(outkey, one);
		}
	}
	
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{
		Map<Text, Integer> map = new HashMap<Text, Integer>();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for(IntWritable t : values){
				sum = sum + t.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args){
		Configuration conf = new Configuration();
		
		try {
			
			String[] paths=new GenericOptionsParser(conf,args).getRemainingArgs();

		    if (paths.length != 2) {
		        System.err.println("Usage: wordcount <in> <out>");
		        System.exit(2);
		      }
		      File outputDir = new File(paths[1]);
		      if(outputDir.exists()){
		    	  String[] files = outputDir.list();
		    	  for(String f : files){
		    		  File newfile = new File(outputDir+"/"+f);
		    		  newfile.delete();
		    	  }
		      	outputDir.delete();
		      	System.out.println("output path exists , now we deleted it");
		      }
		      
		      Job job = Job.getInstance(conf, "Pairs");
		      
		      job.setJarByClass(Pairs.class);
		      
		      job.setMapperClass(MapClass.class);
		      job.setMapOutputKeyClass(Text.class);
		      job.setMapOutputValueClass(IntWritable.class);
		      
		      job.setReducerClass(ReduceClass.class);
		      
		      job.setOutputKeyClass(Text.class);
		      job.setOutputValueClass(IntWritable.class);
		      
	          FileInputFormat.addInputPath(job, new Path(paths[0]));  
	          FileOutputFormat.setOutputPath(job, new Path(paths[1]));  
	          System.exit(job.waitForCompletion(true) ? 0:1);
		      
			
		} catch (IOException e) {
			e.printStackTrace();
		}catch (ClassNotFoundException e) {  
            e.printStackTrace();  
        } catch (InterruptedException e) {  
            e.printStackTrace();  
        } 
	}
}
