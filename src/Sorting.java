import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//import com.sun.xml.bind.v2.schemagen.xmlschema.List;

public class Sorting {
	public static class MapClass 
			extends Mapper<Object, Text, Text, Text>{
//		private LongWritable sum = new LongWritable();
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{
			String line = value.toString();
			String[] items = line.split("\\s");
			if(items[0].equals("time")){
				return;
			}
			String userId = items[2];
			long up = Long.valueOf(items[6]);
			long down = Long.valueOf(items[7]);
			long sum1 = up + down;
			
			line = line + " " + Long.valueOf(sum1);
			Text user = new Text(userId);
			
			context.write(user, new Text(line));
			
			
		}
		
	}
	
	public static class ReduceClass extends Reducer<Text , Text , NullWritable, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String userId = key.toString();
			String line = "";
			Map<String, Long> map = new HashMap<String, Long>();
			for(Text val : values){
				line = val.toString();
				String[] items = line.split("\\s");
				long sum = Long.valueOf(items[8]);
				
				map.put(line, sum);
			}
					
			List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(map.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<String, Long>>(){
				public int compare(Entry<String, Long> e1, Entry<String, Long> e2){
					return (int) (e1.getValue() - e2.getValue()); 
				}
			});
			
			
			for (Map.Entry<String, Long> entry : list){  
				Text value = new Text(entry.getKey());
	            context.write( NullWritable.get(),  value );  
	        }
				
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
			      	outputDir.delete();
			      	System.out.println("output path exists , now we deleted it");
			      }
			      
			      Job job = Job.getInstance(conf, "sorting");
			      
			      job.setJarByClass(Sorting.class);
			      
			      job.setMapperClass(MapClass.class);
			      job.setMapOutputKeyClass(Text.class);
			      job.setMapOutputValueClass(Text.class);
			      
			      job.setReducerClass(ReduceClass.class);
			      
			      job.setOutputKeyClass(Text.class);
			      job.setOutputValueClass(Text.class);
			      
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