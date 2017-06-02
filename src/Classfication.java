import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Time;

public class Classfication {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
 
    private Text word = new Text();

    private String line = "";


    public void map(Object key, Text value, Context context) throws IOException, 
    InterruptedException {

      String[] items = value.toString().split("\\s");
      if(items[0].equals("time")){
    	  return;
      }
      
      String spname = items[5].replaceAll("[:.]", "_");
      word.set(spname);
      context.write(word,value);
    }

  }

  public static class IntSumReducer extends Reducer<Text,Text,NullWritable,Text> {
    private MultipleOutputs<NullWritable, Text> output;
    //private String line = "";类作用域
    @Override
    protected void setup(Context context
    ) throws IOException, InterruptedException {
        output = new MultipleOutputs<NullWritable, Text>(context);
        super.setup(context);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
    InterruptedException {
      String line = "";//函数作用域,出错原因

      for (Text val : values) {
        line = line + val.toString() + "\r\n";
      }
      String spname = key.toString();
      output.write(NullWritable.get(),new Text(line),spname);
      
    }
    @Override
    protected void cleanup(Context context
    		) throws IOException, InterruptedException {
    	output.close();
    	super.cleanup(context);
    }
  }

  public static void main(String[] args) throws Exception {


    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    //这里需要配置参数即输入和输出的HDFS的文件路径
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    File outputDir = new File(otherArgs[1]);
    if(outputDir.exists()){
  	  String[] files = outputDir.list();
  	  for(String f : files){
  		  File newfile = new File(outputDir+"/"+f);
  		  newfile.delete();
  	  }
    	outputDir.delete();
    	System.out.println("output path exists , now we deleted it");
    }
    
   // JobConf conf1 = new JobConf(WordCount.class);
    Job job = Job.getInstance(conf, "Classfication");//Job(Configuration conf, String jobName) 设置job名称和
    job.setJarByClass(Classfication.class);
    job.setMapperClass(TokenizerMapper.class); //为job设置Mapper类 
    job.setCombinerClass(IntSumReducer.class); //为job设置Combiner类  
    job.setReducerClass(IntSumReducer.class); //为job设置Reduce类 

    job.setMapOutputKeyClass(Text.class);  
    job.setMapOutputValueClass(Text.class); 

    job.setOutputKeyClass(NullWritable.class);        //设置输出key的类型
    job.setOutputValueClass(Text.class);//  设置输出value的类型

//    job.setOutputFormatClass(FileOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0])); //为map-reduce任务设置InputFormat实现类   设置输入路径

    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//为map-reduce任务设置OutputFormat实现类  设置输出路径
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
