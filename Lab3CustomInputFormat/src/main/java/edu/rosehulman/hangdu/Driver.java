package edu.rosehulman.hangdu;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured
  implements Tool {
//	static String searchword = "Mary";
	public static String search = "SEARCHWORD";

	static class SearchMapper
	    extends Mapper<NullWritable, Text, Text, IntWritable> {
	  
	  private Text filenameKey;
	  private Text filename;
	  private String searchword;
	  
	  @Override
	  protected void setup(Context context) throws IOException,
	      InterruptedException {
	    InputSplit split = context.getInputSplit();
	    Path path = ((FileSplit) split).getPath();
	    String path_str = path.toString();
	    filenameKey = new Text(path.toString());
	       
	    String[] strs = path_str.split(":");
		String temp = strs[strs.length-1];
		int pos = path_str.indexOf("/");
		String test = temp.substring(pos-1);
		filename = new Text(test);
	    
	    Configuration conf = context.getConfiguration();
	    searchword = conf.get(search);
	  }
	  
	
	  @Override
	  protected void map(NullWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
		  String filecontent = value.toString();
		  int count = 0;
		  int len = searchword.length();
		  int pos = 0;
		  while((pos = filecontent.indexOf(searchword, pos))!=-1){
			  pos = pos+len;
			  count++;
		  }
		  context.write(filename, new IntWritable(count));
		  
	  }	  
	}

	public int run(String[] args) throws Exception {  
		Configuration conf = getConf();
		conf.set(search, args[2]);
		Job job = Job.getInstance(conf, "Custom Input Format");
		job.setJarByClass(Driver.class);
		
		CustomWordCountInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  
	    job.setInputFormatClass(CustomWordCountInputFormat.class);
	//  job.setOutputFormatClass(SequenceFileOutputFormat.class);
	//  SequenceFileOutputFormat.setCompressOutput(job, true);
     	job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	
	    job.setMapperClass(SearchMapper.class);
	    
	
	    return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new Driver(), args);
	    System.exit(exitCode);
	}
}