package edu.rosehulman.hangdu;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.Text;


class CustomRecordReader extends RecordReader<NullWritable, Text> {	  
	  private FileSplit fileSplit;
	  private Configuration conf;
	  private Text value = new Text();
	  private boolean processed = false;

	  @Override
	  public void initialize(InputSplit split, TaskAttemptContext context)
	      throws IOException, InterruptedException {
	    this.fileSplit = (FileSplit) split;
	    this.conf = context.getConfiguration();
	  }
	  
	  @Override
	  public boolean nextKeyValue() throws IOException, InterruptedException {
	    if (!processed) {
	      Path file = fileSplit.getPath();
	      FileSystem fs = file.getFileSystem(conf);
	      BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(file)));
	      StringBuilder sb = new StringBuilder();
	      try {
	    	   String line = br.readLine();
		        while (line != null){
		        	 sb.append(line);
				     line = br.readLine();
		        }
		        String everything = sb.toString();
		        value = new Text(everything);
	      } finally {
	        br.close();
	      }
	      processed = true;
	      return true;
	    }
	    return false;
	  }
	    
	  
	  @Override
	  public NullWritable getCurrentKey() throws IOException, InterruptedException {
	    return NullWritable.get();
	  }

	  @Override
	  public Text getCurrentValue() throws IOException,
	      InterruptedException {
	    return value;
	  }

	  @Override
	  public float getProgress() throws IOException {
	    return processed ? 1.0f : 0.0f;
	  }

	  @Override
	  public void close() throws IOException {
	    // do nothing
	  }
	}
