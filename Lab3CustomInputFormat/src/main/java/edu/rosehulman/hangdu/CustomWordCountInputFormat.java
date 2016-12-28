package edu.rosehulman.hangdu;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class CustomWordCountInputFormat
extends FileInputFormat<NullWritable, Text> {

@Override
protected boolean isSplitable(JobContext context, Path file) {
	return false;
}

@Override
public RecordReader<NullWritable, Text> createRecordReader(
  InputSplit split, TaskAttemptContext context) throws IOException,
  InterruptedException {
	CustomRecordReader reader = new CustomRecordReader();
	reader.initialize(split, context);
	return reader;
	}
}