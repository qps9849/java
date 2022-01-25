package sort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class StringSort {
	public static void main(String[] args)throws Exception{
//		Configuration conf = new Configuration();
//		Job job = Job.getInstance(conf, "StringSort");
//		job.setJarByClass(StringSort.class);
//		job.setMapperClass(Mapper.class);
//		job.setReducerClass(Reducer.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);
//		job.setNumReduceTasks(1);
//		job.setInputFormatClass(KeyValueTextInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
//		
//		FileInputFormat.addInputPath(job, new Path(args[1]));
//		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
//		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
//		job.waitForCompletion(true);
		
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"StringSort");
		job.setJarByClass(StringSort.class);
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class); 
		job.setNumReduceTasks(1);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		SequenceFileOutputFormat.setOutputCompressionType(
				job, SequenceFile.CompressionType.BLOCK);
		job.waitForCompletion(true);		
	}
}
