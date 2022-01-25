package card;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountMapper extends Mapper<LongWritable, Text, Text,IntWritable>{
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey=new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		double amount=0;
		String type="";
		String[] columns=value.toString().split(",");
		if(Long.parseLong(key.toString()) > 1) {
			amount = Double.parseDouble(columns[29]);
			type=columns[30];
		}
		outputKey.set(type);
		if (amount > 0) {
			context.write(outputKey, outputValue);
		}
	}
		
}
