package card;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumMapper extends Mapper<LongWritable, Text, Text,DoubleWritable>{
	private DoubleWritable outputValue=new DoubleWritable(0);
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
			outputValue=new DoubleWritable(amount);
			context.write(outputKey, outputValue);
		}
	}
		
}
