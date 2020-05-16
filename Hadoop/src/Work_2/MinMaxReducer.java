package Work_2;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxReducer extends Reducer<Text, MinMaxWritable, Text, MinMaxWritable>{
	//自定义Reducer类
	private MinMaxWritable result = new MinMaxWritable();

	@Override
	protected void reduce(Text key, Iterable<MinMaxWritable> values,Context context)
			throws IOException, InterruptedException {
		result.setMax(0);
		result.setMin(0);
		//比较
		for (MinMaxWritable val : values){
			if (result.getMin() == 0 | val.getMin() < result.getMin()){
				result.setMin(val.getMin());
			}
			if (result.getMax() == 0 | val.getMax() > result.getMax()){
				result.setMax(val.getMax());
			}
		}
		context.write(key, result);
	}
	
}
