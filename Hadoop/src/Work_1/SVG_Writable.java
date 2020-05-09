package Work_1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

//自定义Writable
public class SVG_Writable implements Writable{

	private int count = 0;		//数据个数
	private int average = 0;	//数据平均值
	
	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getAverage() {
		return average;
	}

	public void setAverage(int average) {
		this.average = average;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		count = in.readInt();
		average = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(count);
		out.writeInt(average);
	}

	//因为最终输出不涉及该Writable输出，注释掉照样得出相同结果
	/*@Override
	public String toString() {
		return count + "\t" + average;
	}
	*/
}
