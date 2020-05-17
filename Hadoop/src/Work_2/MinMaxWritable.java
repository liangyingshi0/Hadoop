package Work_2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MinMaxWritable implements Writable{
	private int min;//记录最小值
	private int max;//记录最大值

	public int getMin() {
		return min;
	}

	public void setMin(int min) {
		this.min = min;
	}

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		max = in.readInt();
		min = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(max);
		out.writeInt(min);
	}

	@Override
	public String toString() {
		return max + " " + min;
	}

}
