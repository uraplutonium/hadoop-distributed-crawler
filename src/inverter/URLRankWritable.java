package inverter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * URL及其相关度类型，存储某一关键字所出现的所有URL及对应的相关度
 * @author Felix.Ting
 *
 * @see HashMap
 * @see WritableComparable
 */
public class URLRankWritable extends HashMap<String, Integer> implements WritableComparable<URLRankWritable> {
	
	private static final long serialVersionUID = 1L;
	
	@Override
	public String toString() {
		String str = new String();
		str += this.size();
		str += '\t';
		for(String url : this.keySet()) {
			str += (url + '\t' + this.get(url) + '\t');
		}
		return str;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.clear();
		int size = in.readInt();
		int i;
		for(i = 0 ; i < size ; i++) {
			String url = Text.readString(in);
			Integer rank = new Integer(in.readInt());
			this.put(url, rank);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.size());
		for(String url : this.keySet()) {
			Text.writeString(out, url);
			out.writeInt(this.get(url));
		}
	}

	@Override
	public int compareTo(URLRankWritable thatURLRank) {
		return (this.hashCode() < thatURLRank.hashCode() ? -1 : (this.hashCode() > thatURLRank.hashCode() ? 1 : 0));
	}

	
}