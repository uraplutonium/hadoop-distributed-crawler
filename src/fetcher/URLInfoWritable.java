package fetcher;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * 此类存储某一url中的信息，包括网页文本的所有分词及其位置，以及该url的所有外链
 * @author Felix.Ting
 *
 * @see WritableComparable
 */
public class URLInfoWritable implements WritableComparable<URLInfoWritable> {
	
	private String url;	// 网页URL
	private Map<String, Set<Integer>> urlLocation;	// 网页中所有分词及其位置的Map
	private Set<String> extendedURL;	// 所有的外链
	
	/**
	 * 构造函数
	 */
	public URLInfoWritable() {
		url = new String();
		urlLocation = new HashMap<String, Set<Integer>>();
		extendedURL = new TreeSet<String>();
	}
	
	/**
	 * 构造函数
	 * @param info 所要复制的URLInfoWritable
	 */
	public URLInfoWritable(URLInfoWritable info) {
		url = new String(info.url);
		for(String keyword : info.urlLocation.keySet()) {
			urlLocation.put(keyword, new TreeSet<Integer>(info.urlLocation.get(keyword)));
		}
	}
	
	public void setURL(String url) {
		this.url = url;
	}
	
	/**
	 * 向urlLocation中加入关键词-分词位置对
	 * @param keyword 关键词/分词
	 * @param wordLocation 分词位置
	 */
	public void putURLLocation(String keyword, Integer wordLocation) {
		if(urlLocation.containsKey(keyword)) {
			Set<Integer> bufLocation = new TreeSet<Integer>(urlLocation.get(keyword));
			bufLocation.add(wordLocation);
			urlLocation.put(keyword, bufLocation);
		}
		else {
			Set<Integer> bufLocation = new TreeSet<Integer>();
			bufLocation.add(wordLocation);
			urlLocation.put(keyword, bufLocation);
		}
	}
	
	/**
	 * 向extendedURL中添加外链
	 * @param url
	 */
	public void addExtendedURL(String url) {
		extendedURL.add(url);
	}

	public String getURL() {
		return url;
	}
	
	public Map<String, Set<Integer>> getLocationMap() {
		return urlLocation;
	}
	
	public Set<String> getURLSet() {
		return extendedURL;
	}
	
	@Override
	public int hashCode() {
		return url.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof URLInfoWritable))
			return false;
		URLInfoWritable thatInfo = (URLInfoWritable)obj;
		return (url == thatInfo.url);
	}
	
	@Override
	public String toString() {
		String str;
		str = String.valueOf(url) + '\t' + "-1" +'\t' + urlLocation.size() + '\t';
		
		for(String keyword : urlLocation.keySet()) {
			str += (keyword + '\t' + urlLocation.get(keyword).size() + '\t');
			for(Integer wordLocation : urlLocation.get(keyword)) {
				str += (String.valueOf(wordLocation) + '\t');
			}
			str += '\t';
		}
		
		str += extendedURL.size();
		for(String url : extendedURL) {
			str += ('\t' + url);
		}
		
		return str;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		urlLocation.clear();
		extendedURL.clear();
	
		url = Text.readString(in);
		int loclen = in.readInt();
		
		int i;
		for(i = 0 ; i < loclen ; i++) {
			String keyword = Text.readString(in);
			Set<Integer> bufLocation = new TreeSet<Integer>();
			int wordlen = in.readInt();
			int j;
			for(j = 0 ; j < wordlen ; j++) {
				Integer wordloc = in.readInt();
				bufLocation.add(wordloc);
			}
			urlLocation.put(keyword, bufLocation);
		}
	
		int extlen = in.readInt();
		for(i = 0 ; i < extlen ; i++) {
			String url = Text.readString(in);
			extendedURL.add(url);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, url);
		out.writeInt(urlLocation.size());
		for(String keyword : urlLocation.keySet()) {
			Text.writeString(out, keyword);
			out.writeInt(urlLocation.get(keyword).size());
			for(Integer wordLocation : urlLocation.get(keyword)) {
				out.writeInt(wordLocation);
			}
		}
		
		out.writeInt(extendedURL.size());
		for(String url : extendedURL) {
			Text.writeString(out, url);
		}
	}

	@Override
	public int compareTo(URLInfoWritable thatInfo) {
		return url.compareTo(thatInfo.url);
	}

}