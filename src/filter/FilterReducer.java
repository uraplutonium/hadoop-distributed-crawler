package filter;

import hdcrawler.HDCrawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * URL外链过滤Reducer
 * @author Felix.Ting
 *
 * @see Reducer
 */
public class FilterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	/**
	 * 打开hdfs上的已扫描URL文件，建立已扫描URL集合
	 * @return 已扫描URL集合
	 */
	private Set<String> openScannedURL() {
		Set<String> scannedURL = new TreeSet<String>();
		Configuration conf = new Configuration();
		try {
			Path urlPath = new Path(HDCrawler.workspace + "scannedURL");
			FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), conf);
			
			FSDataInputStream instream = hdfs.open(urlPath);
			BufferedReader bufferreader = new BufferedReader(new InputStreamReader(instream));
			while(bufferreader.ready()) {
				String line = bufferreader.readLine();
				scannedURL.add(line);
			}
			bufferreader.close();
			instream.close();			
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
		
		return scannedURL;
	}
	
	@Override
	public void reduce(Text filteredURL, Iterable<IntWritable> assessments, Context context) {
		boolean repeated = false;
		IntWritable assessment = new IntWritable();
		for(IntWritable asm : assessments) {	// 检查该URL的所有相关度估计值，若有一项为-1，说明已经存在于重复URL列表中
			if(asm.get() == -1) {
				repeated = true;
				break;
			}
			else
				assessment = asm;
		}
		
		if(!repeated) {	// 若不在重复URL列表中，则进行重复性检查
			Set<String> scannedURL = openScannedURL();
			String url = filteredURL.toString();
			for(String surl : scannedURL) {
				if(HDCrawler.eliminator.isRepeated(surl, url)) {	// 使用消重器检查该URL与已扫描URL列表中所有条目的重复性
					repeated = true;
					break;
				}
			}
		}
		
		try {
			// 若该URL不重复，则以相关度估计值作为值输出，若重复，则以-1作为值输出
			context.write(filteredURL, (repeated ? new IntWritable(-1) : assessment));
		}
		catch(InterruptedException exc) {
			exc.printStackTrace();
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
	
}