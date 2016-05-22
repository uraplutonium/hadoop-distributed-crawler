package extender;

import hdcrawler.HDCrawler;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fetcher.URLInfoWritable;

/**
 * URL外链扩展Mapper
 * @author Felix.Ting
 *
 * @see Mapper
 * @see URLInfoWritable
 */
public class ExtendMapper extends Mapper<Object, Text, Text, IntWritable> {

	/**
	 * 从URL信息文件中读取数据，建立URLInfoWritable对象
	 * @param urlInfo 一段完整的URLInfoWritable的数据
	 * @return 一个URLInfoWritable对象
	 */
	private URLInfoWritable readInfo(Text urlInfo) {
		URLInfoWritable info = new URLInfoWritable();
		StringTokenizer token = new StringTokenizer(urlInfo.toString());
		
		if(token.hasMoreElements()) {
			String url = token.nextToken();
			info.setURL(url);
			token.nextToken();
			int loclen = Integer.valueOf(token.nextToken());
			System.out.println("loclen:" + loclen);
			
			int i;
			for(i = 0 ; i < loclen ; i++) {		// 循环读入URL位置信息
				String keyword = token.nextToken();
				int wordlen = Integer.valueOf(token.nextToken());
				int j;
				for(j = 0 ; j < wordlen ; j++) {
					Integer wordloc = Integer.valueOf(token.nextToken());
					info.putURLLocation(keyword, wordloc);
				}
			}
		
			int extlen = Integer.valueOf(token.nextToken());
			for(i = 0 ; i < extlen ; i++) {		// 循环读入URL外链
				String exturl = token.nextToken();
				info.addExtendedURL(exturl);
			}
		}
		
		return info;
	}
	
	/**
	 * 对URL从格式上进行修饰、过滤
	 * @param url 需要修饰、过滤的URL
	 * @return 若URL合法，则返回修饰后的URL，否则返回null
	 */
	private String getLegalURL(String url) {
		if(url.matches("http://.*")) {		// 以“http://”为首的url，认为合法，直接返回
			return url;
		}
		else if(url.matches("www\\..*")) {	// 以“www.”为首的url，前端加入“http://”后返回
			return ("http://" + url);
		}
		else	// 其他url判定为非法，返回null
			return null;		
	}
	
	@Override
	public void map(Object num, Text urlInfo, Context context) {
		URLInfoWritable info = readInfo(urlInfo);	// 从URL信息文件读取数据
		int assessment = HDCrawler.evaluator.getRank(info, HDCrawler.keywords);	// 使用评估器，计算该网页与爬虫主题的相关度，作为该url外链相关度的估计值

		for(String url : info.getURLSet()) {
			String legalURL = getLegalURL(url);
			if(legalURL != null) {	// 将该URL外链中的合法URL作为键，该外链与主题的相关度的评估值作为值，输出
				try {
					context.write(new Text(legalURL), new IntWritable(assessment));
				}
				catch(InterruptedException exc) {
					exc.printStackTrace();
				}
				catch(IOException exc) {
					exc.printStackTrace();
				}
			}
		}
	}
	
}