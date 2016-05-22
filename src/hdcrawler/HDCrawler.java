package hdcrawler;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import inverter.Inverter;
import selector.SelectAll;
import selector.SelectTopTen;
import selector.Selector;
import eliminator.Eliminator;
import eliminator.SimpleURLEliminator;
import evaluator.CountEvaluator;
import evaluator.Evaluator;
import extender.Extender;
import fetcher.Fetcher;
import filter.Filter;

/**
 * Hadoop Distributed Crawler
 * <p>基于的Hadoop分布式爬虫
 * <p>版权所有(C) 2011 丁飞
 * <p>本程序为自由软件;您可依据自由软件基金会所发表的 GNU 通用公共授权条款,对本程序再次发布和/或修改;无论您依据的是本授权的第三版,或(您可选的)任一日后发行的版本。
 * <p>本程序是基于使用目的而加以发布,然而不负任何担保责任;亦无对适售性或特定目的适用性所为的默示性担保。详情请参照 GNU 通用公共授权。
 * <p>您应已收到附随于本程序的 GNU 通用公共授权的副本;如果没有,请参照http://www.gnu.org/licenses/.
 * <p>联系方式：uraplutonium@gmail.com
 * 
 * @version  1.0 16/05/2011
 * @author  Felix.Ting
 * 
 * @see ClassManager
 * @see ConfParser
 * @see DFSManager
 * @see Fetcher
 * @see Iverter
 * @see Extender
 * @see Filter
 */
public class HDCrawler {
	
	private static long starttime;			// 程序运行开始时间
	private static int maximumdepth;		// 爬虫最大爬取深度

	public static Selector selector;		// 选择器
	public static Evaluator evaluator;		// 评估器
	public static Eliminator eliminator;	// 消重器
	
	public static String workspace;		// 分布式文件系统根目录下的工作空间
	public static Set<String> keywords;	// 主题爬虫的关键字集合，为空时则作为非主题爬虫运行
	
	/**
	 * 将实现Selector、Evaluator、Eliminator接口的算法类注册入ClassManager
	 */
	private static void registerClass() {
		SelectAll selectall = new SelectAll();
		SelectTopTen selecttopten = new SelectTopTen();
		CountEvaluator countevaluator = new CountEvaluator();
		SimpleURLEliminator simpleURLEliminator = new SimpleURLEliminator();
		
		ClassManager.register(selectall);
		ClassManager.register(selecttopten);
		ClassManager.register(countevaluator);
		ClassManager.register(simpleURLEliminator);
	}
	
	/**
	 * 初始化选择器
	 * @param name 所要使用的选择器的名称
	 * @return 初始化成功则返回true，失败则返回false
	 */
	private static boolean initSelector(String name) {
		selector = ClassManager.getSelector(name);
		if(selector == null)
			return false;
		else
			return true;
	}
	
	/**
	 * 初始化评估器
	 * @param name 所要使用的评估器的名称
	 * @return 初始化成功则返回true，失败则返回false
	 */
	private static boolean initEvaluator(String name) {
		evaluator = ClassManager.getEvaluator(name);
		if(evaluator == null)
			return false;
		else
			return true;
	}
	
	/**
	 * 初始化消重器
	 * @param name 所要使用的消重器的名称
	 * @return 初始化成功则返回true，失败则返回false
	 */
	private static boolean initEliminator(String name) {
		eliminator = ClassManager.getEliminator(name);
		if(eliminator == null)
			return false;
		else
			return true;
	}
	
	/**
	 * 打开master本地的种子URL文件，建立SeedURL集合
	 * @param path 种子URL的路径
	 * @return SeedURL集合
	 */
	private static Set<String> openSeedURL(String path) {
		Set<String> seedURL = new TreeSet<String>();
		try {
			FileReader filereader = new FileReader(path);
			BufferedReader bufferreader = new BufferedReader(filereader);
	        String url;
	        while (bufferreader.ready()) {
	        	url = bufferreader.readLine();
	        	seedURL.add(url);
	        	System.out.println(url);
	        }
	        bufferreader.close();
	        filereader.close();
		}
		catch(FileNotFoundException exc) {
			exc.printStackTrace();
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
		return seedURL;
	}
	
	/**
	 * @param args 位于master本地的配置文件的路径
	 */
	public static void main(String[] args) {
		Calendar calendar = new GregorianCalendar();
		starttime = calendar.getTimeInMillis();		// 取得系统时间，作为程序开始运行的时间
		System.out.println("StartTime:" + calendar.getTimeInMillis());
		
		ConfParser conf = new ConfParser(args[0]);	// 读取master本地的配置文件，将配置导入到一个conf对象中	
		
		Set<String> seedURL;
		Set<String> selectedURL;
		Map<String, Integer> URLPool;
		Map<String, Integer> URLPoolCopy;
		
		keywords = conf.getKeywords();			// 从配置取得主题爬虫的关键字集合
		maximumdepth = conf.getMaximumDepth();	// 从配置文件取得最大爬取深度
		workspace = conf.getWorkspace();		// 从配置文件取得hdfs上的工作空间
		seedURL = openSeedURL(conf.getSeedUrlPath());	// 从配置文件取得种子URL文件路径，并打开导入到SeedURL集合

		// 从配置文件取得并计算运行停止的临界时间
		long criticalRuntime =
			1000*(60*(60*(24*Integer.valueOf(conf.getCriticalDay())
				+ Integer.valueOf(conf.getCriticalHour()))
				+ Integer.valueOf(conf.getCriticalMinute()))
				+ Integer.valueOf(conf.getCriticalSecond()));
		
		registerClass();	// 注册所有的算法类
		
		initSelector(conf.getSelector());		// 初始化Selector	
		initEvaluator(conf.getEvaluator());		// 初始化Evaluator
		initEliminator(conf.getEliminator());	// 初始化Eliminator
		
		DFSManager.importSeed(seedURL);		// 将SeedURL集合导入到hdfs
		
		Fetcher fetcher = new Fetcher();	// URL信息抓取模块
		Inverter inverter = new Inverter();	// URL信息倒排模块
		Extender extender = new Extender();	// URL外链扩展模块
		Filter filter = new Filter();		// URL外链过滤模块
		
		int dep = 0;
		// 循环爬取，直到达到最大爬取深度或临界运行时间为止
		while(dep < maximumdepth && (new GregorianCalendar().getTimeInMillis() - starttime < criticalRuntime)) {			
			URLPool = DFSManager.openURLPool();	// 从hdfs上打开URL池，导入到URLPool对象
			
			selectedURL = selector.getSelectedURL(URLPool);	// 使用选择器从URLPool选择需要处理的URL
			DFSManager.uploadSelectedURL(selectedURL);		// 将选择的URL上传至hdfs
			DFSManager.updateScannedURL(selectedURL);		// 将选择的URL加入到ScannedURL文件中
			
			URLPoolCopy = selector.getLeftURL(URLPool);	// 使用选择器得到暂未处理的URL
			DFSManager.updateURLPool(URLPoolCopy);		// 将URLPool的副本（暂未处理的URL）上传至hdfs
			
			DFSManager.deleteURLInfo();		// 删除hdfs上已存在的URL信息文件
			fetcher.fetch();				// 运行URL信息抓取的map-reduce模块
			
			DFSManager.updateInvertedTable();	// 更新hdfs上的倒排表文件
			inverter.invert();					// 运行URL信息倒排的map-reduce模块
			
			DFSManager.deleteExtendedURL();	// 删除hdfs上已存在的URL外链文件
			extender.extend();				// 运行URL外链扩展的map-reduce模块
			
			DFSManager.deleteTempURLPool();	// 删除hdfs上已存在的临时URL池文件
			filter.filter();				// 运行URL外链过滤的map-reduce模块
			
			DFSManager.splitTempURLPool();	// 分离临时URL池，生成URLPool文件和repeatedURL文件
			dep++;
		}
	}

}