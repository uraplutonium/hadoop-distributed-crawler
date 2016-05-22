package hdcrawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * DFS(分布式文件系统)管理器
 * @author Felix.Ting
 * 
 * @see FileSystem
 */
public class DFSManager {
	
	/**
	 * 从dfs的工作空间中删除文件/目录
	 * @param filename 所要删除的文件/目录从工作空间开始的路径
	 */
	private static void deleteFromDFS(String filename) {
		Configuration conf = new Configuration();
		try {
			Path filePath = new Path(HDCrawler.workspace + filename);
			FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), conf);
			
			boolean exist = hdfs.exists(filePath);
			if(exist) {
				System.out.println(filename + " already exists.");
				boolean deleted;
				do {
					deleted = hdfs.delete(filePath, true);
				}
				while(!deleted);
				System.out.println(filename + " deleted.");
			}
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
	
	/**
	 * 将种子URL导入到hdfs中的URL池
	 * @param seedURL 种子URL集合
	 */
	public static void importSeed(Set<String> seedURL) {
		Configuration conf = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), conf);
			
			deleteFromDFS("URLPool");	// 删除已有的URL池

			FSDataOutputStream outstream = hdfs.create(new Path(HDCrawler.workspace + "URLPool"));
			for(String url : seedURL) {
				outstream.write(url.getBytes());
				outstream.write('\t');
				outstream.write('0');
				outstream.write('\n');
			}
			outstream.flush();
			outstream.close();
			System.out.println("SeedURL imported.");
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
	
	/**
	 * 打开hdfs上的URL池
	 * @return URL池Map
	 */
	public static Map<String, Integer> openURLPool() {
		Map<String, Integer> URLPool = new HashMap<String, Integer>();
		Configuration conf = new Configuration();
		try {
			Path poolPath = new Path(HDCrawler.workspace + "URLPool");
			FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), conf);
			
			boolean exist = hdfs.exists(poolPath);
			if(exist) {
				FSDataInputStream instream = hdfs.open(poolPath);
				BufferedReader bufferreader = new BufferedReader(new InputStreamReader(instream));
				Integer rank;
				String url;
				while(bufferreader.ready()) {
					String line = bufferreader.readLine();
					StringTokenizer token = new StringTokenizer(line);
					url = token.nextToken();
					rank = Integer.valueOf(token.nextToken());
					URLPool.put(url, rank);
				}
				bufferreader.close();
				instream.close();		
			}
				
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
		return URLPool;
	}
	
	/**
	 * 将selectedURL上传至hdfs
	 * @param selectedURL 已选择的URL集合
	 */
	public static void uploadSelectedURL(Set<String> selectedURL) {
		Configuration conf = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), conf);
			
			deleteFromDFS("selectedURL");

			FSDataOutputStream outstream = hdfs.create(new Path(HDCrawler.workspace + "selectedURL"));
			for(String url : selectedURL) {
				outstream.write(url.getBytes());
				outstream.write('\n');
			}
			outstream.flush();
			outstream.close();
			System.out.println("SelectedURL uploaded.");
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
	
	/**
	 * 将新选择的URL加入到已扫描URL文件中
	 * @param selectedURL 已选择的URL集合
	 */
	public static void updateScannedURL(Set<String> selectedURL) {
		Set<String> scannedURL = new TreeSet<String>();
		Configuration conf = new Configuration();
		Path urlPath = new Path(HDCrawler.workspace + "scannedURL");
		try {
			FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), conf);
			
			boolean exist = hdfs.exists(urlPath);
			if(exist) {
				FSDataInputStream instream = hdfs.open(urlPath);
				BufferedReader bufferreader = new BufferedReader(new InputStreamReader(instream));
				while(bufferreader.ready()) {
					String line = bufferreader.readLine();
					scannedURL.add(line);
				}
				bufferreader.close();
				instream.close();
			}
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
		
		scannedURL.addAll(selectedURL);
		
		try {			
			deleteFromDFS("scannedURL");
			
			FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), conf);

			FSDataOutputStream outstream = hdfs.create(new Path(HDCrawler.workspace + "scannedURL"));
			for(String url : scannedURL) {
				outstream.write(url.getBytes());
				outstream.write('\n');
			}
			outstream.flush();
			outstream.close();
			System.out.println("scannedURL updated.");
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
	
	/**
	 * 删除原有的URL池副本，根据当前URL池生成新的URL池副本
	 * @param URLPoolCopy URL池副本Map
	 */
	public static void updateURLPool(Map<String, Integer> URLPoolCopy) {
		Configuration conf = new Configuration();
		try {			
			FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), conf);
			
			deleteFromDFS("URLPoolCopy");

			FSDataOutputStream outstream = hdfs.create(new Path(HDCrawler.workspace + "URLPoolCopy"));
			for(String url : URLPoolCopy.keySet()) {
				outstream.write(url.getBytes());
				outstream.write('\t');
				outstream.write(URLPoolCopy.get(url).toString().getBytes());
				outstream.write('\n');
			}
			outstream.flush();
			outstream.close();
			System.out.println("URLPoolCopy uploaded.");
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
	
	/**
	 * 将临时URL池分割，生成URL池以及重复URL列表
	 */
	public static void splitTempURLPool() {
		Map<String, Integer> tempURLPool = new HashMap<String, Integer>();
		Configuration conf = new Configuration();
		
		Path tempPath = new Path(HDCrawler.workspace + "tempURLPool/part-r-00000");
		try {
			FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), conf);
			
			boolean exist = hdfs.exists(tempPath);
			if(exist) {
				FSDataInputStream instream = hdfs.open(tempPath);
				BufferedReader bufferreader = new BufferedReader(new InputStreamReader(instream));
				while(bufferreader.ready()) {
					String line = bufferreader.readLine();
					StringTokenizer token = new StringTokenizer(line);
					String url = token.nextToken();
					Integer asm = Integer.valueOf(token.nextToken());
					tempURLPool.put(url, asm);
				}
				bufferreader.close();
				instream.close();
			}
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}	
		
		try {			
			FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), conf);
			
			deleteFromDFS("URLPool");
			deleteFromDFS("repeatedURL");

			FSDataOutputStream poolstream = hdfs.create(new Path(HDCrawler.workspace + "URLPool"));
			FSDataOutputStream repeatedstream = hdfs.create(new Path(HDCrawler.workspace + "repeatedURL"));
			
			for(String url : tempURLPool.keySet()) {
				if(tempURLPool.get(url) == -1) {
					repeatedstream.write(url.getBytes());
					repeatedstream.write('\t');
					repeatedstream.write("-1".getBytes());
					repeatedstream.write('\n');
				}
				else {
					poolstream.write(url.getBytes());
					poolstream.write('\t');
					poolstream.write(tempURLPool.get(url).toString().getBytes());
					poolstream.write('\n');
				}
			}
			repeatedstream.flush();
			poolstream.flush();
			
			repeatedstream.close();
			poolstream.close();
			
			System.out.println("URLPool & repeatedURL uploaded.");
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
	
	/**
	 * 删除已有的URL信息文件
	 */
	public static void deleteURLInfo() {
		deleteFromDFS("URLInfo");
	}
	
	/**
	 * 删除已有的临时倒排表文件，并将现有的倒排表文件重命名为"tempInertedTalbe"
	 */
	public static void updateInvertedTable() {
		deleteFromDFS("tempInvertedTable");
		
		Configuration conf = new Configuration();
		Path tablePath = new Path(HDCrawler.workspace + "InvertedTable");
		Path tempPath = new Path(HDCrawler.workspace + "tempInvertedTable");

		try {			
			FileSystem hdfs = FileSystem.get(URI.create(HDCrawler.workspace), conf);
			
			boolean exist = hdfs.exists(tablePath);
			if(exist) {
				boolean renamed = false;
				do {
					renamed = hdfs.rename(tablePath, tempPath);
				}
				while(!renamed);
				System.out.println("tempInvertedTable renamed.");
			}
			else {
				FSDataOutputStream outstream = hdfs.create(tempPath);
				outstream.flush();
				outstream.close();
				System.out.println("tempInvertedTable created.");
			}
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
	
	/**
	 * 删除已有的URL外链文件
	 */
	public static void deleteExtendedURL() {
		deleteFromDFS("extendedURL");
	}
	
	/**
	 * 删除已有的临时URL池文件
	 */
	public static void deleteTempURLPool() {
		deleteFromDFS("tempURLPool");
	}
	
}