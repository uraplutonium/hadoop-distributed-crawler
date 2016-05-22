package hdcrawler;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * 配置文件分析器
 * @author  Felix.Ting
 * @see DocumentBuilderFactory
 * @see DocumentBuilder
 */
public class ConfParser {
	
	private String workspace = null;
	private String seedurlpath = null;
	private int maximumdepth = 0;
	private int criticalday = 0;
	private int criticalhour = 0;
	private int criticalminute = 0;
	private int criticalsecond = 0;
	private Set<String> keywords = null;
	private String selector = null;
	private String evaluator = null;
	private String eliminator = null;
	
	public String getWorkspace() {
		return workspace;
	}
	
	public String getSeedUrlPath() {
		return seedurlpath;
	}
	
	public int getMaximumDepth() {
		return maximumdepth;
	}
	
	public int getCriticalDay() {
		return criticalday;
	}
	
	public int getCriticalHour() {
		return criticalhour;
	}
	
	public int getCriticalMinute() {
		return criticalminute;
	}
	
	public int getCriticalSecond() {
		return criticalsecond;
	}
	
	public Set<String> getKeywords() {
		return keywords;
	}
	
	public String getSelector() {
		return selector;
	}
	
	public String getEvaluator() {
		return evaluator;
	}
	
	public String getEliminator() {
		return eliminator;
	}
	
	public ConfParser(String path) {
		DocumentBuilderFactory domfac = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder dombuilder=domfac.newDocumentBuilder();
			InputStream instream = new FileInputStream(path);
			Document doc = dombuilder.parse(instream);
			Element root = doc.getDocumentElement();
			NodeList confs = root.getChildNodes();
			
			if(confs != null) {
				int i;
				for(i = 0 ; i < confs.getLength() ; i++) {
					Node conf=confs.item(i);
					if(conf.getNodeType() == Node.ELEMENT_NODE){
						if(conf.getNodeName() == "workspace") {			//若结点描述工作空间
							workspace = new String(conf.getFirstChild().getNodeValue());
						}
						else if(conf.getNodeName() == "seedurlpath") {	// 若结点描述种子URL文件路径
							seedurlpath = new String(conf.getFirstChild().getNodeValue());
						}
						else if(conf.getNodeName() == "maximumdepth") {	// 若结点描述最大深度
							maximumdepth = Integer.valueOf(conf.getFirstChild().getNodeValue());
						}
						else if(conf.getNodeName() == "criticaltime") {	// 若结点描述临界时间
							for(Node timenode = conf.getFirstChild() ; timenode!=null ; timenode = timenode.getNextSibling()) {
								if(timenode.getNodeType() == Node.ELEMENT_NODE) {
									if(timenode.getNodeName() == "day") {
										criticalday = Integer.valueOf(timenode.getFirstChild().getNodeValue());
									}
									else if(timenode.getNodeName() == "hour") {
										criticalhour = Integer.valueOf(timenode.getFirstChild().getNodeValue());
									}
									else if(timenode.getNodeName() == "minute") {
										criticalminute = Integer.valueOf(timenode.getFirstChild().getNodeValue());
									}
									else if(timenode.getNodeName() == "second") {
										criticalsecond = Integer.valueOf(timenode.getFirstChild().getNodeValue());
									}
								}
							}
						}
						else if(conf.getNodeName() == "theme") {		// 若结点描述爬虫主题
							keywords = new TreeSet<String>();
							for(Node wordnode = conf.getFirstChild() ; wordnode != null ; wordnode = wordnode.getNextSibling()) {
								if(wordnode.getNodeType() == Node.ELEMENT_NODE) {
									if(wordnode.getNodeName() == "keyword") {
										keywords.add(new String(wordnode.getFirstChild().getNodeValue()));
									}
								}
							}
						}
						else if(conf.getNodeName() == "classconf") {	// 若结点描述算法类
							for(Node timenode = conf.getFirstChild() ; timenode!=null ; timenode = timenode.getNextSibling()) {
								if(timenode.getNodeType() == Node.ELEMENT_NODE) {
									if(timenode.getNodeName() == "selector") {
										selector = new String(timenode.getFirstChild().getNodeValue());
									}
									else if(timenode.getNodeName() == "evaluator") {
										evaluator = new String(timenode.getFirstChild().getNodeValue());
									}
									else if(timenode.getNodeName() == "eliminator") {
										eliminator = new String(timenode.getFirstChild().getNodeValue());
									}
								}
							}
						}
					}
				}
			}
		}
		catch(ParserConfigurationException exc) {
			exc.printStackTrace();
		}
		catch(FileNotFoundException exc) {
			exc.printStackTrace();
		}
		catch(SAXException exc) {
			exc.printStackTrace();
		}
		catch(IOException exc) {
			exc.printStackTrace();
		}
	}
}