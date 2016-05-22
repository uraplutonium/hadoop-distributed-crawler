package fetcher;

import java.io.IOException;
import java.io.StringReader;
import java.net.URLConnection;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.htmlparser.Node;
import org.htmlparser.Parser;
import org.htmlparser.filters.NotFilter;
import org.htmlparser.filters.OrFilter;
import org.htmlparser.filters.TagNameFilter;
import org.htmlparser.lexer.Lexer;
import org.htmlparser.nodes.TagNode;
import org.htmlparser.nodes.TextNode;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.util.NodeIterator;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;
import org.wltea.analyzer.lucene.IKAnalyzer;

/**
 * URL分析器
 * @author  Felix.Ting
 * @see Parser
 * @see  IKAnalyzer
 */
@SuppressWarnings("deprecation")
public class URLAnalyzer {
	
	private URLInfoWritable urlInfo;
	private int location;
	private int depth;

	/**
	 * 获取URL信息
	 * @param url 所要分析的URL
	 * @return URLInfoWritable类型的URL信息
	 */
	public URLInfoWritable getURLInfo(String url) {
		urlInfo = new URLInfoWritable();
		location = 0;
		depth = 0;
		parserHtml(url);
		return urlInfo;
	}
	
	/**
	 * 对html文本进行词法分析
	 * @param url 所要分析的url
	 */
	private void parserHtml(String url) {
		urlInfo.setURL(url);
		Parser parser = new Parser();
		System.out.println("parser:" + url);
		try {
			System.out.println("parser start.");
			parser.setURL(url);
			parser.setEncoding("UTF-8");
			
			URLConnection uc = parser.getConnection();
			uc.connect();
			System.out.println("url connected.");
			
			NodeIterator nit = parser.elements();
			while(nit.hasMoreNodes()) {
				Node node = nit.nextNode();
				parserNode(node);	// 对每个结点进行词法分析
			}			
		}
		catch(ParserException exc) {
			System.out.println("ParserException");
			//exc.printStackTrace();
		}
		catch(IOException exc) {
			System.out.println("IOException");
			//exc.printStackTrace();
		}
	}
	
	/**
	 * 对结点进行词法分析
	 * @param node 所要分析的结点
	 */
	private void parserNode(Node node) {
		depth ++;
		String regex = "[ \b\t\n\f\r]*";
		if(node instanceof TextNode) {	// 若为文本结点，则进行分词
			if(depth == 1) {
				System.out.println("TextNode!");
				Lexer lexer = new Lexer(node.getPage());
				Parser parser = new Parser(lexer, Parser.STDOUT);
				//TODO filter script & style
				OrFilter it = new OrFilter(new NotFilter(new TagNameFilter("script ")), new NotFilter(new TagNameFilter("style ")));

				try {
					NodeList nl = parser.extractAllNodesThatMatch(it);
					NodeIterator nit = nl.elements();
					while(nit.hasMoreNodes()) {
						Node n = nit.nextNode();
						if(n instanceof TextNode) {
							if(!(n.getText().matches(regex))) {	// 用正则表达式进行匹配，对非空的文本进行分词
								segment(n.getText());	// 对网页中的文本进行分词
							}
						}
					}
				}
				catch(ParserException exc) {
					System.out.println("ParserException");
					//exc.printStackTrace();
				}
			}
		}
		else if(node instanceof TagNode) {	// 若为链接结点，则扩展外链
			if(node instanceof LinkTag) {
				LinkTag tag = (LinkTag)node;
				if(!(tag.getLink().matches(regex))) {	
					urlInfo.addExtendedURL(tag.getLink());	// 将得到的外链加入到urlInfo中
				}
			}			
			dealTag(node);
		}
		depth --;
	}
	
	/**
	 * 处理标签
	 * @param tag 所要处理的标签
	 */
	private void dealTag(Node tag) {		
		NodeList list = tag.getChildren();
		if(list != null) {
			NodeIterator nit = list.elements();
			try {
				while(nit.hasMoreNodes()) {
					Node node = nit.nextNode();
					parserNode(node);	// 递归调用分析结点
				}
			}
			catch(ParserException exc) {
				System.out.println("ParserException");
				//exc.printStackTrace();
			}
		}
	}
	
	/**
	 * 对一段文本进行分词，并将分词及其位置加入到urlInfo中
	 * @param text 待分词的文本
	 */
	private void segment(String text) {
		IKAnalyzer analyzer = new IKAnalyzer(true);
		StringReader reader = new StringReader(text);
		TokenStream tokenStream = analyzer.tokenStream("*", reader);
		TermAttribute termAtt = tokenStream.getAttribute(TermAttribute.class);
		
		try {
			while (tokenStream.incrementToken()) {
				location ++;
				String term = termAtt.term();		
				urlInfo.putURLLocation(term, location);
			}
		}
		catch(IOException exp) {
			exp.printStackTrace();
		}
	}
	
}