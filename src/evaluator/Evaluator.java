package evaluator;

import java.util.Set;

import fetcher.URLInfoWritable;

/**
 * 评估器接口：用于对某关键词与某网页的相关度进行评估
 * @author Felix.Ting
 */
public interface Evaluator {

	/**
	 * 取得评估器的名称，用于算法类管理器返回算法类实例
	 * @return 评估器的名称
	 */
	String getName();
	
	/**
	 * 对某一个关键词同某一个网页的相关度进行评估
	 * @param urlInfo 网页的URL信息
	 * @param keyword 关键词
	 * @return 相关度，应为一个非负数
	 */
	int getRank(URLInfoWritable urlInfo, String keyword);
	
	/**
	 * 对一些关键词组合同某一个网页的相关度进行评估
	 * @param urlInfo 网页的URL信息
	 * @param keywords 关键词集合
	 * @return 相关度，应为一个非负数
	 */
	int getRank(URLInfoWritable urlInfo, Set<String> keywords);
	
}