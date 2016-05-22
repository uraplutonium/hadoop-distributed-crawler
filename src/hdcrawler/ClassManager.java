package hdcrawler;

import java.util.HashMap;
import java.util.Map;

import selector.Selector;
import evaluator.Evaluator;
import eliminator.Eliminator;

/**
 * 算法类管理器
 * @author Felix.Ting
 * 
 * @see Selector
 * @see Evaluator
 * @see Eliminator
 */
public class ClassManager {
	
	private static Map<String, Selector> selectorMap = new HashMap<String, Selector>();			// 选择器Map
	private static Map<String, Evaluator> evaluatorMap = new HashMap<String, Evaluator>();		// 评估器Map
	private static Map<String, Eliminator> eliminatorMap = new HashMap<String, Eliminator>();	// 消重器Map
	
	/**
	 * 注册一个算法类
	 * @param obj 实现了任一种算法接口的类的实例化对象
	 * @return 若传入对象符合任一种算法接口，则注册成功且返回true，否则返回false
	 */
	public static boolean register(Object obj) {
		boolean legalclass;
		if(obj instanceof Selector) {
			Selector selector = (Selector)obj;
			selectorMap.put(selector.getName(), selector);
			legalclass = true;
		}
		else if(obj instanceof Evaluator) {
			Evaluator evaluator = (Evaluator)obj;
			evaluatorMap.put(evaluator.getName(), evaluator);
			legalclass = true;
		}
		else if(obj instanceof Eliminator) {
			Eliminator eliminator = (Eliminator)obj;
			eliminatorMap.put(eliminator.getName(), eliminator);
			legalclass = true;
		}
		else
			legalclass = false;
		
		return legalclass;
	}
	
	/**
	 * 从算法名取得一个选择器对象
	 * @param name 选用的选择器类的名称
	 * @return 一个选择器对象
	 */
	public static Selector getSelector(String name) {
		Selector sel = selectorMap.get(name);
		System.out.println("Selector: " + sel.toString());
		return sel;
	}
	
	/**
	 * 从算法名取得一个评估器对象
	 * @param name 选用的评估器类的名称
	 * @return 一个评估器对象
	 */
	public static Evaluator getEvaluator(String name) {
		Evaluator eva = evaluatorMap.get(name);
		System.out.println("Evaluator: " + eva.toString());
		return eva;
	}
	
	/**
	 * 从算法名取得一个消重器对象
	 * @param name 选用的消重器类的名称
	 * @return 一个消重器对象
	 */
	public static Eliminator getEliminator(String name) {
		Eliminator eli = eliminatorMap.get(name);
		System.out.println("Eliminator: " + eli.toString());
		return eli;
	}
	
}