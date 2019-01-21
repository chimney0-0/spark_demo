package tag;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author chimney
 */
public class JsFunction {

	static ScriptEngineManager manager = new ScriptEngineManager();
	static ScriptEngine engine = manager.getEngineByName("javascript");
	static Compilable compilable = (Compilable) engine;
	static String customJs = "";

	static {
		try {

			customJs += "Date.prototype.format=function(format){var date={\"M+\":this.getMonth()+1,\"d+\":this.getDate(),\"h+\":this.getHours(),\"m+\":this.getMinutes(),\"s+\":this.getSeconds(),\"q+\":Math.floor((this.getMonth()+3)/3),\"S+\":this.getMilliseconds()};if(/(y+)/i.test(format)){format=format.replace(RegExp.$1,(this.getFullYear()+\"\").substr(4-RegExp.$1.length))}for(var k in date){if(new RegExp(\"(\"+k+\")\").test(format)){format=format.replace(RegExp.$1,RegExp.$1.length==1?date[k]:(\"00\"+date[k]).substr((\"\"+date[k]).length))}}return format};  ";
			customJs += "var DateUtils = Java.type('com.seassoon.utils.DateUtils');";
			customJs += "function formatDateNoSign(date){ return  DateUtils.format(DateUtils.parseDate(date,'yyyyMMdd'),'yyyy-MM-dd');  }";
			customJs += "function contains(value,data,operation){var result=true;if(operation.toLowerCase()==\"and\"){for(var i in data){if(value.indexOf(data[i])==-1){result=false;break}}}else{if(operation.toLowerCase()==\"or\"){result=false;for(var i in data){if(value.indexOf(data[i])>-1){result=true;break}}}else{result=false}}return result}function notContains(value,data,operation){var result=true;if(operation.toLowerCase()==\"and\"){for(var i in data){if(value.indexOf(data[i])>-1){result=false;break}}}else{if(operation.toLowerCase()==\"or\"){result=false;for(var i in data){if(value.indexOf(data[i])==-1){result=true;break}}}else{result=false}}return result};";

			customJs += "function cccccc(value){return value+1; } function contains(value,data,operation){var result=true;if(operation.toLowerCase()==\"and\"){for(var i in data){if(value.indexOf(data[i])==-1){result=false;break}}}else{if(operation.toLowerCase()==\"or\"){result=false;for(var i in data){if(value.indexOf(data[i])>-1){result=true;break}}}else{result=false}}return result}function notContains(value,data,operation){var result=true;if(operation.toLowerCase()==\"and\"){for(var i in data){if(value.indexOf(data[i])>-1){result=false;break}}}else{if(operation.toLowerCase()==\"or\"){result=false;for(var i in data){if(value.indexOf(data[i])==-1){result=true;break}}}else{result=false}}return result};";

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static Object execute(Map<String, Object> map, String js, String onErrorType) {
		js = customJs + js;

		Bindings bindings = engine.createBindings();
		for (Entry<String, Object> m : map.entrySet()) {
			bindings.put(m.getKey(), m.getValue());
		}

		Object result = null;
		try {

			result = compilable.compile(js).eval(bindings);
			if (result instanceof ScriptObjectMirror) {
				throw new Exception(result.toString());

			}

		} catch (Exception e) {

			if (onErrorType != null) {

				if (OnErrorTypeEnum.SET_TO_BLANK.name().equals(onErrorType)) {
					return "";
				} else if (OnErrorTypeEnum.STORE_ERROR.name().equals(onErrorType)) {
					return e.getMessage();
				} else if (OnErrorTypeEnum.KEEP_ORIGINAL.name().equals(onErrorType)) {
					return map.get("value");
				}

			}
		}

		return result;
	}

}
