package com.mazh.aura.pre;

import com.mazh.aura.mrbean.WebLogBean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Set;

/**
 * 日志解析器
 *  目的就是把access.log中的一行日志解析成一个 WebLogBean
 */
public class WebLogParser {

	public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

	public static WebLogBean parser(String line) {
		WebLogBean webLogBean = new WebLogBean();
		String[] arr = line.split(" ");
		for (int i = 0; i < arr.length; i++) {
			System.out.println("i:" + arr[i]);
		}
		if (arr.length > 11) {//没有HEAD / HTTP/1.1这个的直接删除 也就是404的
			webLogBean.setRemote_addr(arr[0]);
			webLogBean.setRemote_user(arr[1]);
			String time_local = formatDate(arr[3].substring(1));//去掉[
			if(null==time_local || "".equals(time_local)) time_local="-invalid_time-";//如果没有时间添加默认为-invalid_time-
			webLogBean.setTime_local(time_local);
			webLogBean.setRequest(arr[6]);
			webLogBean.setStatus(arr[8]);
			webLogBean.setBody_bytes_sent(arr[9]);
			webLogBean.setHttp_referer(arr[10]);

			//如果useragent元素较多，拼接useragent
			if (arr.length > 12) {
				StringBuilder sb = new StringBuilder();
				for(int i=11;i<arr.length;i++){
					sb.append(arr[i]);
				}
				webLogBean.setHttp_user_agent(sb.toString());
			} else {
				webLogBean.setHttp_user_agent(arr[11]);
			}

			// 大于400，HTTP错误
			if (Integer.parseInt(webLogBean.getStatus()) >= 400) {
				webLogBean.setValid(false);
			}
			
			if("-invalid_time-".equals(webLogBean.getTime_local())){
				webLogBean.setValid(false);
			}
		} else {
			webLogBean=null;
		}
		return webLogBean;
	}

	/**
	 * 添加数据记录标识
	 * 判断url数据是否合法(是否包含page set中字符串)，不和法就添加false
	 */
	public static void filtStaticResource(WebLogBean bean, Set<String> pages) {
		//不包含pages中的内容就是非法的 ，  非法的设置为false
		if (!pages.contains(bean.getRequest())) {
			bean.setValid(false);
		}
	}

	/**
	 * 格式化时间
	 * 时间转换
	 */
	public static String formatDate(String time_local) {
		try {
			return df2.format(df1.parse(time_local));
		} catch (ParseException e) {
			return null;
		}
	}
}
