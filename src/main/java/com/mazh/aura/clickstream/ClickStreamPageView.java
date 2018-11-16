package com.mazh.aura.clickstream;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import com.mazh.aura.mrbean.WebLogBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 将清洗之后的日志梳理出点击流pageviews模型数据
 * 
 * 输入数据是清洗过后的结果数据
 * 
 * 区分出每一次会话，给每一次visit（session）增加了session-id（随机uuid）
 * 梳理出每一次会话中所访问的每个页面（请求时间，url，停留时长，以及该页面在这次session中的序号）
 * 保留referral_url，body_bytes_send，useragent
 *
 * 结果数据：
 * 745a8b10-d4d9-4d8b-9b09-2b3b299877b8174.120.8.226-2013-09-18 13:22:30/hadoop-mahout-roadmap/16"-""WordPress/3.3.1;http://www.getonboardbc.com"0200
 * 745a8b10-d4d9-4d8b-9b09-2b3b299877b8174.120.8.226-2013-09-18 13:22:36/hadoop-mahout-roadmap/2196"-""WordPress/3.3.1;http://www.getonboardbc.com"0200
 * 745a8b10-d4d9-4d8b-9b09-2b3b299877b8174.120.8.226-2013-09-18 13:25:52/hadoop-mahout-roadmap/371"-""WordPress/3.3.1;http://www.getonboardbc.com"0200
 * 745a8b10-d4d9-4d8b-9b09-2b3b299877b8174.120.8.226-2013-09-18 13:27:03/hadoop-mahout-roadmap/460"-""WordPress/3.3.1;http://www.getonboardbc.com"0200
 */
public class ClickStreamPageView {

	static class ClickStreamMapper extends Mapper<LongWritable, Text, Text, WebLogBean> {

		Text k = new Text();
		WebLogBean v = new WebLogBean();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			String[] fields = line.split("\001");
			if (fields.length < 9) return;
			//将切分出来的各字段set到weblogbean中
			v.set("true".equals(fields[0]) ? true : false, fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]);
			//只有有效记录才进入后续处理 key ip value:剩余的状态为true的：一个对象
			//ip相同的为一组
			if (v.isValid()) {
				k.set(v.getRemote_addr());
				context.write(k, v);
			}
		}
	}

	static class ClickStreamReducer extends Reducer<Text, WebLogBean, NullWritable, Text> {

		Text v = new Text();

		@Override
		protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
			ArrayList<WebLogBean> beans = new ArrayList<WebLogBean>();

			// 先将一个用户的所有访问记录中的时间拿出来排序
			try {
				for (WebLogBean bean : values) {
					WebLogBean webLogBean = new WebLogBean();
					try {
						//java对象拷贝之BeanUtils.copyProperties() https://blog.csdn.net/mr_linjw/article/details/50236279
						BeanUtils.copyProperties(webLogBean, bean);
					} catch(Exception e) {
						e.printStackTrace();
					}
					beans.add(webLogBean);//所有元素存放在Array中，如果没有array拿到的所有对象是都是最后一个
				}
				//将bean按时间先后顺序排序
				Collections.sort(beans, new Comparator<WebLogBean>() {

					//比较器：按照时间sort
					@Override
					public int compare(WebLogBean o1, WebLogBean o2) {
						try {
							Date d1 = toDate(o1.getTime_local());
							Date d2 = toDate(o2.getTime_local());
							if (d1 == null || d2 == null)
								return 0;
							return d1.compareTo(d2);
						} catch (Exception e) {
							e.printStackTrace();
							return 0;
						}
					}

				});

				/**
				 * 以下逻辑为：从有序bean中分辨出各次visit，并对一次visit中所访问的page按顺序标号step
				 * 核心思想：
				 * 就是比较相邻两条记录中的时间差，如果时间差<30分钟，则该两条记录属于同一个session
				 * 否则，就属于不同的session
				 */
				int step = 1;
				String session = UUID.randomUUID().toString();
				for (int i = 0; i < beans.size(); i++) {
					WebLogBean bean = beans.get(i);
					// 如果仅有1条数据，则直接输出
					if (1 == beans.size()) {
						
						// 设置默认停留市场为60s
						v.set(session+"\001"+key.toString()
								     +"\001"+bean.getRemote_user()
								     + "\001" + bean.getTime_local()
								     + "\001" + bean.getRequest()
								     + "\001" + step
								     + "\001" + (60)
								     + "\001" + bean.getHttp_referer()
									 + "\001" + bean.getHttp_user_agent()
								     + "\001" + bean.getBody_bytes_sent()
								     + "\001" + bean.getStatus());
						context.write(NullWritable.get(), v);
						session = UUID.randomUUID().toString();//重新生成一个uuid，下一个使用
						break;
					}

					// 如果不止1条数据，则将第一条跳过不输出，遍历第二条时再输出
					if (i == 0) {//第一条跳过，第二条输出第一条  循环结束，多输出一条，输出最后一条：所有输出延后一条，算时间差
						continue;
					}

					// 求近两次时间差
					long timeDiff = timeDiff(toDate(bean.getTime_local()), toDate(beans.get(i - 1).getTime_local()));
					// 如果本次-上次时间差<30分钟，则输出前一次的页面访问信息
					
					if (timeDiff < 30 * 60 * 1000) {
						//同一个session
						v.set(session+"\001"+key.toString()+"\001"+beans.get(i - 1).getRemote_user() + "\001" + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001" + step + "\001" + (timeDiff / 1000) + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
								+ beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent() + "\001" + beans.get(i - 1).getStatus());
						context.write(NullWritable.get(), v);
						step++;
					} else {
						
						// 如果本次-上次时间差>30分钟，则输出前一次的页面访问信息且将step重置，以分隔为新的visit
						v.set(session+"\001"+key.toString()+"\001"+beans.get(i - 1).getRemote_user() + "\001" + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001" + (step) + "\001" + (60) + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
								+ beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent() + "\001" + beans.get(i - 1).getStatus());
						context.write(NullWritable.get(), v);
						// 输出完上一条之后，重置step编号
						step = 1;
						session = UUID.randomUUID().toString();//下一个不同的session
					}

					// 如果此次遍历的是最后一条，则将本条直接输出
					if (i == beans.size() - 1) {
						// 设置默认停留市场为60s
						v.set(session+"\001"+key.toString()+"\001"+bean.getRemote_user() + "\001" + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step + "\001" + (60) + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001" + bean.getStatus());
						context.write(NullWritable.get(), v);
					}
				}

			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		private String toStr(Date date) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
			return df.format(date);
		}

		private Date toDate(String timeStr) throws ParseException {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
			return df.parse(timeStr);
		}

		private long timeDiff(String time1, String time2) throws ParseException {
			Date d1 = toDate(time1);
			Date d2 = toDate(time2);
			return d1.getTime() - d2.getTime();

		}
		//求时间差
		private long timeDiff(Date time1, Date time2) throws ParseException {
			return time1.getTime() - time2.getTime();
		}
	}

	public static void main(String[] args) throws Exception {

		System.setProperty("HADOOP_USER_NAME", "hadoop1");
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(ClickStreamPageView.class);

		job.setMapperClass(ClickStreamMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WebLogBean.class);

		job.setReducerClass(ClickStreamReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		FileInputFormat.setInputPaths(job, new Path("/weblog/output"));
//		FileOutputFormat.setOutputPath(job, new Path("/weblog/pageviews"));
		FileInputFormat.setInputPaths(job, new Path("/logerror_out2/"));
		FileOutputFormat.setOutputPath(job, new Path("/logerror_out4/"));

		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);

	}
}