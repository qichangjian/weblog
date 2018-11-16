package com.mazh.aura.clickstream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.mazh.aura.mrbean.PageViewsBean;
import com.mazh.aura.mrbean.VisitBean;
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
 * 输入数据：pageviews模型结果数据
 * 从pageviews模型结果数据中进一步梳理出visit模型
 * sessionid  start-time   out-time   start-page   out-page   pagecounts  ......
 *
 * 原始数据
 * ffa9ac81-7d88-4d3d-ad9d-913a076b9400113.107.237.31-2013-09-18 09:06:46/finance-rhive-repurchase/160"-""-"45271200
 * 转换后的数据
 *7389ce87-3df7-409f-b983-748bf6bef8b5208.43.225.842013-09-18 17:21:312013-09-18 17:27:23/cassandra-clustor//about"-"3
 */
public class ClickStreamVisit {

    // 以session作为key，发送数据到reducer
    static class ClickStreamVisitMapper extends Mapper<LongWritable, Text, Text, PageViewsBean> {

        PageViewsBean pvBean = new PageViewsBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //一个session会话一组
            String line = value.toString();
            String[] fields = line.split("\001");
            int step = Integer.parseInt(fields[5]);
            //(String session, String remote_addr, String timestr, String request, int step, String staylong, String referal, String useragent, String bytes_send, String status)
            //299d6b78-9571-4fa9-bcc2-f2567c46df3472.46.128.140-2013-09-18 07:58:50/hadoop-zookeeper-intro/160"https://www.aura.cn/""Mozilla/5.0"14722200
            pvBean.set(fields[0], fields[1], fields[2], fields[3], fields[4], step, fields[6], fields[7], fields[8], fields[9]);
            k.set(pvBean.getSession());
            context.write(k, pvBean);

        }

    }

    static class ClickStreamVisitReducer extends Reducer<Text, PageViewsBean, NullWritable, VisitBean> {

        @Override
        protected void reduce(Text session, Iterable<PageViewsBean> pvBeans, Context context) throws IOException, InterruptedException {

            // 将pvBeans按照step排序
            ArrayList<PageViewsBean> pvBeansList = new ArrayList<PageViewsBean>();
            for (PageViewsBean pvBean : pvBeans) {
                PageViewsBean bean = new PageViewsBean();
                try {
                    BeanUtils.copyProperties(bean, pvBean);
                    pvBeansList.add(bean);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            //按照step排序
            Collections.sort(pvBeansList, new Comparator<PageViewsBean>() {

                @Override
                public int compare(PageViewsBean o1, PageViewsBean o2) {

                    return o1.getStep() > o2.getStep() ? 1 : -1;
                }
            });

            // 取这次visit的首尾pageview记录，将数据放入VisitBean中
            VisitBean visitBean = new VisitBean();
            // 取visit的首记录
            visitBean.setInPage(pvBeansList.get(0).getRequest());
            visitBean.setInTime(pvBeansList.get(0).getTimestr());
            // 取visit的尾记录
            visitBean.setOutPage(pvBeansList.get(pvBeansList.size() - 1).getRequest());
            visitBean.setOutTime(pvBeansList.get(pvBeansList.size() - 1).getTimestr());
            // visit访问的页面数
            visitBean.setPageVisits(pvBeansList.size());
            // 来访者的ip
            visitBean.setRemote_addr(pvBeansList.get(0).getRemote_addr());
            // 本次visit的referal
            visitBean.setReferal(pvBeansList.get(0).getReferal());
            visitBean.setSession(session.toString());

            context.write(NullWritable.get(), visitBean);
        }
    }

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hadoop1");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ClickStreamVisit.class);

        job.setMapperClass(ClickStreamVisitMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageViewsBean.class);

        job.setReducerClass(ClickStreamVisitReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(VisitBean.class);


//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		FileInputFormat.setInputPaths(job, new Path("/weblog/pageviews"));
//		FileOutputFormat.setOutputPath(job, new Path("/weblog/visitout"));
        FileInputFormat.setInputPaths(job, new Path("/logerror_out4/"));
        FileOutputFormat.setOutputPath(job, new Path("/logerror_out5/"));

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
