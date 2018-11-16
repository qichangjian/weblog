package com.mazh.aura.pre;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.mazh.aura.mrbean.WebLogBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 处理原始日志，过滤出真实pv请求 转换时间格式 对缺失字段填充默认值
 * 对记录标记valid和invalid
 *
 * 当前程序的目的：
 *     就是为了把原始数据：access.log清洗成：ods_weblog_orgin
 *
 *
 *     163.177.71.12 - - [18/Sep/2013:06:49:33 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"
 * 清洗为：
 *     false163.177.71.12-2013-09-18 06:49:33/20020"-""DNSPod-Monitor/1.0"
 *   是否合法
 */
public class WeblogPreProcess {

    static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        // 用来存储网站url分类数据  就是不需要过滤的url数据
        Set<String> pages = new HashSet<String>();
        Text k = new Text();
        NullWritable v = NullWritable.get();

        /**
         * 从外部配置文件中加载网站的有用url分类数据 存储到maptask的内存中，用来对日志数据进行过滤
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            pages.add("/about");
            pages.add("/black-ip-list/");
            pages.add("/cassandra-clustor/");
            pages.add("/finance-rhive-repurchase/");
            pages.add("/hadoop-family-roadmap/");
            pages.add("/hadoop-hive-intro/");
            pages.add("/hadoop-zookeeper-intro/");
            pages.add("/hadoop-mahout-roadmap/");
            //只有url包含这些才是合法的
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            WebLogBean webLogBean = WebLogParser.parser(line);
            if (webLogBean != null) {//错误页面返回是null,需要过滤
                // 过滤js/图片/css等静态资源
                WebLogParser.filtStaticResource(webLogBean, pages);
                //非法数据过滤 下边是
                /* if (!webLogBean.isValid()) return; */
                k.set(webLogBean.toString());
                context.write(k, v);
            }
        }

    }

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "hadoop1");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WeblogPreProcess.class);

        job.setMapperClass(WeblogPreProcessMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

//		 FileInputFormat.setInputPaths(job, new Path(args[0]));
//		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("/weblog/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/weblog/output/"));
//        FileInputFormat.setInputPaths(job, new Path("/logerror_in/"));
//        FileOutputFormat.setOutputPath(job, new Path("/logerror_out3"));

        job.setNumReduceTasks(0);

        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);

    }
}
