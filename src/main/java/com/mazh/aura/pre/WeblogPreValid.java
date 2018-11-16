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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeblogPreValid {

    static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, WebLogBean> {
        // 用来存储网站url分类数据
        Set<String> pages = new HashSet<String>();
        Text k = new Text();
        NullWritable v = NullWritable.get();

        /**
         * 从外部加载网站url分类数据
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

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            WebLogBean webLogBean = WebLogParser.parser(line);
            // 过滤js/图片/css等静态资源
            WebLogParser.filtStaticResource(webLogBean, pages);
            //如果是标记为无效的数据，就不输出
            if (webLogBean.isValid()) {
                k.set(webLogBean.getRemote_addr());
                context.write(k, webLogBean);
            }
        }

    }

    static class WeblogPreProcessReducer extends Reducer<Text, WebLogBean, NullWritable, WebLogBean> {

        @Override
        protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
            for (WebLogBean bean : values) {
                context.write(NullWritable.get(), bean);
            }
        }

    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WeblogPreValid.class);

        job.setMapperClass(WeblogPreProcessMapper.class);
        job.setReducerClass(WeblogPreProcessReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WebLogBean.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(WebLogBean.class);

//		 FileInputFormat.setInputPaths(job, new Path(args[0]));
//		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("c:/weblog/18"));
        FileOutputFormat.setOutputPath(job, new Path("c:/weblog/18valid"));


        job.waitForCompletion(true);

    }

}
