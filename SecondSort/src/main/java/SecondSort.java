import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.Collections;
import java.util.Vector;



public class SecondSort {

    public static class SecondSortMapper
            extends Mapper<Object, Text, TwoNum, Text>{
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{

            String [] arr = value.toString().split("\\s+");
            TwoNum TN = new TwoNum();
            TN.set(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]));
            context.write(TN ,new Text());
        }
    }

    public static class SecondSortReducer
            extends Reducer<TwoNum,Text, IntWritable,IntWritable> {
        private Text result = new Text();

        @Override
        protected void reduce(TwoNum key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            context.write(new IntWritable(key.getA()),new IntWritable(key.getB()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SecondSort");
        job.setJarByClass(SecondSort.class);
        job.setMapperClass(SecondSortMapper.class);
        job.setMapOutputKeyClass(TwoNum.class);
        job.setReducerClass(SecondSortReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}