import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

import java.util.Vector;

public class MyJoin {
    public static class MyJoinMapper
            extends Mapper<Object, Text, Text, Text>{
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if(fileName.contains("order"))
            {
                String [] str_arr = value.toString().split("\\s+");
                Text product_id = new Text(str_arr[2]);
                Text order_info = new Text("a#"+str_arr[0]+' '+str_arr[1]+' '+str_arr[3]);
                context.write(product_id, order_info);
            }
            else
            {
                String [] str_arr = value.toString().split("\\s+");
                Text product_id = new Text(str_arr[0]);
                Text product_info = new Text("b#"+str_arr[1]+' '+str_arr[2]);
                context.write(product_id, product_info);
            }
        }
    }

    public static class MyJoinReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            Vector<String> product = new Vector<String>(); // 存放来自表A的值
            Vector<String> order = new Vector<String>(); // 存放来自表B的值
            for(Text val : values){
                String value = val.toString();
                if(value.startsWith("a")) {
                    order.add(value.substring(2));
                }
                else if(value.startsWith("b")){
                    product.add(value.substring(2));
                }
            }
            for(int i=0; i<order.size(); i++){
                String [] product_arr = product.get(0).split("\\s+");
                String [] order_arr = order.get(i).split("\\s+");
                context.write(new Text(order_arr[0]+"\t"+order_arr[1]), new Text(key.toString()+"\t"+
                        product_arr[0]+"\t"+product_arr[1]+"\t"+order_arr[2]));
            }

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sort");
        job.setJarByClass(MyJoin.class);
        job.setMapperClass(MyJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setReducerClass(MyJoinReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
