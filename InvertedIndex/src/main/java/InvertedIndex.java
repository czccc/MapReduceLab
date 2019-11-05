import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class InvertedIndex {

    public static class InvertedIndexMapper
            extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException
        // default RecordReader: LineRecordReader; key: line offset; value: line string
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            Text word = new Text();
            Text fileName_lineOffset = new Text(fileName+"#"+key.toString());
            StringTokenizer itr = new StringTokenizer(value.toString());
            for(; itr.hasMoreTokens(); )
            { word.set(itr.nextToken());
                context.write(word, fileName_lineOffset);
            }
        }
    }

    public static class NewPartitioner
        extends HashPartitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            String term = key.toString().split(",")[0]; //<term, docid>=>term
            super.getPartition(term, value, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer
            extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            Iterator<Text> it = values.iterator();
            StringBuilder all = new StringBuilder();
            if(it.hasNext()) all.append(it.next().toString());
            for(; it.hasNext(); )
            { all.append(";");
                all.append(it.next().toString());
            }
            context.write(key, new Text(all.toString()));
        } //最终输出键值对示例：(“fish", “doc1#0; doc1#8;doc2#0;doc2#8 ")
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(InvertedIndexMapper.class);
        Job.setPartitionerClass(NewPartitioner.class);
        // job.setCombinerClass(InvertedIndexReducer.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
