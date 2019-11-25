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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

public class InvertedIndex {

    public static class InvertedIndexMapper
            extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        // default RecordReader: LineRecordReader; key: line offset; value: line string
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            fileName = fileName.substring(0, fileName.length()-14);

            Text word_filename = new Text();
            Text count = new Text("1");
            StringTokenizer itr = new StringTokenizer(value.toString());
            for(; itr.hasMoreTokens(); )
            {
                word_filename.set(itr.nextToken()+"#"+fileName);
                context.write(word_filename, count);
            }
        }
    }

    public static class InvertedIndexCombiner
            extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }

            Text count = new Text(String.valueOf(sum));
            context.write(key, count);
        }
    }

    public static class InvertedIndexPartitioner
        extends HashPartitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            String word = key.toString().split("#")[0]; //<term, docid> =>term
            Text term = new Text(word);
            return super.getPartition(term, value, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text prevWord, word;
        private Map<Text, Integer> wordCount;
        private StringBuilder all;
        private double totalCount;
        private double numOfBooks;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            prevWord = new Text();
            wordCount = new HashMap<Text, Integer>();
            all = new StringBuilder();
            totalCount = 0.0;
            numOfBooks = 0.0;
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException{
            all.append(totalCount / numOfBooks);
            for (Text eachFile : wordCount.keySet()) {
                all.append(";");
                all.append(eachFile);
                all.append(":");
                all.append(wordCount.get(eachFile));
            }
            context.write(prevWord, new Text(all.toString()));
            wordCount.clear();
            all.delete(0, all.length());
            totalCount = 0.0;
            numOfBooks = 0.0;
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            word = new Text(key.toString().split("#")[0]);
            Text filename = new Text(key.toString().split("#")[1]);

            Iterator<Text> it = values.iterator();
            if (!word.equals(prevWord) && !prevWord.equals(new Text())) {
                all.append(totalCount / numOfBooks);
                for (Text eachFile : wordCount.keySet()) {
                    all.append(";");
                    all.append(eachFile);
                    all.append(":");
                    all.append(wordCount.get(eachFile));
                }
                context.write(prevWord, new Text(all.toString()));
                wordCount.clear();
                all.delete(0, all.length());
                totalCount = 0.0;
                numOfBooks = 0.0;
            }
            for(; it.hasNext(); ) {
                if (wordCount.get(filename) == null) {
                    wordCount.put(filename, 0);
                    numOfBooks++;
                }

                int value = Integer.parseInt(it.next().toString());
                wordCount.put(filename, wordCount.get(filename) + value);
                totalCount += value;
            }
            prevWord = word;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setPartitionerClass(InvertedIndexPartitioner.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
