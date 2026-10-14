import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
    // 停用词表
    private static final Set<String> STOP_WORDS = new HashSet<>();
    static {
        String[] sw = {"a","an","and","are","as","at","be","by","for","from","has","he","in","is","it",
            "its","of","on","that","the","to","was","were","will","with","i","you","we","they","this",
            "these","those","but","not","or","so","if","do","does","did","have","had","can","could",
            "would","should","may","might","must","than","then","there","here","when","where","which",
            "who","whom","what","why","how","all","any","both","each","few","more","most","other",
            "some","such","no","nor","only","own","same","too","very","just","into","about","up","out"};
        for (String s : sw) STOP_WORDS.add(s);
    }

    // Mapper：输出 单词 -> 文件名:1
    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text wordText = new Text();
        private Text docCount = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取当前读取的文件名
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString().toLowerCase();
            // 按非字母字符分割英文单词
            String[] words = line.split("[^a-z]+");
            for (String w : words) {
                if (w.isEmpty() || STOP_WORDS.contains(w)) continue;
                wordText.set(w);
                docCount.set(filename + ":1");
                context.write(wordText, docCount);
            }
        }
    }

    // Combiner：本地合并同一文档内同一个词的频次
    public static class IndexCombiner extends Reducer<Text, Text, Text, Text> {
        private Text outVal = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<>();
            for (Text v : values) {
                String[] parts = v.toString().split(":");
                int c = Integer.parseInt(parts[1]);
                map.put(parts[0], map.getOrDefault(parts[0], 0) + c);
            }
            for (Map.Entry<String, Integer> e : map.entrySet()) {
                outVal.set(e.getKey() + ":" + e.getValue());
                context.write(key, outVal);
            }
        }
    }

    // Reducer：汇总单词在各个文档出现频次 + 总频次
    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<>();
            int total = 0;
            for (Text v : values) {
                String[] parts = v.toString().split(":");
                int c = Integer.parseInt(parts[1]);
                map.put(parts[0], map.getOrDefault(parts[0], 0) + c);
                total += c;
            }
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> e : map.entrySet()) {
                sb.append("<").append(e.getKey()).append(",").append(e.getValue()).append(">;");
            }
            sb.append("<total,").append(total).append(">.");
            result.set(sb.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted_index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setCombinerClass(IndexCombiner.class);
        job.setReducerClass(IndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/exp2/input"));
        FileOutputFormat.setOutputPath(job, new Path("/exp2/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
