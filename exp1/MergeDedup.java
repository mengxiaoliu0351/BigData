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
import java.io.IOException;

public class MergeDedup {
    public static class MapTask extends Mapper<LongWritable, Text, Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //清理多余空格，保证相同内容匹配
            String line = value.toString().trim().replaceAll("\\s+"," ");
            context.write(new Text(line), NullWritable.get());
        }
    }

    public static class ReduceTask extends Reducer<Text, NullWritable, Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"merge_dedup");
        job.setJarByClass(MergeDedup.class);
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job,new Path("/mr/input1"));
        FileOutputFormat.setOutputPath(job,new Path("/mr/out1"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

