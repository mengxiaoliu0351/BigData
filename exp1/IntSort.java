import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class IntSort {
    public static class MapTask extends Mapper<LongWritable,Text, IntWritable,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString().trim();
            if(line.length()==0) return;
            int num=Integer.parseInt(line);
            context.write(new IntWritable(num),new IntWritable(num));
        }
    }

    public static class ReduceTask extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
        private int index=1;
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for(IntWritable v:values){
                context.write(new IntWritable(index++),key);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"int_sort");
        job.setJarByClass(IntSort.class);
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job,new Path("/mr/input2"));
        FileOutputFormat.setOutputPath(job,new Path("/mr/out2"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

