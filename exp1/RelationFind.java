import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RelationFind {
    public static class MapTask extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString().trim();
            //跳过表头、空行
            if(line.startsWith("child") || line.isEmpty()) return;
            String[] arr=line.split("\\s+");
            //新增判断：必须至少2个字段才处理
            if(arr.length < 2) return;
            String child=arr[0];
            String parent=arr[1];
            context.write(new Text(child),new Text("P:"+parent));
            context.write(new Text(parent),new Text("C:"+child));
        }
    }

    public static class ReduceTask extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> children=new ArrayList<>();
            List<String> parents=new ArrayList<>();
            for(Text v:values){
                String s=v.toString();
                if(s.startsWith("C:")){
                    children.add(s.substring(2));
                }else if(s.startsWith("P:")){
                    parents.add(s.substring(2));
                }
            }
            for(String c:children){
                for(String p:parents){
                    context.write(new Text(c),new Text(p));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"relation_find");
        job.setJarByClass(RelationFind.class);
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path("/mr/input3"));
        FileOutputFormat.setOutputPath(job,new Path("/mr/out3"));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

