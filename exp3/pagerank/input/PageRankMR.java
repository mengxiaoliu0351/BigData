import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageRankMR {
    private static final double D = 0.85;
    private static final int ITER_NUM = 10;
    private static final String BASE_OUT = "/pagerank/mr_out";

    public static class PRMap extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            String page = parts[0];
            String prStr = parts[1];
            String[] links;
            double pr;
            try{
                pr = Double.parseDouble(prStr);
                links=new String[0];
            }catch (NumberFormatException e){
                String[] tmp=prStr.split(",");
                links=tmp;
                pr=1.0;
            }
            StringBuilder sb=new StringBuilder();
            for(String l:links){
                sb.append(l).append(",");
            }
            if(sb.length()>0) sb.setLength(sb.length()-1);
            context.write(new Text(page),new Text("|"+sb));
            int outCnt=links.length;
            if(outCnt>0){
                double share=pr/outCnt;
                for(String target:links){
                    context.write(new Text(target),new Text(String.valueOf(share)));
                }
            }
        }
    }

    public static class PRReduce extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String outLinkStr="";
            double sumShare=0.0;
            for(Text v:values){
                String s=v.toString();
                if(s.startsWith("|")){
                    outLinkStr=s.substring(1);
                }else{
                    sumShare += Double.parseDouble(s);
                }
            }
            double newPr = (1-D)+ D*sumShare;
            context.write(key,new Text(String.format("%.10f\t%s",newPr,outLinkStr)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        String inPath="/pagerank/input/page.txt";
        for(int i=1;i<=ITER_NUM;i++){
            String outPath=BASE_OUT+"_"+i;
            Job job= Job.getInstance(conf,"PageRank iter "+i);
            job.setJarByClass(PageRankMR.class);
            job.setMapperClass(PRMap.class);
            job.setReducerClass(PRReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job,new Path(inPath));
            FileOutputFormat.setOutputPath(job,new Path(outPath));
            job.waitForCompletion(true);
            inPath=outPath+"/part-r-00000";
        }
    }
}
