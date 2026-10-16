import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Task2Driver
{
    /*
     * Mapper输入：任务一结果中的一行。
     * Mapper 输出：
     * key   = 指数编号，例如 101
     * value = 关系标记，例如 BOTH
     */
    public static class CountMapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context)throws  IOException, InterruptedException{//输入的格式是任务一的输出
            String line =value.toString().trim();
            if(line.isEmpty()){
                return;
            }
            String fields[]=line.split("\\s+");//分割
            if (fields.length != 3) {
                throw new IOException("任务一结果应包含三列：" + line);
            }
            context.write(new Text(fields[0]),new Text(fields[2]));//输出就是第一列和第三列

        }
    }
    public static class CountReducer extends Reducer<Text,Text,Text,Text>{
        @Override 
        protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException,InterruptedException{
            long both=0;
            long aonly=0;
            long bonly=0;
            for(Text value:values){
                switch(value.toString()){
                    case "BOTH"-> both++;
                    case "A_ONLY"-> aonly++;
                    case "B_ONLY"-> bonly++;
                    default -> throw new IOException(
                            "未知关系：" + value.toString());
                }
            }
            long union = both+aonly+bonly;
            Text result = new Text(
                    both + "\t" + aonly + "\t" + bonly + "\t" + union
            );//构建输出（四项）
            context.write(key,result);
        }
    }
    public static void main(String[]args)throws Exception{
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "参数：<任务一HDFS结果目录> <任务二HDFS输出目录>");
        }
        Job job = Job.getInstance(new Configuration(), "Homework5-Task2");
        job.setJarByClass(Task2Driver.class);
        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}