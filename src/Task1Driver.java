
import java.io.IOException;

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
public class Task1Driver{
    public static class RelationMapper extends Mapper<LongWritable, Text, Text, Text>{//输入，输出键值对
        private final Text source = new Text();
        //初始化
        @Override
        protected void setup(Context context) throws IOException{
            // 获取当前输入分片，转换为 FileSplit
            // 再取得它所属文件的路径和文件名
            String name =((FileSplit) context.getInputSplit()).getPath().getName();
            if("A.txt".equals(name)){
                source.set("A");
            }//分片来自A
            else if("B.txt".equals(name)){
                source.set("B");
            }//分片来自B
            else{
                throw new IOException("未知输入文件"+name);
            }
        }
        @Override 
        protected void map(LongWritable key,Text value ,Context context) throws IOException, InterruptedException{//key：字节偏移量，value:文本
            String line=value.toString().trim();// 将Text 转换为 String，并去掉行首行尾空白。
            if(line.isEmpty()){
                return;
            }//空串返回
            String[] fields = line.split("\\s+");//按照空白拆分
            if (fields.length != 2) {
                throw new IOException("输入行应包含两列：" + line);
            }//每一行应该是两列：0是指数编号，1是股票代码。

            Text stockKey = new Text(fields[0]+"\t"+fields[1]);//构建输出key，变成同一指数同一股票有相同的键
            context.write(stockKey,source);//输出键值对
        }
        //将 Mapper 输出按键排序、分组。相同键的来源会交给同一次 reduce。 
    }//Mapper
    public static class RelationReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
            boolean inA =false,inB =false;
            for(Text value:values){//遍历来源标记
                String source =value.toString();
                if("A".equals(source)){
                    inA=true;
                }
                else if("B".equals(source)){
                    inB=true;
                }//出现过A,B被记录
                
            } 
            String relation;
            if(inA && inB){
                relation="BOTH";
            }
            else if(inA){
                relation="A_ONLY";
            }
            else {
                relation="B_ONLY";
            }
            context.write(key,new Text(relation));
        }
    }//Reducer
    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "参数：<HDFS输入目录> <HDFS输出目录>");
        }
        Job job =Job.getInstance(new Configuration(), "Homework5-Task1");
        job.setJarByClass(Task1Driver.class);//获取代码
        job.setMapperClass(RelationMapper.class);//Mapper类
        job.setReducerClass(RelationReducer.class);//Reducer类
        job.setOutputKeyClass(Text.class);//输出键类
        job.setOutputValueClass(Text.class);//输出值类
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//命令行输入作为参数
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }//Main
}