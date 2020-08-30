import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;

public class Hits extends Configured implements Tool {
    public static class HitsMapper extends Mapper<LongWritable, Text, Text, Text>{
        private int iter;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            iter = Integer.parseInt(conf.get("curIteration"));
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String[] valParts = value.toString().split("\t");
            long aut;
            long hub;
            if (iter == 0 && !valParts[0].equals("-1")){ ///new graph
                aut = 1;
                hub = 1;
                if (!valParts[1].equals("N")) {
                    String[] input = valParts[1].split(",");
                    for (String inNode : input) {
                        context.write(new Text(inNode), new Text("H:" + Long.toString(aut)));
                    }
                }
                if (valParts.length > 2 && !valParts[2].equals("N")) {
                    String[] output = valParts[2].split(",");
                    for (String outNode : output) {
                        context.write(new Text(outNode), new Text("A:" + Long.toString(hub)));
                    }
                }
                if (valParts.length > 2 && !valParts[4].equals("N")){
                    String[] outLeafs = valParts[4].split(",");
                    for (String outLeaf : outLeafs) {
                        context.write(new Text("U:"+outLeaf), new Text("A:" + Long.toString(hub)));
                    }
                }
                if (valParts.length > 2) {
                    context.write(new Text(valParts[0]),
                            new Text(valParts[1] + "\t" + valParts[2] + "\t" + valParts[4]));
                } else{
                    context.write(new Text(valParts[0]), new Text(valParts[1]));
                }

            } else if (!valParts[0].equals("-1")){
                aut = Long.parseLong(valParts[1]);
                hub = Long.parseLong(valParts[2]);
                if (valParts.length > 3 &&!valParts[3].equals("N")) {
                    String[] input = valParts[3].split(",");
                    for (String inNode : input) {
                        context.write(new Text(inNode), new Text("H:" + Long.toString(aut)));
                    }
                }
                if (valParts.length > 4 && !valParts[4].equals("N")) {
                    String[] output = valParts[4].split(",");
                    for (String outNode : output) {
                        context.write(new Text(outNode), new Text("A:" + Long.toString(hub)));
                    }
                }
                if (valParts.length > 4 && !valParts[5].equals("N")){
                    String[] outLeafs = valParts[5].split(",");
                    for (String outLeaf : outLeafs) {
                        context.write(new Text("U:"+outLeaf), new Text("A:" + Long.toString(hub)));
                    }
                }
                if (valParts.length > 4) {
                    context.write(new Text(valParts[0]),
                            new Text(valParts[3] + "\t" + valParts[4] + "\t" + valParts[5]));
                }else if(valParts.length > 3){
                    context.write(new Text(valParts[0]), new Text(valParts[3]));
                }
            }
        }
    }


    public static class HitsReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long hub = 0;
            long aut = 0;
            String node = "";
            for (Text val:values){
                String valSt = val.toString();
                if (valSt.startsWith("H:")) {
                    hub += Long.parseLong(valSt.substring(2, valSt.length()));
                } else if(valSt.startsWith("A:")){
                    aut += Long.parseLong(valSt.substring(2, valSt.length()));
                } else {
                    node = valSt;
                }
            }
            context.write(new Text(key), new Text(Long.toString(aut)+"\t"+Long.toString(hub)+"\t"+node));
        }
    }

    private int maxIter = 5;

    private Job GetJobConf(Configuration conf, String inputPath, String outputPath) throws IOException{
        Job job = Job.getInstance(conf);
        job.setJarByClass(Hits.class);
        job.setJobName(Hits.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(HitsMapper.class);
        job.setReducerClass(HitsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(50);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;

    }

    public int run(String[] args) throws Exception{
        for (int i = 0; i < maxIter; i++){
            Job job;
            if (i ==0 ){
                job = GetJobConf(getConf(), args[0], args[1]+Integer.toString(i));
            } else{
                job = GetJobConf(getConf(), args[1]+Integer.toString(i-1)+"/part-*",
                        args[1]+Integer.toString(i));
            }
            Configuration conf = job.getConfiguration();
            conf.set("curIteration", Integer.toString(i));

            if(!job.waitForCompletion(true)){
                return 1;
            }

        }
        return 0;
    }

    public static void main(String[] args) throws Exception{
        int rc = ToolRunner.run(new Hits(), args);
        System.exit(rc);
    }
}
