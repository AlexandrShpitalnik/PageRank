import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;


public class SortJob extends Configured implements Tool{


    public static class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        private int SORT_VAL;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            SORT_VAL = Integer.parseInt(conf.get("cur"));
            //nPages = Integer.parseInt(conf.get("nPages"));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String valSt = value.toString();
            String[] valParts = valSt.split("\t");
            Double val = 0.0;
            switch (SORT_VAL) {
                case 0:
                    String valS = valParts[1];
                    val = Double.parseDouble(valS);
                    break;
                case 1:
                    valS = valParts[1];
                    val = (double) Long.parseLong(valS);
                    break;
                case 2:
                    valS = valParts[2];
                    val = (double) Long.parseLong(valS);
                    break;
            }
            context.write(new DoubleWritable(val), new Text(valParts[0]));
        }
    }
    public static class DoubleComparator extends WritableComparator {

        public DoubleComparator() {
            super(DoubleWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1,
                           byte[] b2, int s2, int l2) {

            Double v1 = ByteBuffer.wrap(b1, s1, l1).getDouble();
            Double v2 = ByteBuffer.wrap(b2, s2, l2).getDouble();

            return v1.compareTo(v2) * (-1);
        }
    }




    public static class SortReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text>
    {
        private HashMap<Integer, String> idToUrl = new HashMap<>();
        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
            String line;
            Configuration conf = context.getConfiguration();
            Path toUrlIdx = new Path(conf.get("urlIdxPath"));
            FileSystem fs = toUrlIdx.getFileSystem(conf);
            FSDataInputStream urlIdxStream = fs.open(toUrlIdx);
            BufferedReader urlIdxBr = new BufferedReader(new InputStreamReader(urlIdxStream));
            while ((line = urlIdxBr.readLine()) != null) {
                String[] idUrl = line.split("\t");
                int id = Integer.parseInt(idUrl[0]);
                String url;
                if (idUrl[1].endsWith("/")) {
                    url = idUrl[1].substring(0, idUrl[1].length() - 1);
                } else{
                    url = idUrl[1];
                }
                idToUrl.put(id, url);
            }
            urlIdxStream.close();
        }



        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text v : values)
            {
                String vSt = v.toString();
                if(vSt.startsWith("U")) {
                    context.write(key, new Text(vSt.substring(2, vSt.length())));
                } else if(!vSt.equals("-1")){
                    String url = idToUrl.get(Integer.parseInt(vSt));
                    context.write(key, new Text(url));
                }
            }
        }
    }



    @Override
    public int run(String[] args) throws Exception
    {
        int res = 1;
        String res_path = args[1];
        Job job;
        if (args[3].equals("PR")) {
            //SORT_VAL = SortingVal.PR;
            job = GetJobConf(getConf(), args[0], res_path + "pr/");
            Configuration conf = job.getConfiguration();
            conf.set("cur", Integer.toString(0));
            conf.set("urlIdxPath", args[2]);
            res = job.waitForCompletion(true) ? 0 : 1;
        } else if (args[3].equals("A")) {

            //SORT_VAL = SortingVal.A;
            job = GetJobConf(getConf(), args[0], res_path + "hits_a/");
            Configuration conf = job.getConfiguration();
            conf.set("cur", Integer.toString(1));
            conf.set("urlIdxPath", args[2]);
            res = job.waitForCompletion(true) ? 0 : 1;
        } else if ((args[3].equals("H"))) {

            //SORT_VAL = SortingVal.H;
            job = GetJobConf(getConf(), args[0], res_path + "hits_h/");
            Configuration conf = job.getConfiguration();
            conf.set("cur", Integer.toString(2));
            conf.set("urlIdxPath", args[2]);
            res = job.waitForCompletion(true) ? 0 : 1;
        }

        return res;
    }


    private Job GetJobConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(SortJob.class);
        job.setJobName(SortJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        FileInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setNumReduceTasks(1);
        job.setSortComparatorClass(DoubleComparator.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }


    public static void main(String[] args) throws Exception {


        int exitCode = ToolRunner.run(new SortJob(), args);
        System.exit(exitCode);
    }

}
