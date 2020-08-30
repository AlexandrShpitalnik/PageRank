import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PageRank extends Configured implements Tool {


    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        private int iter;
        private int nPages;
        private double prFromLeaf;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            iter = Integer.parseInt(conf.get("curIteration"));
            nPages = Integer.parseInt(conf.get("nPages"));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String valSt = value.toString();
            String[] valParts = valSt.split("\t");
            double pageRank;
            if (valParts[0].equals("-1")) {
                int nLeafs;
                if (iter == 0) {
                    nLeafs = Integer.parseInt(valParts[1]);
                    pageRank = 1.0 * nLeafs;
                } else {
                    nLeafs = Integer.parseInt(valParts[2]);
                    pageRank = Double.parseDouble(valParts[1]);
                }
                //value which adds to each not leaf node
                prFromLeaf = pageRank / nPages; //nPages + nLeafs
                long n_reducers = context.getConfiguration().getLong(MRJobConfig.NUM_REDUCES, 1);
                for (int i = 0; i < n_reducers; i++) {
                    context.write(new Text("-1:" + Integer.toString(i)), new Text(Double.toString(prFromLeaf)));
                }
                context.write(new Text("-1"), new Text("N\t"+Integer.toString(nLeafs)));

            } else if (!valParts[0].startsWith("U")){
                if (iter == 0) {
                    pageRank = 1.0;
                } else {
                    pageRank = Double.parseDouble(valParts[1]);
                }
                int nLeaf = Integer.parseInt(valParts[3]);
                String[] neighbours =  valParts[2].split(",");
                int nLinks = nLeaf;
                double forEach;

                if (!valParts[2].equals("N")) {
                    nLinks += neighbours.length;
                    forEach = pageRank / nLinks;
                    for (String nb : neighbours) {
                        context.write(new Text(nb), new Text("P:" + Double.toString(forEach)));
                    }
                }
                else{
                    nLinks = nLeaf;
                    forEach = pageRank / nLinks;
                }
                context.write(new Text(valParts[0]), new Text(valParts[2] + "\t" + valParts[3]));

                if(nLinks != 0) {
                    double forLeafs = forEach * nLeaf;
                    context.write(new Text("-1"), new Text(Double.toString(forLeafs)));
                } else{
                    context.write(new Text("-1"), new Text(Double.toString(pageRank)));
                }
            }
        }
    }


    public static class PageRankPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text val, int numPartitions) {
            String key_st = key.toString();
            if (key_st.startsWith("-1:")) {
                String[] key_p = key.toString().split(":");
                int v = Integer.valueOf(key_p[1]);
                return v;
            }
            else {
                return Math.abs(key.hashCode()) % numPartitions;
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {

        //Sprivate int iter;
        private int nPages;
        private double prFromLeaf = 0;
        private double alpha = 0.15;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            nPages = Integer.parseInt(conf.get("nPages"));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double pageRank = 0;
            String keySt = key.toString();
            if (keySt.equals("-1")){
                int nLeaf = 0;
                for (Text val:values){
                    String valSt = val.toString();
                    if (valSt.startsWith("N")){
                        nLeaf += Integer.parseInt(valSt.substring(2, valSt.length()));
                    } else{
                        pageRank += Double.parseDouble(valSt); // teleportation???
                    }
                }
                context.write(new Text("-1"), new Text(Double.toString(pageRank)+"\t"+Integer.toString(nLeaf)));
            }else if (keySt.startsWith("-1")) {
                for (Text val : values) {
                    prFromLeaf += Double.parseDouble(val.toString());
                }
            }else{
                String nodeConfig = "";
                for (Text val:values){
                    String valSt = val.toString();
                    if (valSt.startsWith("P")){
                        pageRank += Double.parseDouble(valSt.substring(2, valSt.length()));
                    } else {
                        nodeConfig = valSt;
                    }
                }
                pageRank += prFromLeaf;
                pageRank = alpha*(1.0/nPages) + (1-alpha)*pageRank;


                context.write(new Text(key), new Text(Double.toString(pageRank)+"\t"+nodeConfig));
            }
        }
    }




    private Job GetJobConf(Configuration conf, String input, String out_dir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(PageRank.class);
        job.setJobName(PageRank.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        job.setMapperClass(PageRank.PageRankMapper.class);
        job.setReducerClass(PageRank.PageRankReducer.class);

        job.setPartitionerClass(PageRankPartitioner.class);
        job.setSortComparatorClass(Text.Comparator.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(50);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    private int maxIter = 10;

    public int run(String args[]) throws Exception{
        Job job;

        for (int i=0; i<maxIter; i++){
            if(i == 0){
                job = GetJobConf(getConf(), args[0], args[1]+Integer.toString(i));
            } else {
                job = GetJobConf(getConf(), args[1]+Integer.toString(i-1)+"/part-*",
                        args[1]+Integer.toString(i));
            }

            Configuration conf = job.getConfiguration();
            conf.set("nPages", "564549");
            conf.set("curIteration", Integer.toString(i));

            if (!job.waitForCompletion(true)){
                return 1;
            }
        }
        return 0;
    }


    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new PageRank(), args);
        System.exit(rc);
    }
}
