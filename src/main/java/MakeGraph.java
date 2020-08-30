import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.commons.lang.ObjectUtils;
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

public class MakeGraph extends Configured implements Tool {


    public static class GraphMapper extends Mapper<LongWritable, Text, Text, Text> {
        private HashMap<Integer, String> idToUrl = new HashMap<>();
        private HashMap<String, Integer> urlToId = new HashMap<>();
        static final String linkRegex = "<a[^>]+href=[\"']?([^\"'\\s>]+)[\"']?[^>]*>";
        static final String urlRegex = "\"[^\"]*/[^\"]*\"";
        int mapperLeaf = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
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
                urlToId.put(url, id);
            }
            urlIdxStream.close();
        }


        public static String decode(String encoded) throws IOException{
            byte[] doc_decoded = Base64.getDecoder().decode(encoded);

            Inflater infl = new Inflater();
            ByteArrayOutputStream byte_out = new ByteArrayOutputStream();
            byte[] defl_buf = new byte[1024];
            infl.setInput(doc_decoded);
            try {
                for (; ; ) {
                    int res_size = infl.inflate(defl_buf);
                    if (res_size == 0){
                        break;
                    }
                    byte_out.write(defl_buf);
                }
            } catch(DataFormatException e){}
            String doc_string = byte_out.toString();
            return doc_string;
        }


        public static ArrayList<String> getUrls(String html){
            Pattern linkPattern = Pattern.compile(linkRegex, Pattern.CASE_INSENSITIVE);
            Pattern urlPattern = Pattern.compile(urlRegex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
            Matcher matcher = linkPattern.matcher(html);
            ArrayList<String> res = new ArrayList<>();
            while (matcher.find()) {
                String regSt = html.substring(matcher.start(), matcher.end());
                Matcher inRegMatcher = urlPattern.matcher(regSt);
                String url = "";
                if (inRegMatcher.find()){
                    String unnorm_url = regSt.substring(inRegMatcher.start()+1, inRegMatcher.end()-1);
                    if (unnorm_url.charAt(0) == '/') {

                        url = "http://lenta.ru" + unnorm_url;
                    } else{
                        try {
                            unnorm_url = unnorm_url.replace("www.", "");
                            URI uri = new URI(unnorm_url);
                            url = "http://" + uri.getAuthority() + uri.getPath(); //add query sort (no null)
                        }  catch (URISyntaxException e){ }
                    }
                    if (url.endsWith("/")){
                        url = url.substring(0, url.length() -1);
                    }
                    res.add(url);
                    //System.out.print(url + '\n');
                } else {
                    //System.out.print("url.err \n");
                }
                //System.out.print(regSt+ '\n');
            }
            return res;
        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] idDoc = value.toString().split("\t");
            String decoded = GraphMapper.decode(idDoc[1]);
            ArrayList<String> urls = GraphMapper.getUrls(decoded);
            HashMap<String, Integer> isParsedBefore = new HashMap<>();
            int pageId = Integer.parseInt(idDoc[0]);
            int pageLeafs = 0;
            StringBuilder out = new StringBuilder();
            StringBuilder outLeafs = new StringBuilder();
            for (String url : urls) {
                if (!isParsedBefore.containsKey(url)) {
                    isParsedBefore.put(url, 1);
                    if (urlToId.containsKey(url)) {
                        int linkId = urlToId.get(url);
                        if (linkId != pageId) {
                            //context.write(new LongWritable(pageId), new Text("O:" + Integer.toString(linkId)));
                            //context.write(new LongWritable(linkId), new Text("I:"+ Integer.toString(pageId)));
                            //context.write(new Text(idDoc[0]), new Text("O:" + Integer.toString(linkId)));
                            out.append(Integer.toString(linkId));
                            out.append(',');
                            context.write(new Text(Integer.toString(linkId)), new Text("I:" + idDoc[0]));
                        }
                    } else {
                        pageLeafs += 1;
                        context.write(new Text("U:" + url), new Text(idDoc[0]));
                        outLeafs.append(url);
                        outLeafs.append(',');
                    }
                }
            }

            if(out.length()>0){
                String outSt = out.substring(0, out.length()-1);
                context.write(new Text(idDoc[0]), new Text("O:" + outSt));
            }

            if(outLeafs.length()>0){
                String outLeafsSt = outLeafs.substring(0, outLeafs.length()-1);
                context.write(new Text(idDoc[0]), new Text("T:" + outLeafsSt));
            }


            mapperLeaf += pageLeafs;
            //context.write(new LongWritable(pageId), new Text("L:" + Integer.toString(pageLeafs)));
            context.write(new Text(idDoc[0]), new Text("L:" + Integer.toString(pageLeafs)));
        }

        //@Override
        //protected void cleanup(Context context) throws IOException, InterruptedException {
        //    long n_reducers = context.getConfiguration().getLong(MRJobConfig.NUM_REDUCES, 1);
        //    for (int i = 0; i<n_reducers; i++){
        //        context.write(new Text("A:"+Integer.toString(i)), new IntText(mapperLeaf));
        //    }
        //}
    }

    public static class GraphReducer extends Reducer<Text, Text, Text, Text> {
        public int leafNum = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String keySt = key.toString();
            if (keySt.startsWith("U")) {
                StringBuilder in = new StringBuilder();
                for (Text val:values){
                    in.append(val.toString());
                    in.append(',');
                }
                String inSt = in.toString();
                context.write(new Text(keySt), new Text(inSt.substring(0, inSt.length()-1)));
                leafNum += 1;
            } else {
                StringBuilder in = new StringBuilder();
                String outLeafs = "N";
                String out = "N";
                int leafs = 0;
                for (Text val : values) {
                    String valSt = val.toString();
                    switch (valSt.charAt(0)) {
                        case 'O':
                            out = valSt.substring(2, valSt.length());
                            break;
                        case 'T':
                            outLeafs = valSt.substring(2, valSt.length());
                            break;
                        case 'I':
                            in.append(valSt.subSequence(2, valSt.length()));
                            in.append(',');
                            break;
                        case 'L':
                            String[] parts = valSt.split(":");
                            leafs = Integer.parseInt(parts[1]);
                    }
                }
                String node = "";
                if (in.length() > 0) {
                    node += in.substring(0, in.length() - 1);
                } else {
                    node += "N";
                }
                node += "\t";
                node += out;
                node += "\t";
                node += Integer.toString(leafs);
                node += "\t";
                node += outLeafs;

                context.write(key, new Text(node));
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            //System.out.print(Integer.toString(leafNum) + "\n");
            context.write(new Text("-1"), new Text(Integer.toString(leafNum)));
            //Configuration conf = context.getConfiguration();
            //Path toUrlIdx = new Path(conf.get("toLeafNum"));
            //FileSystem fs = toUrlIdx.getFileSystem(conf);
            //FSDataOutputStream urlIdxStream = fs.open(toUrlIdx);
        }

    }


    @Override
    public int run(String[] args) throws Exception{
        Job job = GetJobConf(getConf(), args[0], args[1]);

        Configuration conf = job.getConfiguration();
        conf.set("urlIdxPath", args[2]);
        conf.set("toLeafNum", args[1]+"/LeafNum");

        return job.waitForCompletion(true) ? 0 : 1;
    }


    private Job GetJobConf(Configuration conf, String input, String out_dir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(MakeGraph.class);
        job.setJobName(MakeGraph.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        job.setMapperClass(GraphMapper.class);
        job.setReducerClass(GraphReducer.class);
        job.setNumReduceTasks(50);

        //job.setPartitionerClass(GraphPartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new MakeGraph(), args);
        System.exit(rc);
    }



}
