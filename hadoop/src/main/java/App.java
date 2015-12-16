import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import step1.RankMapper;
import step1.RankReducer;
import step2.ResultMapper;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Created by Maxime on 16/12/2015.
 */
public class App {

    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        App app = new App();

        String lastResultPath = null;

        for (int runs = 0; runs < 5; runs++) {
            String inPath = null;
            if(runs == 0)
                inPath = args[0];
            else
                inPath = args[1]+"/iter" + nf.format(runs);
            lastResultPath = args[1]+"/iter" + nf.format(runs + 1);

            app.runPageRank(inPath, lastResultPath);
        }
        app.rankOrdering(lastResultPath, args[1]+"/result");
    }

    private boolean runPageRank(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(true);
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        Job runPageRank = Job.getInstance(conf, "runPageRank");
        runPageRank.setJarByClass(App.class);

        runPageRank.setOutputKeyClass(Text.class);
        runPageRank.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(runPageRank, new Path(inputPath));
        runPageRank.setInputFormatClass(KeyValueTextInputFormat.class);

        FileOutputFormat.setOutputPath(runPageRank, new Path(outputPath));
        runPageRank.setOutputFormatClass(TextOutputFormat.class);

        runPageRank.setMapperClass(RankMapper.class);
        runPageRank.setReducerClass(RankReducer.class);

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputPath)))
            hdfs.delete(new Path(outputPath), true);

        return runPageRank.waitForCompletion(true);
    }

    public boolean rankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        // Create configuration
        Configuration conf = new Configuration(true);
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        // Create job
        Job rankOrdering = Job.getInstance(conf, "rankOrdering");
        rankOrdering.setJarByClass(App.class);

        rankOrdering.setOutputKeyClass(FloatWritable.class);
        rankOrdering.setOutputValueClass(Text.class);

        rankOrdering.setMapperClass(ResultMapper.class);

        FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

        rankOrdering.setInputFormatClass(KeyValueTextInputFormat.class);
        rankOrdering.setOutputFormatClass(TextOutputFormat.class);

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputPath)))
            hdfs.delete(new Path(outputPath), true);

        return rankOrdering.waitForCompletion(true);
    }

}
