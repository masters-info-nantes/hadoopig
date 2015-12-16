package step2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Maxime on 16/12/2015.
 */
public class ResultMapper extends Mapper<Text, Text, FloatWritable, Text> {

    @Override
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        int splitPos = value.toString().indexOf("\t");
        float pageRank = Float.parseFloat(value.toString().substring(0, splitPos));
        context.write(new FloatWritable(pageRank), key);
    }

}
