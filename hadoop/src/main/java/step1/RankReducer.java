package step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Maxime on 16/12/2015.
 */
public class RankReducer extends Reducer<Text,Text,Text,Text> {

    private static final double damping = 0.85D;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isExistingWikiPage = false;
        String[] split;
        float sumShareOtherPageRanks = 0;
        String links = "";
        String pageWithRank;

        while(values.iterator().hasNext()){
            pageWithRank = values.iterator().next().toString();

            System.out.println(pageWithRank);

            if(pageWithRank.startsWith("!")) {
                isExistingWikiPage = true;
                links = pageWithRank.substring(1);
            } else {
                split = pageWithRank.split("\t");

                float pageRank = Float.valueOf(split[1]);
                int countOutLinks = Integer.valueOf(split[2]);

                sumShareOtherPageRanks += (pageRank / countOutLinks);
            }
        }

        if(!isExistingWikiPage)
            return;

        double newRank = damping * sumShareOtherPageRanks + (1-damping);
        context.write(key, new Text(newRank + "\t" + links));
    }

}