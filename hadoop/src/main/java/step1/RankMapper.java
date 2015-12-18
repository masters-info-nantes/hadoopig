package step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Maxime on 16/12/2015.
 */

public class RankMapper extends Mapper<Text, Text, Text, Text> {

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String page = key.toString();
        int splitPos = value.toString().indexOf("\t");
        float pageRank = Float.parseFloat(value.toString().substring(0, splitPos));
        String links = value.toString().substring(splitPos + 1, value.toString().length());
        String[] linksTab = links.split(" ");

        Text currentPage = new Text(page+"\t"+pageRank+"\t"+linksTab.length);

        for(String link : linksTab) {
            context.write(new Text(link), currentPage);
            System.out.println(link.toString() + "\t" + currentPage.toString());
        }
        context.write(new Text(page), new Text("!"+links));
        System.out.println(page + "\t" + "!"+links);
    }

}
