import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @Author Jwei
 * @Date 2020/6/3 17:21
 */
public class WordCountMapper
        extends Mapper<Object, Text, Text, LongWritable> {

    private final static LongWritable ONE = new LongWritable(1L);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String[] strs = value.toString().split("\t");
        if (strs.length != 2) return;
        StringTokenizer itr = new StringTokenizer(strs[1].toString());
        while (itr.hasMoreTokens()) {
            word.set(strs[0] + "_" + itr.nextToken());
            context.write(word, ONE);
        }
    }
}
