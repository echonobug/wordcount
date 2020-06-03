import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author Jwei
 * @Date 2020/6/3 17:22
 */
public class WordCountReducer
        extends Reducer<Text, LongWritable, WcWritable, NullWritable> {
    NullWritable n = NullWritable.get();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context) throws IOException, InterruptedException {
        long sum = 0L;
        for (LongWritable value : values) {
            sum += value.get();
        }
        String[] s = key.toString().split("_");
        context.write(new WcWritable(s[1], Integer.valueOf(s[0]), sum), n);
    }
}
