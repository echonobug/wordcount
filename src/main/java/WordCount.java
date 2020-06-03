import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

/**
 * @Author Jwei
 * @Date 2020/6/3 15:01
 */
public class WordCount {

    private static final String hdfs = "hdfs://jwei.fun:9000";
    private static final String inFile = "/ca/comment.txt";
    private static final String driverPath = "/ca/mysqlDriver/mysql-connector-java-8.0.19.jar";
    private static final String url = "jdbc:mysql://123.56.125.48:3306/commentanalysis?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true";
    private static final String driver = "com.mysql.cj.jdbc.Driver";
    private static final String user = "mrli";
    private static final String passwd = "Liqi@233526";

    private static final String[] WCFieldNames = {"word", "type", "count"};

    public static void main(String[] args) {
        try {
            run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static int run() throws Exception {

        Configuration conf = new Configuration();
        DBConfiguration.configureDB(conf, driver, url, user, passwd);
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(WordCountReducer.class);

        FileInputFormat.addInputPath(job, new Path(hdfs + inFile));

        DBOutputFormat.setOutput(job, "word_count", WCFieldNames);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(WcWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.addArchiveToClassPath(new Path(hdfs + driverPath));

        int ret = job.waitForCompletion(true) ? 0 : 1;
        return ret;
    }

}
