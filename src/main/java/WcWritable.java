import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author Jwei
 * @Date 2020/6/3 15:38
 */
public class WcWritable implements DBWritable, Writable {
    private String word;
    private int type;
    private Long count;

    public WcWritable(String word, int type, Long count) {
        this.word = word;
        this.type = type;
        this.count = count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(type);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        type = in.readInt();
        count = in.readLong();
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, word);
        preparedStatement.setInt(2, type);
        preparedStatement.setLong(3, count);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        word=resultSet.getString(1);
        type = resultSet.getInt(2);
        count = resultSet.getLong(3);
    }
}
