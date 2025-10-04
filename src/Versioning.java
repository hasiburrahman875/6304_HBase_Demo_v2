import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Cell;

public class Versioning {

    public static String Table_Name = "Twitter";

    public static void main(String[] argv) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(Table_Name))) {

            String row_key = "https://twitter.com/realDonaldTrump/status/651184379566227456";

            // initialize a put with row key
            Put put = new Put(Bytes.toBytes(row_key));

            // insert additional data
            put.addColumn(Bytes.toBytes("Tweets"), Bytes.toBytes("Type"), Bytes.toBytes("type1"));
            table.put(put);

            // initialize a get with max versions
            Get get = new Get(Bytes.toBytes(row_key));
            get.readAllVersions();
            get.setMaxVersions(3);

            Result result = table.get(get);

            // get all versions of "Tweets:Type"
            List<Cell> cells = result.getColumnCells(Bytes.toBytes("Tweets"), Bytes.toBytes("Type"));
            for (Cell cell : cells) {
                String value = Bytes.toString(
                        cell.getValueArray(),
                        cell.getValueOffset(),
                        cell.getValueLength()
                );
                System.out.println(value);
            }
        }
    }
}