import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class RetweetsFilter {

    public static String Table_Name = "Twitter";

    public static void main(String[] argv) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(Table_Name))) {

            // define the filter (retweets <= 821)
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("Info"),
                    Bytes.toBytes("Retweets"),
                    CompareOp.LESS_OR_EQUAL,
                    new BinaryComparator(Bytes.toBytes(821))
            );

            Scan scan = new Scan();
            scan.setFilter(filter);

            // extract the result
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    int retweets = Bytes.toInt(result.getValue(
                            Bytes.toBytes("Info"),
                            Bytes.toBytes("Retweets")));

                    String tweet_text = Bytes.toString(result.getValue(
                            Bytes.toBytes("Tweets"),
                            Bytes.toBytes("Tweet_Text")));

                    System.out.println("Retweets: " + retweets + " ||| Tweet_Text: " + tweet_text);
                }
            }
        }
    }
}