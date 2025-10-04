import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class GroupByType {
    public static String Table_Name = "Twitter";

    public static void main(String[] args) throws Throwable {
        Configuration conf = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(Table_Name))) {

            // Filter for text
            SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                    Bytes.toBytes("Tweets"),
                    Bytes.toBytes("Type"),
                    CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes("text"))
            );

            // Filter for link
            SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
                    Bytes.toBytes("Tweets"),
                    Bytes.toBytes("Type"),
                    CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes("link"))
            );

            Scan scan1 = new Scan().setFilter(filter1);
            Scan scan2 = new Scan().setFilter(filter2);

            ResultScanner scanner1 = table.getScanner(scan1);
            ResultScanner scanner2 = table.getScanner(scan2);

            int textNo = 0;
            for (Result result : scanner1) textNo++;

            int linkNo = 0;
            for (Result result : scanner2) linkNo++;

            System.out.println("Text: " + textNo);
            System.out.println("Link: " + linkNo);
        }
    }
}