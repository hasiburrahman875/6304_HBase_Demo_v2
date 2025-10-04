import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InsertData extends Configured implements Tool {

    public String Table_Name = "Twitter";

    @Override
    public int run(String[] argv) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(Table_Name);

            // check if table exists
            if (!admin.tableExists(tableName)) {
                // create table with column families
                TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("DateTime"))
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("Tweets"))
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("Info"))
                        .build();

                admin.createTable(tableDesc);
                System.out.println("Created table: " + Table_Name);
            }

            try (BufferedReader br = new BufferedReader(new FileReader("Twitter.txt"));
                 Table table = connection.getTable(tableName)) {

                String line;
                int row_count = 0;

                while ((line = br.readLine()) != null) {
                    if (line.isEmpty()) continue;

                    row_count++;

                    String[] lineArray = line.split("\t");
                    String date = lineArray[0];
                    String time = lineArray[1];
                    String tweet_text = lineArray[2];
                    String type = lineArray[3];
                    String media_type = lineArray[4];
                    String hashtags = lineArray[5];
                    String tweet_url = lineArray[6];
                    int retweets = Integer.parseInt(lineArray[7]);

                    // row key = tweet_url
                    Put put = new Put(Bytes.toBytes(tweet_url));

                    // add column data
                    put.addColumn(Bytes.toBytes("DateTime"), Bytes.toBytes("Date"), Bytes.toBytes(date));
                    put.addColumn(Bytes.toBytes("DateTime"), Bytes.toBytes("Time"), Bytes.toBytes(time));

                    put.addColumn(Bytes.toBytes("Tweets"), Bytes.toBytes("Tweet_Text"), Bytes.toBytes(tweet_text));
                    put.addColumn(Bytes.toBytes("Tweets"), Bytes.toBytes("Type"), Bytes.toBytes(type));

                    put.addColumn(Bytes.toBytes("Info"), Bytes.toBytes("Media_Type"), Bytes.toBytes(media_type));
                    put.addColumn(Bytes.toBytes("Info"), Bytes.toBytes("Hashtags"), Bytes.toBytes(hashtags));
                    put.addColumn(Bytes.toBytes("Info"), Bytes.toBytes("Retweets"), Bytes.toBytes(retweets));

                    // write row to HBase
                    table.put(put);
                }

                System.out.println("Inserted " + row_count + " rows into table " + Table_Name);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return 0;
    }

    public static void main(String[] argv) throws Exception {
        int ret = ToolRunner.run(new InsertData(), argv);
        System.exit(ret);
    }
}