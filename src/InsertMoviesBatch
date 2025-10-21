import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InsertMoviesBatch extends Configured implements Tool {

    private static final String TABLE_NAME = "Movies";
    private static final int BATCH_SIZE = 5000;  // Insert in chunks

    @Override
    public int run(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");  // Update if needed
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        // --- Connection & performance tuning ---
        conf.setInt("hbase.rpc.timeout", 60000);
        conf.setInt("hbase.client.operation.timeout", 120000);
        conf.setInt("hbase.client.scanner.timeout.period", 120000);
        conf.setInt("hbase.client.write.buffer", 8 * 1024 * 1024); // 8MB client buffer

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Admin admin = connection.getAdmin()) {

            TableName tableName = TableName.valueOf(TABLE_NAME);

            // Create table if missing
            if (!admin.tableExists(tableName)) {
                TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("Product"))
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("Review"))
                        .build();
                admin.createTable(tableDesc);
                System.out.println("✅ Created table: " + TABLE_NAME);
            }

            // Use BufferedMutator for high-performance bulk writes
            BufferedMutatorParams params = new BufferedMutatorParams(tableName)
                    .writeBufferSize(10 * 1024 * 1024); // 10MB per batch client buffer

            try (BufferedMutator mutator = connection.getBufferedMutator(params);
                 BufferedReader br = new BufferedReader(new FileReader("/home/mrpk9/Desktop/movies.txt"))) {

                String line;
                int totalRows = 0;
                List<Put> batch = new ArrayList<>();

                String productId = "", userId = "", profileName = "", helpfulness = "",
                        score = "", time = "", summary = "", text = "";

                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;

                    if (line.startsWith("product/productId:")) {
                        productId = line.split(":", 2)[1].trim();
                    } else if (line.startsWith("review/userId:")) {
                        userId = line.split(":", 2)[1].trim();
                    } else if (line.startsWith("review/profileName:")) {
                        profileName = line.split(":", 2)[1].trim();
                    } else if (line.startsWith("review/helpfulness:")) {
                        helpfulness = line.split(":", 2)[1].trim();
                    } else if (line.startsWith("review/score:")) {
                        score = line.split(":", 2)[1].trim();
                    } else if (line.startsWith("review/time:")) {
                        time = line.split(":", 2)[1].trim();
                    } else if (line.startsWith("review/summary:")) {
                        summary = line.split(":", 2)[1].trim();
                    } else if (line.startsWith("review/text:")) {
                        text = line.split(":", 2)[1].trim();

                        String rowKey = productId + "_" + userId + "_" + time;
                        Put put = new Put(Bytes.toBytes(rowKey));

                        put.addColumn(Bytes.toBytes("Product"), Bytes.toBytes("ProductId"), Bytes.toBytes(productId));
                        put.addColumn(Bytes.toBytes("Review"), Bytes.toBytes("UserId"), Bytes.toBytes(userId));
                        put.addColumn(Bytes.toBytes("Review"), Bytes.toBytes("ProfileName"), Bytes.toBytes(profileName));
                        put.addColumn(Bytes.toBytes("Review"), Bytes.toBytes("Helpfulness"), Bytes.toBytes(helpfulness));
                        put.addColumn(Bytes.toBytes("Review"), Bytes.toBytes("Score"), Bytes.toBytes(score));
                        put.addColumn(Bytes.toBytes("Review"), Bytes.toBytes("Time"), Bytes.toBytes(time));
                        put.addColumn(Bytes.toBytes("Review"), Bytes.toBytes("Summary"), Bytes.toBytes(summary));
                        put.addColumn(Bytes.toBytes("Review"), Bytes.toBytes("Text"), Bytes.toBytes(text));

                        batch.add(put);
                        totalRows++;

                        if (batch.size() >= BATCH_SIZE) {
                            mutator.mutate(batch);
                            mutator.flush();
                            batch.clear();
                            System.out.println("Inserted " + totalRows + " rows...");
                        }
                    }
                }

                // Flush any remaining records
                if (!batch.isEmpty()) {
                    mutator.mutate(batch);
                    mutator.flush();
                }

                System.out.println("✅ Completed insertion of " + totalRows + " total rows into " + TABLE_NAME);
            }

        } catch (Exception e) {
            System.err.println("❌ Error during insertion: " + e.getMessage());
            e.printStackTrace();
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new InsertMoviesBatch(), args);
        System.exit(ret);
    }
}
