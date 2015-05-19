package tutuhadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class SinglePutEx {
	public static void main(String[] args) throws IOException {
		// Get instance of Default Configuration
		Configuration conf = HBaseConfiguration.create();
		// Get table instance
		HTable table = new HTable(conf, "tab1");
		// Create Put with rowkey
		Put put = new Put(Bytes.toBytes("row-1"));
		// Add a column with value "Hello", in "cf1:greet", to the // Put.
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("greet"),
				Bytes.toBytes("Hello"));
		// Add more column with value "John", in "cf1:person",
		// to the Put.
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("person"),
				Bytes.toBytes("John"));
		table.put(put);
		table.close();
	}
}