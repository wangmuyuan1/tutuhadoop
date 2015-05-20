package tutuhadoop.hbase.ch03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class PutUsingClientBuffer
{
    public static void main(String[] args) throws IOException
    {
        Configuration config = HBaseConfiguration.create();

        HTable table = new HTable(config, "testtable");
        System.out.println("Auto Flush: " + table.isAutoFlush());
        table.setAutoFlushTo(false);
        System.out.println("Auto Flush: " + table.isAutoFlush());

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        table.put(put1);

        Put put2 = new Put(Bytes.toBytes("row2"));
        put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        table.put(put2);

        Put put3 = new Put(Bytes.toBytes("row3"));
        put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
        table.put(put3);

        Get get = new Get(Bytes.toBytes("row1"));
        Result res1 = table.get(get);
        System.out.println("Result 1: " + res1);
        table.flushCommits();

        Result res2 = table.get(get);
        System.out.println("Result 2: " + res2);
    }
}
