package tutuhadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangmuyuan on 28/03/15.
 */
public class ListPutGetDeleteEx {

    public static void main(String[] args) throws IOException {

        // Get instance of Default Configuration
        Configuration conf = HBaseConfiguration.create();

        HTable table = new HTable(conf, "tab1");

        putValues(table);

        getValues(table);

        deleteValues(table);
    }

    private static void deleteValues(HTable table) throws IOException {
        List<Delete> deletes = new ArrayList<>();
        Delete delete1 = new Delete(Bytes.toBytes("row1"));
        //delete1.setTimestamp(4);
        deletes.add(delete1);

        Delete delete2 = new Delete(Bytes.toBytes("row2"));
        delete2.deleteColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        delete2.deleteColumns(Bytes.toBytes("colfam2"), Bytes.toBytes("qual3"));
        deletes.add(delete2);

        Delete delete3 = new Delete(Bytes.toBytes("row3"));
        delete3.deleteFamily(Bytes.toBytes("colfam1"));
        delete3.deleteFamily(Bytes.toBytes("colfam2"));
        deletes.add(delete3);

        table.delete(deletes);
        table.close();
    }

    private static void putValues(HTable table) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        List<Put> puts = new ArrayList<>();

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                Bytes.toBytes("val1"));
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.add(Bytes.toBytes("colfam2"), Bytes.toBytes("qual1"),
                Bytes.toBytes("val2"));
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("row2"));
        put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
                Bytes.toBytes("val3"));
        puts.add(put3);
        table.put(puts);
    }


    private static void getValues(HTable table) throws IOException {
        List<Get> gets = new ArrayList<>();

        byte[] cf1 = Bytes.toBytes("colfam1");
        byte[] qf1 = Bytes.toBytes("qual1");
        byte[] qf2 = Bytes.toBytes("qual2");
        byte[] row1 = Bytes.toBytes("row1");
        byte[] row2 = Bytes.toBytes("row2");
        Get get1 = new Get(row1);
        get1.addColumn(cf1, qf1);
        gets.add(get1);
        Get get2 = new Get(row2);
        get2.addColumn(cf1, qf1);
        gets.add(get2);
        Get get3 = new Get(row2);
        get3.addColumn(cf1, qf2);
        gets.add(get3);
        Result[] results = table.get(gets);

        System.out.println("Second iteration...");
        for (Result result : results) {
            for (KeyValue kv : result.raw()) {
                System.out.println("Row: " + Bytes.toString(kv.getRow()) +
                        " Value: " + Bytes.toString(kv.getValue()));
            }
        }
    }
}
