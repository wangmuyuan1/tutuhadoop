package tutuhadoop.hbase.ch03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by wangmuyuan on 20/05/15.
 */
public class GetSimple
{
    public static void main(String[] args) throws IOException
    {
        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, "testtable");

        Get get = new Get(Bytes.toBytes("row1"));
        get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        Result res = table.get(get);
        byte[] val = res.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        System.out.println("Value: " + Bytes.toString(val));
    }
}
