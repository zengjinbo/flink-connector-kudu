

 
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
 
import java.util.ArrayList;
import java.util.List;

public class SelectKuduData2
{
    private static String tableName = "event_wos_p1";
 
    //private static KuduClient client = new KuduClient.KuduClientBuilder("IP,IP,IP").defaultAdminOperationTimeoutMs(60000).build();
 
 
    // 获取需要查询数据的列
    private static List<String> projectColumns = new ArrayList<String>();
 
    private static KuduTable table;
    private static KuduScanner.KuduScannerBuilder builder;
    private static KuduScanner scanner;
    private static KuduPredicate predicate1;
    private static KuduPredicate predicate2;
    private static KuduClient client;

    public static void main(String[] args) throws KuduException
    {
        try
        {
            //select 查询字段名
            projectColumns.add("user_id"); //字段名
            projectColumns.add("day");
            table = client.openTable(tableName);
 
            // 简单的读取 newScannerBuilder(查询表)  setProjectedColumnNames(指定输出列)  build()开始扫描
//            KuduScanner scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).build();
            client=new KuduClient.KuduClientBuilder("writerConfig.getMasters()").build();
            builder = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns);
            /**
             * 设置搜索的条件 where 条件过滤字段名
             * 如果不设置，则全表扫描
             */
            //下面的条件过滤 where user_id = xxx and day = xxx;
             long userID = 7232560922086310458L;
             int Day = 17889;
             //比较方法ComparisonOp：GREATER、GREATER_EQUAL、EQUAL、LESS、LESS_EQUAL
            predicate1 = predicate1.newComparisonPredicate(table.getSchema().getColumn("user_id"),
                                                                KuduPredicate.ComparisonOp.EQUAL, userID);
            predicate2 = predicate2.newComparisonPredicate(table.getSchema().getColumn("day"),
                                                                KuduPredicate.ComparisonOp.EQUAL, Day);
 
            builder.addPredicate(predicate1);
            builder.addPredicate(predicate2);
 
            // 开始扫描
            scanner = builder.build();
 
            while (scanner.hasMoreRows())
            {
                RowResultIterator results = scanner.nextRows();
                /*
                  RowResultIterator.getNumRows()
                        获取此迭代器中的行数。如果您只想计算行数，那么调用这个函数并跳过其余的。
                        返回：此迭代器中的行数
                        如果查询不出数据则 RowResultIterator.getNumRows() 返回的是查询数据的行数，如果查询不出数据返回0
                 */
                // 每次从tablet中获取的数据的行数 
                int numRows = results.getNumRows();
                System.out.println("numRows count is : " + numRows);
                while (results.hasNext()) {
                    RowResult result = results.next();
                    long user_id = result.getLong(0);
                    int day = result.getInt(1);
                     System.out.println("user_id is : " + user_id + "  ===  day: " + day );
                }
                System.out.println("--------------------------------------");
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally 
        {
            scanner.close();
            client.close();
        }
    }
}