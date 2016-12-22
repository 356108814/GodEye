package cc.meda.test;

import cc.meda.common.HbaseCommons;
import org.apache.commons.collections.map.HashedMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Yurisues 16-10-28 上午10:48
 */
public class TestHbaseBatch {
    public void batchUserAction(){
//        String[] columns = {"sex"};
//        String[] values = {"1"};
//        HbaseCommons.addData("c1", "user_action", columns, values);
//        HbaseCommons.batchAddData("user_action");

        ArrayList dataList = new ArrayList();
        HashMap data = new HashMap();
        data.put("row_key", "row1");
        HashMap rowData = new HashMap();
        rowData.put("info:name", "yuri");
        rowData.put("info:age", "27");
        data.put("data", rowData);
        dataList.add(data);

        HbaseCommons.batchAddData("user_action", dataList);
    }
    
    public static void main(String[] args){
        TestHbaseBatch batch = new TestHbaseBatch();
        String[] family = {"info"};
//        HbaseCommons.deleteTable("user_action");
//        HbaseCommons.deleteTable("user_action_metrics");
//        HbaseCommons.createTable("user_action", family);
//        HbaseCommons.createTable("user_action_metrics", family);
        batch.batchUserAction();
    }
}
