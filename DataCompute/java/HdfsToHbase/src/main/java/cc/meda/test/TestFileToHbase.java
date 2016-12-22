package cc.meda.test;

import cc.meda.common.HbaseCommons;
import net.sf.json.JSONObject;
import org.apache.commons.collections.map.HashedMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Yurisues 16-10-28 下午5:36
 */
public class TestFileToHbase {
    /**
     * 以行为单位读取文件，常用于读面向行的格式化文件
     */
    public ArrayList<String> readFileByLines(String fileName) {
        ArrayList<String> lines = new ArrayList<String>();
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                lines.add(tempString);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return lines;
    }

    public static void main(String[] args) {
        TestFileToHbase fh = new TestFileToHbase();
        ArrayList dataList = new ArrayList();
        ArrayList<String> lines = fh.readFileByLines("/var/lib/hadoop-hdfs/DataCompute/java/21.log");
        int index = 1;
        for(String line: lines){
            line = line.replace("u'", "\"").replace("'", "\"");
            try {
                JSONObject jsonObject = JSONObject.fromObject(line);
                Iterator it = jsonObject.keys();
                Map<String, Object> rowMap = new HashedMap();
                rowMap.put("rowKey", "row" + index);
                Map<String, String> acionMap = new HashMap<String, String>();
                while (it.hasNext()) {
                    String key = (String) it.next();
                    String value = jsonObject.getString(key);
                    acionMap.put("info:" + key, value);
                }
                rowMap.put("data", acionMap);
                dataList.add(rowMap);
                index++;
            } catch (Exception e) {

            }
        }
        HbaseCommons.batchAddData("user_action", dataList);
    }
}
