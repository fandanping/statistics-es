package com.neusoft.jszk.statistics.test;

import com.neusoft.jszk.statistics.SortStatistics;
import com.neusoft.jszk.statistics.service.ELasticInsertHandler;
import com.neusoft.jszk.statistics.service.ELasticSearchHandler;
import com.neusoft.jszk.statistics.service.ElasticIndexHandler;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 测试整个统计流程
 */
public class TestElastic {
    public static void main (String[] args){
        //测试批量插入数据及统计
      /*  TransportClient client=ElasticIndexHandler.getTransportClient();
        Map<String, Object> jsonMap1 = new HashMap<String, Object>();
        jsonMap1.put("an","an1");
        jsonMap1.put("date","1111");
        jsonMap1.put("ipc",new String[]{"A01/12","A02/12","A13/12","B01/12","","A10"});
        Map<String, Object> jsonMap2 = new HashMap<String, Object>();
        jsonMap2.put("an","an2");
        jsonMap2.put("date","2222");
        jsonMap2.put("ipc",new String[]{"","A02","A03","A04","A07","A10","A11","A12","A13","B01","B03"});
        Map<String, Object> jsonMap3 = new HashMap<String, Object>();
        jsonMap3.put("an","an3");
        jsonMap3.put("date","2222");
        jsonMap3.put("ipc",new String[]{"A05","A01","A08","A09","A10","B01","B02"});
        List<Map<String,Object>> listOfObjects = new ArrayList<Map<String,Object>>();
        listOfObjects.add(jsonMap1);
        listOfObjects.add(jsonMap2);
        listOfObjects.add(jsonMap3);
        Map<String,Integer>  result=SortStatistics.getIpcSort(listOfObjects,client);
        System.out.println(result);*/
        /*TransportClient client= ElasticIndexHandler.getTransportClient();
        Map<String, Object> jsonMap1 = new HashMap<String, Object>();
        jsonMap1.put("an","an1");
        jsonMap1.put("date","1111");
        jsonMap1.put("ipc",new String[]{"A01/12","A02/12","A13/12","B01/12","","A10"});
        Map<String, Object> jsonMap2 = new HashMap<String, Object>();
        jsonMap2.put("an","an2");
        jsonMap2.put("date","2222");
        jsonMap2.put("ipc",new String[]{"","A02","A03","A04","A07","A10","A11","A12","A13","B01","B03"});
        Map<String, Object> jsonMap3 = new HashMap<String, Object>();
        jsonMap3.put("an","an3");
        jsonMap3.put("date","2222");
        jsonMap3.put("ipc",new String[]{"A05","A01","A08","A09","A10","B01","B02"});
        List<Map<String,Object>> listOfObjects = new ArrayList<Map<String,Object>>();
        listOfObjects.add(jsonMap1);
        listOfObjects.add(jsonMap2);
        listOfObjects.add(jsonMap3);
        ELasticInsertHandler.searchBulk("test","test",listOfObjects,client);*/


       /* TransportClient client= ElasticIndexHandler.getTransportClient();
        ELasticSearchHandler.getAnResult("aa","aa","an",client);*/
        //测试插入单篇文档
       /* TransportClient client= ElasticIndexHandler.getTransportClient();
        Map<String, Object> jsonMap1 = new HashMap<String, Object>();
        jsonMap1.put("an","an1");
        jsonMap1.put("ipc",new String[]{"A01/12","A02/12","A13/12","B01/12","","A10"});
        ELasticInsertHandler.insertDoc("test","test",jsonMap1,client);*/
       //测试查询单篇文档
       /* TransportClient client=ElasticIndexHandler.getTransportClient();
        ELasticInsertHandler.getDoc("test","test","Tl-mpWYB1lGPeyY7zsPb",client);*/
       //测试删除一篇文档
       /*  TransportClient client=ElasticIndexHandler.getTransportClient();
        ELasticInsertHandler.deleteDoc("test","test","Tl-mpWYB1lGPeyY7zsPb",client);*/
    }
}
