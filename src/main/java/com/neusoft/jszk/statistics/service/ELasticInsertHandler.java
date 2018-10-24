package com.neusoft.jszk.statistics.service;
import com.neusoft.jszk.statistics.Utils.ESTools;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.transport.TransportClient;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * ES 插入方法工具类封装
 *  * 新版的elasticsearch 在调用setSource的时候传入json字符串、对象后都会爆:
 *  The number of object passed must be even but was [1].其实是新版需要传入Map类型。
 */
public class ELasticInsertHandler {
    /**
     * 方法说明：批量导入数据
     * @param index
     * @param type
     * @param listOfObjects
     */
    public  static void  searchBulk(String index,String type,List<Map<String ,Object>> listOfObjects,TransportClient client){
        BulkRequestBuilder bulkRequest= client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        Iterator<Map<String,Object>> itr = listOfObjects.iterator();
        while (itr.hasNext()){
            Map<String,Object> document = itr.next();
            bulkRequest.add(client.prepareIndex(index,type).setSource(document));
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();

        if (bulkResponse.hasFailures()) {
            System.out.println(bulkResponse.buildFailureMessage());
        }
    }
}
