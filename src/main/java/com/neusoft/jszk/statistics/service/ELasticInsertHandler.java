package com.neusoft.jszk.statistics.service;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
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
     * 方法1说明：批量导入数据:一次1000条
     * @param index 索引名称
     * @param type type名称
     * @param listOfObjects Map集合
     */
    public  static void  searchBulk(String index,String type,List<Map<String ,Object>> listOfObjects,TransportClient client){
        //做法2：每次导入1000条
        BulkRequestBuilder bulkRequest= client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        Iterator<Map<String,Object>> itr = listOfObjects.iterator();
        int count=0;
        while (itr.hasNext()){
            count++;
            Map<String,Object> document = itr.next();
            bulkRequest.add(client.prepareIndex(index,type).setSource(document));
            if(count %1000 ==0){
                System.out.println("本次提交了1000条");
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    System.out.println("message"+bulkResponse.buildFailureMessage());
                }
                //重新创建一个bulk
                bulkRequest=client.prepareBulk();
            }
        }
        bulkRequest.execute().actionGet();
        System.out.println("总共提交了："+count);
    }

    /**
     * 方法2说明：批量导入数据:(和方法1有所区别 )
     * @param index
     * @param type
     * @param listOfObjects
     * @param client
     */
    public  static void  insertBulkReconds(String index,String type,List<Map<String ,Object>> listOfObjects,TransportClient client){
        //做法1：一次性批量导入
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

    /**
     * 方法3说明：插入单篇文档
     * @param index
     * @param type
     * @param map
     * @param client
     */
    public static void  insertDoc(String index,String type,Map<String,Object> map,TransportClient client){
        IndexResponse response=client.prepareIndex(index,type).setSource(map).get();
        //文档是否创建成功,如果是新创建的,就返回created;如果文档不是首次创建二是被更新过的就返回Ok
        System.out.println(response.status());
    }

    /**
     * 方法4说明：通过文档id读取一个文档
     * @param index
     * @param type
     * @param id
     * @param client
     * @return
     */
    public static String getDoc(String index,String type,String id,TransportClient client){
        GetResponse response=client.prepareGet(index,type,id).get();
        String content=response.getSourceAsString();
        System.out.println(content);
        return content;
    }

    /**
     * 方法5说明：通过文档id删除一个文档
     * @param index
     * @param type
     * @param id
     * @param client
     * @return
     */
    public static String deleteDoc(String index,String type,String id,TransportClient client){
        DeleteResponse response=client.prepareDelete(index,type,id).get();
        System.out.println(response.status().toString());
        return  response.status().toString(); //删除成功返回ok；删除失败返回NOT_FOUND
    }

}
