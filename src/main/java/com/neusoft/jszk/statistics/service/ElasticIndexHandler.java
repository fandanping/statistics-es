package com.neusoft.jszk.statistics.service;

import com.neusoft.jszk.statistics.Utils.ESIndexMapping;
import com.neusoft.jszk.statistics.Utils.ESTools;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;

/**
 * ES公共方法封装类
 */
public class ElasticIndexHandler {
    /**
     * 获取连接
     * @return
     */
    public static TransportClient getTransportClient(){
         return ESTools.getTransportClient();
    }
    /**
     * 关闭连接
     * @param client
     */
    public  static void close(TransportClient client){
         ESTools.closeClient(client);
    }

    /**
     * 创建索引,配置参数采用默认
     */
    public static boolean createIndex(String index,TransportClient transportClient){
        //构建一个Index（索引）
       // System.out.println("index创建开始。。");
        CreateIndexRequest request = new CreateIndexRequest(index);
        CreateIndexResponse createIndexResponse = transportClient.admin().indices().prepareCreate(index).get();
        //System.out.println("index创建完成。。");
        return createIndexResponse.isAcknowledged()?true:false;
    }

    /**
     * 创建索引，设置分片数.....
     * @param indexName
     * @param shards
     * @param replicas
     * @return
     */
    public static boolean createIndex(String indexName, int shards, int replicas,TransportClient transportClient){
        Settings settings = Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas)
                .build();
        CreateIndexResponse createIndexResponse = transportClient.admin().indices()
                .prepareCreate(indexName.toLowerCase())
                .setSettings(settings)
                .execute().actionGet();
        return createIndexResponse.isAcknowledged()?true:false;
    }
    /**
     * 删除索引
     * @param indexName
     * @return
     */
    public static boolean deleteIndex(String indexName,TransportClient transportClient) {
        DeleteIndexResponse deleteResponse = transportClient.admin().indices()
                .prepareDelete(indexName)
                .execute()
                .actionGet();
        return deleteResponse.isAcknowledged()?true:false;
    }

    /**
     * 判定索引是否存在
     * @param indexName
     * @return
     */
    public static boolean isExists(String indexName,TransportClient transportClient){
        IndicesExistsResponse response=transportClient.admin().indices().prepareExists(indexName).get();
        return response.isExists()?true:false;
    }

    /**
     * 创建索引的映射mapping
     */
    public static void createMapping(String index,String type,TransportClient transportClient){
        //创建Mapping
       // System.out.println("mapping创建开始。。");
        PutMappingRequest mapping = Requests.putMappingRequest(index).type(type).source(ESIndexMapping.getMapping());
        transportClient.admin().indices().putMapping(mapping).actionGet();
        //System.out.println("mapping创建完成。。");
    }



}
