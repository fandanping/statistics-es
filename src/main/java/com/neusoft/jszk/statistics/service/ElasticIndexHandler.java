package com.neusoft.jszk.statistics.service;

import com.neusoft.jszk.statistics.Utils.ESIndexMapping;
import com.neusoft.jszk.statistics.Utils.ESTools;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;

/**
 * 类说明：ES索引管理操作封装
 */
public class ElasticIndexHandler {
    /**
     * 方法1：获取连接
     * @return
     */
    public static TransportClient getTransportClient(){
         return ESTools.getTransportClient();
    }
    /**
     * 方法2：关闭连接
     * @param client
     */
    public  static void close(TransportClient client){
         ESTools.closeClient(client);
    }

    /**
     * 方法3说明：创建索引,配置参数采用默认
     * 注意：索引名最好小写，如果索引名不规范，会报InvalidIndexNameException异常。 最好在创建索引之前使用String类的toLowerCase()方法进行小写转换
     */
    public static boolean createIndex(String index,TransportClient transportClient){
        //构建一个Index（索引）
       // System.out.println("index创建开始。。");
        CreateIndexResponse createIndexResponse = transportClient.admin().indices().prepareCreate(index.toLowerCase()).get();
        //System.out.println("index创建完成。。");
        return createIndexResponse.isAcknowledged()?true:false;
    }

    /**
     * 方法4说明：创建索引，并设置分片数.....
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
     * 方法5说明：删除索引
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
     * 方法6说明：判定索引是否存在
     * @param indexName
     * @return
     */
    public static boolean isExistsIndex(String indexName,TransportClient transportClient){
        IndicesExistsResponse response=transportClient.admin().indices().prepareExists(indexName).get();
        return response.isExists()?true:false;
    }

    /**
     * 方法7说明：设置mapping
     */
    public static void createMapping(String index,String type,TransportClient transportClient){
        //创建Mapping
       // System.out.println("mapping创建开始。。");
        PutMappingRequest mapping = Requests.putMappingRequest(index).type(type).source(ESIndexMapping.getMapping());
        transportClient.admin().indices().putMapping(mapping).actionGet();
        //System.out.println("mapping创建完成。。");
    }

    /**
     * 方法8说明：判断type是否存在
     * @param indexName
     * @param type
     * @param transportClient
     * @return
     */
    public static boolean isExistsType(String indexName,String type,TransportClient transportClient){
        TypesExistsResponse existsResponse=transportClient.admin().indices().prepareTypesExists(indexName).setTypes(type).get();
        return existsResponse.isExists()?true:false;
    }

    /**
     * 方法9说明：获取mapping
     * @param indexName
     * @param type
     * @param transportClient
     * @return
     */
    public static String getMapping(String indexName,String type,TransportClient transportClient){
        GetMappingsResponse mappingsResponse=transportClient.admin().indices().prepareGetMappings(indexName).get();
        ImmutableOpenMap<String,MappingMetaData> mapings=mappingsResponse.getMappings().get(indexName);
        MappingMetaData metaData =mapings.get(type);
        return metaData.getSourceAsMap().toString();

    }



}
