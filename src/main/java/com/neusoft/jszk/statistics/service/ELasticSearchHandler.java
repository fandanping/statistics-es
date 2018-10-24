package com.neusoft.jszk.statistics.service;

import com.neusoft.jszk.statistics.Utils.ESTools;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.*;

/**
 * ES查询类方法封装
 */
public class ELasticSearchHandler {
    /**
     * 方法说明(聚合查询统计)：查询某个字段中每个词出现的次数，排序后取前十
     * 目前返回所有
     * @param index
     * @param type
     * @param termsVale
     * @param fieldValue
     */
    public  static Map<String,Integer>  getIcResult(String index, String type, String termsVale, String fieldValue, TransportClient client){
        SearchRequestBuilder requestBuilder =client.prepareSearch(index).setTypes(type).setQuery(QueryBuilders.matchAllQuery());
        //聚合分析查询出现次数最多的10个词汇
       // SearchResponse actionGet = requestBuilder.addAggregation(AggregationBuilders.terms("ipc_count").field("ipc")).execute().actionGet();
        SearchResponse actionGet = requestBuilder.addAggregation(AggregationBuilders.terms(termsVale).field(fieldValue).size(Integer.MAX_VALUE)).execute().actionGet();
        //获取分析后的数据
        Aggregations aggregations = actionGet.getAggregations();
        Terms term = aggregations.get(termsVale);
        List<Map<String,Integer>> list=new ArrayList<Map<String,Integer>>();
        Map<String,Integer> map=new LinkedHashMap<String,Integer>();
        for(Terms.Bucket b : term.getBuckets()){
            map.put(b.getKeyAsString(),new Integer((int)(b.getDocCount())));
            //System.out.println(b.getKeyAsString()+":"+b.getDocCount());
            //System.out.println(b.getDocCount());
        }
        return map;
    }

    /**
     * 查询指定字段的所有值
     * @param index
     * @param type
     * @param fields
     * @return
     */
    public static  String getAnResult(String index,String type,String fields,TransportClient client){
        //查询指定字段的所有值
        //注释：setFetchSource 返回指定字段
        SearchResponse response = client.prepareSearch(index).setTypes(type).setFetchSource(new String[] { "an" }, null)
                .setQuery(QueryBuilders.matchAllQuery())
                .setExplain(false)
                .execute().actionGet();
        //遍历打印结果
        String sortKeyword="";
        for(SearchHit hit:response.getHits()){
           // System.out.println("source:"+hit.getSourceAsString());
           // System.out.println("index:"+hit.getIndex());
           // System.out.println("type:"+hit.getType());
            //System.out.println("id:"+hit.getId());
            //遍历文档的每个字段
            Map<String,Object> map=hit.getSourceAsMap();
            sortKeyword +=map.get("an")+ " or ";
        }
        int orindex=sortKeyword.lastIndexOf("or");
        String result =sortKeyword.substring(0,orindex).trim();
        System.out.println(result);
        return result;
    }
}
