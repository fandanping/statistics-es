package com.neusoft.jszk.statistics.service;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import java.util.*;

/**
 * 类说明：ES查询类方法封装
 */
public class ELasticSearchHandler {
    /**
     * 方法1说明(聚合查询统计-桶聚合方式)：查询某个字段中每个词出现的次数，返回排序后的所有词频
     *
     * @param index 索引名称
     * @param type type类型名称
     * @param termsValue  自定义统计字段别名
     * @param fieldValue  要统计的字段名
     */
    public  static Map<String,Integer>  getIcResults(String index, String type, String termsValue, String fieldValue, TransportClient client){
        TermsAggregationBuilder termAgg=AggregationBuilders.terms(termsValue).field(fieldValue).size(Integer.MAX_VALUE);
        SearchResponse response=client.prepareSearch(index).setTypes(type).addAggregation(termAgg).execute().actionGet();
        Terms term=response.getAggregations().get(termsValue);
        Map<String,Integer> map=new LinkedHashMap<String,Integer>();
        for(Terms.Bucket b : term.getBuckets()){
            map.put(b.getKeyAsString(),new Integer((int)(b.getDocCount())));
            //System.out.println(b.getKeyAsString()+":"+b.getDocCount());
            //System.out.println(b.getDocCount());
        }
        return map;
    }
    /**
     * 方法1说明(聚合查询统计-桶聚合方式)：查询某个字段中每个词出现的次数，排序后取前十
     *                                     目前由于ipc字段会存入空值，所以取前十的时候排除调空值
     * @param index 索引名称
     * @param type type类型名称
     * @param termsValue  自定义统计字段别名
     * @param fieldValue  要统计的字段名
     */
    public  static Map<String,Integer>  getIcResult(String index, String type, String termsValue, String fieldValue, TransportClient client){
        Map<String,Integer> result=new LinkedHashMap<String,Integer>();
        Map<String,Integer> map=ELasticSearchHandler.getIcResults(index,type,termsValue,fieldValue,client);
        int count=1;
        for(Map.Entry<String, Integer> entry : map.entrySet()){
              if(count<=10){
                  if(entry.getKey().length()!=0){
                      result.put(entry.getKey(),entry.getValue());
                      count++;
                  }else{
                      continue;
                  }
              }
        }
        return result;
    }

    /**
     * 方法2说明：查询指定字段的所有值
     * @param index 索引名称
     * @param type type类型名称
     * @param fields 查询的字段名
     * @return  由于排序接口需要an号以Or拼接,在此拼接好返回
     */
    public static  String getAnResult(String index,String type,String fields,TransportClient client){
        //查询指定字段的所有值
        //注释：setFetchSource 返回指定字段
        SearchResponse response = client.prepareSearch(index).setTypes(type).setFetchSource(new String[] { fields}, null)
                .setQuery(QueryBuilders.matchAllQuery())
                .setExplain(false)
                .execute().actionGet();
        //遍历打印结果
        String sortKeyword="";
        for(SearchHit hit:response.getHits()){
            //遍历文档的每个字段
            Map<String,Object> map=hit.getSourceAsMap();
            sortKeyword +=map.get("an")+ " or ";
        }
        //int orindex=sortKeyword.lastIndexOf("or");
        String result =sortKeyword.substring(0,sortKeyword.length()-4).trim();
        System.out.println(result);
        return result;
    }
}
