package com.neusoft.jszk.statistics;
import com.neusoft.jszk.statistics.Utils.IndexGenerator;
import com.neusoft.jszk.statistics.domain.Constants;
import com.neusoft.jszk.statistics.service.ELasticInsertHandler;
import com.neusoft.jszk.statistics.service.ELasticSearchHandler;
import com.neusoft.jszk.statistics.service.ElasticIndexHandler;
import org.elasticsearch.client.transport.TransportClient;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  类说明：排序统计调用入口
 */
public class SortStatistics {
    /**
     * 方法1：获取案卷ipc排序前十结果
     * @return
     */
    public static  Map<String,Integer> getIpcSort(List<Map<String ,Object>> listOfObjects,TransportClient client){
        String INDEX_NAME= IndexGenerator.generate().toLowerCase();
       //String INDEX_NAME= "aa";
        String  TYPE=INDEX_NAME;
        Map<String,Integer> map=new HashMap<String,Integer>();
        //1. 采用默认配置参数创建索引
        //判断索引是否存在，如果存在,重新生成索引
       /* if(ElasticIndexHandler.isExists(INDEX_NAME,client)){
            INDEX_NAME=IndexGenerator.generate();
        }*/
        if(ElasticIndexHandler.createIndex(INDEX_NAME,client)){
            //2. 创建mapping
            ElasticIndexHandler.createMapping(INDEX_NAME,TYPE,client);
            //3. 批量插入数据
            ELasticInsertHandler.searchBulk(INDEX_NAME,TYPE,listOfObjects,client);
            //4. 聚合统计
            map= ELasticSearchHandler.getIcResult(INDEX_NAME,TYPE,"ipc_count","ipc",client);
            //5. 删除索引
            ElasticIndexHandler.deleteIndex(INDEX_NAME,client);
         }else{
                map.put("index",Constants.INDEX_NAME_CREATE_ERROR);
         }
        //6. 返回数据
        return  map;
    }

}
