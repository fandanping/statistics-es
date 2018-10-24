package com.neusoft.jszk.statistics.Utils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import java.io.IOException;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * 构建索引映射
 */
public class ESIndexMapping {
    public static XContentBuilder getMapping(){
        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("date")
                    .field("type","text")
                    .field("index",true)
                    .endObject()
                    .startObject("an")
                    .field("type","text")
                    .field("index",true)
                    .endObject()
                    //关联数据
                    //注意:分词的需要text类型，不分词的是Keyword类型
                    .startObject("ipc").field("type","keyword").endObject()
                    .endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapping;
    }

}
