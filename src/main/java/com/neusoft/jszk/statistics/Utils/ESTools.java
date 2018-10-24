package com.neusoft.jszk.statistics.Utils;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;

/**
 * 客户端连接es集群工具类
 */
public class ESTools {
    public  static TransportClient transportClient = null;
    /**
     * 获取连接
     * @return
     */
     public static   TransportClient  getTransportClient(){
        String firstIp = "192.168.1.198";
        String secondIp = "192.168.1.198";
        String thirdIp = "192.168.1.198";
        int firstPort = 9300;
        int secondPort = 9300;
        int thirdPort= 9300;
        String clusterName="statistics";
        try {
            System.out.println("创建Elasticsearch Client 开始");
            Settings settings = Settings
                    .builder()
                    .put("cluster.name",clusterName)
                    .put("client.transport.sniff", true)
                    .build();
            transportClient=new PreBuiltTransportClient(settings);
            TransportAddress firstAddress = new TransportAddress(InetAddress.getByName(firstIp),firstPort);
            TransportAddress secondAddress = new TransportAddress(InetAddress.getByName(secondIp),secondPort);
            TransportAddress thirdAddress = new TransportAddress(InetAddress.getByName(thirdIp),thirdPort);
            transportClient.addTransportAddress(firstAddress);
            transportClient.addTransportAddress(secondAddress);
            transportClient.addTransportAddress(thirdAddress);
            System.out.println("ElasticSearch初始化完成。。");
        } catch (Exception e) {
            System.out.println("ElasticSearch初始化失败");
        }
        return transportClient;
    }

    /**
     * 关闭连接
     */
    public  static void closeClient(TransportClient client){
        if(client !=null){
            try {
                client.close();
            } catch (Exception e) {

            }
        }
    }

}
