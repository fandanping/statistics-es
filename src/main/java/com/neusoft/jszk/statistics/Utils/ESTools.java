package com.neusoft.jszk.statistics.Utils;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import java.net.InetAddress;

/**
 * 类说明：客户端连接es集群工具类
 */
public class ESTools {
    public  volatile  static TransportClient transportClient;
    public static String clusterName="statistics"; //集群名称
    //服务器的ip 以及端口号
    public static String firstIp = "192.168.1.198";
    public static String secondIp = "192.168.1.198";
    public static String thirdIp = "192.168.1.198";
    public static int firstPort = 9300;
    public static int secondPort = 9300;
    public static int thirdPort= 9300;
    /**
     * 方法1说明：获取连接
     * 1.Settings对象中可以添加配置信息。
     *   1.1由于集群名称不是默认的elasticsearch，需要在Setting对象中指定
     *   1.2 启用嗅探机制：该特性允许它动态地添加新主机并删除旧主机
     * 2.创建transportClient对象
     * 补充：为了避免每次请求都创建一个新的TransportClient对象,封装双重加锁单例模式返回TransportClient对象
     * @return
     */
     public static   TransportClient  getTransportClient(){
         if(transportClient == null){
             synchronized (TransportClient.class){
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
             }
         }
         return transportClient;
    }

    /**
     * 方法2说明：关闭TransportClient连接
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
