package com.example.demo;

import com.alibaba.fastjson.JSONObject;
import com.example.demo.util.ElasticSearchSinkUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;


/**
 * @author 030
 * @date 21:50 2021/11/15
 * @description Main 方法测试
 */
public class Test {

    public static void main(String[] args) throws Exception {
        String propertiesPath = args[0];
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesPath);
        List<HttpHost> esAddresses = ElasticSearchSinkUtil.getEsAddress(parameterTool.get("es.hosts"));
        int bulk_size = parameterTool.getInt("es.bulk.flushMaxAction");
        int sinkParallelism = parameterTool.getInt("es.sink.parallelism");
        String rawPath = parameterTool.get("rawPath");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile(rawPath);
        SingleOutputStreamOperator<Tuple7<String, String, String, String, String, String, String>> map =
                dataStreamSource.map(new MapFunction<String, Tuple7<String, String, String, String, String, String, String>>() {
            @Override
            public Tuple7<String, String, String, String, String, String, String> map(String s) throws Exception {
                String[] splits = s.split("\t");
                String field1 = splits[0];
                String field2 = splits[1];
                String field3 = splits[2];
                String field4 = splits[3];
                String field5 = splits[4];
                String field6 = splits[5];
                String field7 = splits[6];
//                return new Tuple7<>(uid, timestamp, desc_info, related_identity, record_num, desc_type, date);
                return new Tuple7<>("uid", "timestamp",  "desc_info",  "related_identity",  "record_num",  "desc_type",  "date");
            }
        });


        ElasticSearchSinkUtil.addSink(esAddresses, bulk_size, sinkParallelism, map, new ElasticsearchSinkFunction<Tuple7<String, String, String, String, String, String, String>>() {
            @Override
            public void process(Tuple7<String, String, String, String, String, String, String> data, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                IndexRequest indexRequest = null;
                try {
                    indexRequest = createIndexRequest(data);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                requestIndexer.add(indexRequest);
            }

            public IndexRequest createIndexRequest(Tuple7<String, String, String, String, String, String, String> data) throws IOException {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("field1", data.f0);
                jsonObject.put("field2", data.f1);
                jsonObject.put("field3", JSONObject.parseObject(data.f2));
                jsonObject.put("field4", JSONObject.parseObject(data.f3));
                jsonObject.put("field5", data.f4);
                jsonObject.put("field6", data.f5);
                jsonObject.put("field7", data.f6);
                return Requests.indexRequest()
                        .index("my_index")
                        .type("type").source(jsonObject.toString(), XContentType.JSON);
            }
        });

        // map.setParallelism(1).print();
        env.execute("Test");
    }
}
