package com.example.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author 030
 * @date 21:04 2021/11/16
 * @description
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 创建一个 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 定义一个文件
        String filePath = "E:/github-project/flink-quickstart/src/main/resources/hello.txt";
        // 读取文件内容，到一个结果集，这里用父类 DataSet 结果集来接收
        DataSet<String> inputDataSet = env.readTextFile(filePath);

        // 对数据集进行处理，按空格分词展开，转换成(word, 1) 二元组进行统计
        // 扁平化处理，打散结果集中的 每个 String，作相应的处理
        DataSet<Tuple2<String, Integer>> tuple2DataSet =
                inputDataSet
                        .flatMap(new MyFlatMapImpl())
                        .groupBy(0) // 按照第一个位置的word分组
                        .sum(1); // 将第二个位置上的数据求和

        tuple2DataSet.print();
    }

    // 自定义实现 FlatMapFunction 扁平化操作
    public static class MyFlatMapImpl implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按 空格 分词
            String[] words = value.split("\\w");
            // 遍历所有 word，包成 二元组输出
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}


