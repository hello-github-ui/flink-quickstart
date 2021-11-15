/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataStream<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         *
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * https://flink.apache.org/docs/latest/apis/streaming/index.html
         *
         */
        /**
         * 读取 properties 文件，方式一：基于 ClassLoader 读取配置文件
         * 注意：该方式只能读取类路径下的配置文件，有局限但是如果配置文件在类路径下比较方便。
         */
//		Properties properties = new Properties();
        // 使用 ClassLoader 加载 properties 配置文件生成对应的输入流
//		InputStream is = StreamingJob.class.getClassLoader().getResourceAsStream("hello.txt");
        // 使用properties对象加载输入流
//		properties.load(is);

        // 方式二：使用 InputStream 读取配置文件,注意：该方式的优点在于可以读取任意路径下的配置文件
//		BufferedReader br = new BufferedReader(new FileReader("E:/github-project/flink-quickstart/src/main/resources/hello.txt"));
//		properties.load(br);

        // 定义需要处理文件的位置
//        String filePath = "E:/github-project/flink-quickstart/src/main/resources/hello.txt";
        // 读取文件，获取 数据源
        DataStreamSource<String> socket = env.socketTextStream("127.0.0.1", 9999);
        // 分词之后做 count 实现一个 wordCount 示例
        socket
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] s = value.split("  ");
                        for (String word : s) {
                            out.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return new Tuple2<>(word, 1);
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();


        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
