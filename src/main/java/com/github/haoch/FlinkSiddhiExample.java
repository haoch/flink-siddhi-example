/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.haoch;

import com.github.haoch.source.Event;
import com.github.haoch.source.RandomEventSource;
import org.apache.flink.contrib.siddhi.SiddhiCEP;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

public class FlinkSiddhiExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input1 = env.addSource(new RandomEventSource(1000).closeDelay(1500), "input1");
        DataStream<Event> input2 = env.addSource(new RandomEventSource(1000).closeDelay(1500), "input2");

        DataStream<Map<String, Object>> output = SiddhiCEP
                .define("inputStream1", input1.keyBy("name"), "id", "name", "price", "timestamp")
                .union("inputStream2", input2.keyBy("name"), "id", "name", "price", "timestamp")
                .sql(
                    "from every s1 = inputStream1[id == 2] "
                            + " -> s2 = inputStream2[id == 3] "
                            + "select s1.id as id_1, s1.name as name_1, s2.id as id_2, s2.name as name_2 "
                            + "insert into outputStream"
                )
                .returnAsMap("outputStream");

        output.print();
        env.execute();
    }
}