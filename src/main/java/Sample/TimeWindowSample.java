/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package Sample;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.config.SiddhiContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.query.compiler.SiddhiCompiler;
import io.siddhi.core.util.parser.SiddhiAppParser;


/**
 * Sample Siddhi application that groups the events by symbol and calculates aggregates such as the sum of price and
 * sum of volume for the last sliding 5 seconds time window.
 */
public class TimeWindowSample {

    public static void main(String[] args) throws InterruptedException {

        //Create Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        SiddhiContext siddhiContext = new SiddhiContext();

        //Siddhi Application
        String siddhiApp = "" +
                "define stream StockEventStream (symbol string, price float, volume long); " +
                " " +
                "@info(name = 'query1') " +
                "from StockEventStream#window.time(5 sec)  " +
                "select symbol, sum(price) as price, sum(volume) as volume " +
                "group by symbol " +
                "insert into AggregateStockStream ;";

//        System.out.println(SiddhiCompiler.parse(siddhiApp).toString());
//        System.out.println(SiddhiCompiler.parse(siddhiApp).getQueryContextEndIndex());
//        System.out.println(SiddhiCompiler.parse(siddhiApp).getFunctionDefinitionMap());

//        System.out.println(siddhiContext.getStatisticsConfiguration());
//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).toString());

//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).getStreamDefinitionMap());
//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).getTableDefinitionMap());
//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).getWindowDefinitionMap());
//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).getAggregationDefinitionMap());
//
//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).getStreamJunctions());
//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).getSourceMap());
//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).getSinkMap());
//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).getTableMap());
//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).getWindowMap());
//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).getAggregationMap());
//        System.out.println(SiddhiAppParser.parse(SiddhiCompiler.parse(siddhiApp), siddhiApp, siddhiContext).getLockSynchronizer());

        //Generate runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        System.out.println(siddhiAppRuntime.getStreamDefinitionMap());
        System.out.println(siddhiAppRuntime.getTableDefinitionMap());
        System.out.println(siddhiAppRuntime.getWindowDefinitionMap());
        System.out.println(siddhiAppRuntime.getAggregationDefinitionMap());
        System.out.println(siddhiAppRuntime.getName());
        System.out.println(siddhiAppRuntime.getQueryNames());
        System.out.println(siddhiAppRuntime.getSources());
        System.out.println(siddhiAppRuntime.getSinks());
        System.out.println(siddhiAppRuntime.getTables());
        System.out.println(siddhiAppRuntime.getPartitionedInnerStreamDefinitionMap());
        System.out.println(siddhiAppRuntime.getStatisticsLevel());

        //Add callback to retrieve output events from stream
        siddhiAppRuntime.addCallback("AggregateStockStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        //Retrieve input handler to push events into Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockEventStream");

        //Start event processing
        siddhiAppRuntime.start();

        //Send events to Siddhi
        inputHandler.send(new Object[]{"IBM", 100f, 100L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"IBM", 200f, 300L});
        inputHandler.send(new Object[]{"WSO2", 60f, 200L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"WSO2", 70f, 400L});
        inputHandler.send(new Object[]{"GOOG", 50f, 30L});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"IBM", 200f, 400L});
        Thread.sleep(2000);
        inputHandler.send(new Object[]{"WSO2", 70f, 50L});
        Thread.sleep(2000);
        inputHandler.send(new Object[]{"WSO2", 80f, 400L});
        inputHandler.send(new Object[]{"GOOG", 60f, 30L});
        Thread.sleep(1000);

        //Shutdown the runtime
        siddhiAppRuntime.shutdown();

        //Shutdown Siddhi Manager
        siddhiManager.shutdown();

    }
}
