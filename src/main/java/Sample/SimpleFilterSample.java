package Sample;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.config.SiddhiContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiAppRuntimeBuilder;
import io.siddhi.core.util.parser.SiddhiAppParser;
import io.siddhi.query.compiler.SiddhiCompiler;

/**
 * The sample demonstrate how to use Siddhi within another Java program.
 * This sample contains a simple filter query.
 */
public class SimpleFilterSample {

    public static void main(String[] args) throws InterruptedException {

        // Create Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        SiddhiContext siddhiContext = new SiddhiContext();
//        private SiddhiContext siddhiContext;
//        public SiddhiManager() {
//            siddhiContext = new SiddhiContext();
//        }

        //Siddhi Application
        String siddhiApp = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "" +
                "@info(name = 'query1') " +
                "from StockStream[volume < 150] " +
                "select symbol, price " +
                "insert into OutputStream;";

        System.out.println(SiddhiCompiler.parse(siddhiApp).toString());
        System.out.println(SiddhiCompiler.parse(siddhiApp).getQueryContextEndIndex());
        System.out.println(SiddhiCompiler.parse(siddhiApp).getFunctionDefinitionMap());

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


//        System.out.println(siddhiAppRuntime.getStreamDefinitionMap());
//        System.out.println(siddhiAppRuntime.getTableDefinitionMap());
//        System.out.println(siddhiAppRuntime.getWindowDefinitionMap());
//        System.out.println(siddhiAppRuntime.getAggregationDefinitionMap());
//        System.out.println(siddhiAppRuntime.getName());
//        System.out.println(siddhiAppRuntime.getQueryNames());
//        System.out.println(siddhiAppRuntime.getSources());
//        System.out.println(siddhiAppRuntime.getSinks());
//        System.out.println(siddhiAppRuntime.getTables());
//        System.out.println(siddhiAppRuntime.getPartitionedInnerStreamDefinitionMap());
//        System.out.println(siddhiAppRuntime.getStatisticsLevel());



        //Adding callback to retrieve output events from stream
        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(toMap(events));
                //EventPrinter.print(events);
                //To convert and print event as a map
                EventPrinter.print(toMap(events));
            }
        });

        //Get InputHandler to push events into Siddhi
        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockStream");


        //Start processing
        siddhiAppRuntime.start();


        //Sending events to Siddhi
        inputHandler.send(new Object[]{"IBM", 700f, 100L});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
        inputHandler.send(new Object[]{"GOOG", 50f, 30L});
        inputHandler.send(new Object[]{"IBM", 76.6f, 400L});
        inputHandler.send(new Object[]{"WSO2", 45.6f, 50L});
        Thread.sleep(500);


        //System.out.println(inputHandler.getStreamId());

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

        //Shutdown runtime
        siddhiAppRuntime.shutdown();

        //Shutdown Siddhi Manager
        siddhiManager.shutdown();

    }
}
