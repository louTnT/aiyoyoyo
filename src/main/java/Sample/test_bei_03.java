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
public class test_bei_03 {

    public static void main(String[] args) throws InterruptedException {

        // Create Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();

        //Siddhi Application
//        String siddhiApp = "" +
////                "@source(type='file',mode='TEXT.FULL',tailing='false',file.uri='/Users/lou/Desktop/3.txt',action.after.process='delete',@map(type='json'))\n"+
//                "@source(type='file',mode='TEXT.FULL',tailing='false',file.uri='file:/Users/lou/Desktop/3.txt'," +
//                "@map(type='json'," +
//                "@attributes(UserName = \"message.UserName\", IPAddress = \"message.IPAddress\", id = \"id\")))\n" +
//                "define stream InputStream (UserName string, IPAddress string, id long);\n" +
//                "" +
//                "from InputStream\n" +
//                "select UserName, IPAddress, id\n" +
//                "insert into OutputStream;\n";

//        String siddhiApp = "" +
//                "@source(type='file',mode='text.full',tailing='false',file.uri='file:/Users/lou/Desktop/3.txt',@map(type='json',enclosing.element=\"\",),@attributes(name = \"name\", age=\"age\"))\n" +
//
//                "define stream InputStream (name string, age int, country string);\n" +
//                "" +
//                //"@sink(type='file', @map(type='json'), append='true', file.uri='file:/Users/lou/Desktop/bei/4.txt')\n" +
//                //"define stream OutputStream (name string, age int);\n" +
//                //"" +
//                "from InputStream\n" +
//                "select name, age\n" +
//                "insert into OutputStream;\n";

        String siddhiApp = "" +
                "@source(type=\"file\",mode=\"text.full\",tailing=\"false\",file.uri=\"file://Users/lou/Desktop/4/4.txt\",@map(type=\"json\"))\n" +
                "define stream InputStream (symbol string, price float, volume long);\n" +
                "" +
                "@sink(type='file', @map(type='text'), append='true', file.uri='file:/Users/lou/Desktop/4.txt')\n" +
                "define stream OutputStream (symbol string, price float, volume long);\n" +
                //"" +
                "from InputStream\n" +
                "select symbol, price, volume\n" +
                "insert into OutputStream;\n";



        //System.out.println(SiddhiCompiler.parse(siddhiApp).toString());
        //Generate runtime
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        //Adding callback to retrieve output events from stream
        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(toMap(events));
                EventPrinter.print(events);
                //To convert and print event as a map
            }
        });
        System.out.println("11");

        //Get InputHandler to push events into Siddhi
//        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");


        //Start processing
        siddhiAppRuntime.start();


        //Sending events to Siddhi
//        inputHandler.send(new Object[]{"IBM", 700f, 100L});
//        inputHandler.send(new Object[]{"WSO2", 60.5f, 200L});
//        inputHandler.send(new Object[]{"GOOG", 50f, 30L});
//        inputHandler.send(new Object[]{"IBM", 76.6f, 400L});
//        inputHandler.send(new Object[]{"WSO2", 45.6f, 50L});
//        inputHandler.send(new Object[]{"@timestamp":"2019-01-23T00:09:14.592Z","module":"03","host":"84.17.5.3","id":"1548173354592000","type":"beixinyuan","message":{"ChangeTime":"2019-01-22 12:36:20","UserName":"ZJ","HardName":"GQ","MacAddress":"94C6917F8283","OLD1":"","NEW1":"PLDS DVD-RW DA8AESH ATA Device","ClassName":"SWBGT","Tel":"","DeptName":"","IPAddress":"84.18.1.186","LogonUserName":"lenovo","OfficeName":"","DeviceName":"LENOVO-PC"}});
//        Thread.sleep(500);

        //Shutdown runtime
        siddhiAppRuntime.shutdown();

        //Shutdown Siddhi Manager
        siddhiManager.shutdown();
        System.out.println("22");

    }
}



