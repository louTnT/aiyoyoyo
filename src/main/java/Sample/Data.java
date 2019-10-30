package Sample;


import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Query 1
 *
 */
public class Data {
    public static void main(String[] args) throws InterruptedException, IOException {
        SiddhiManager siddhiManager = new SiddhiManager();

//        String siddhiApp = "" +
//                "define stream DataStream (symbol string, price float, volume long); " +
//                "" +
//                "@info(name = 'query1') " +
//                "from DataStream[volume < 150] " +
//                "select symbol, price " +
//                "insert into OutputStream;";

        String siddhiApp = "" +
                "define stream InputStream (amount int); " +
                "" +
                "@info(name = 'query1') " +
                "from InputStream#window.lengthBatch(3) " +
                "select avg(amount) as avg_amount " +
                "insert into evt;" +
                "" +
                "from every ss0=evt,ss1=evt,ss2=evt[(ss2.avg_amount>(ss2.avg_amount+ss1.avg_amount+ss0.avg_amount)/3) and (ss2.avg_amount>1)] " +
                "select ss0.avg_amount as ss0avg_amount0, ss1.avg_amount as ss0avg_amount1, ss2.avg_amount as ss0avg_amount2 " +
                "insert into OutputStream;";

//        String siddhiApp = "" +
//                "define stream InputStream (amount int);\n" +
//                "@info(name = 'query1') " +
//                "from InputStream#window.lengthBatch(3)\n" +
//                "select avg(amount) as avg_amount\n" +
//                "insert into evt;\n" +
//                "@info(name = 'query2') " +
//                "from every ss0=evt,ss1=evt,ss2=evt[(ss2.avg_amount>(ss2.avg_amount+ss1.avg_amount+ss0.avg_amount)/3) and (ss2.avg_amount>1)]\n" +
//                "select ss0.avg_amount,ss1.avg_amount,ss2.avg_amount\n" +
//                "insert into OutputStream;" ;

//        String siddhiApp =
//                "define stream InputStream (weight int);\n" +
//                "\n" +
//                "@sink(type='log', prefix='LOGGER')\n" +
//                "define stream OutStream(weight int, averageWeight double);\n" +
//                "\n" +
//                "@sink(type='log', prefix='LOGGER')\n" +
//                "define stream OutputStream(weight int, totalWeight long, averageWeight double);\n" +
//                "\n" +
//                "@info(name='HelloWorldQuery')\n" +
//                "from InputStream#window.length(3)\n" +
//                "select weight, avg(weight) as averageWeight\n" +
//                "insert into OutStream;\n" +
//                "\n" +
//                "@info(name='HelloWorldQuery2')\n" +
//                "from InputStream#window.length(3)\n" +
//                "select weight, sum(weight) as totalWeight, avg(weight) as averageWeight\n" +
//                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");

        //siddhiAppRuntime.start();

        File file = new File("/Users/lou/Desktop/1.txt");
        FileReader f = new FileReader(file);
        BufferedReader reader = new BufferedReader(f);
        String line = null;
        //Object[] obj = new Object[3];
        Object[] obj = new Object[1];

        while ((line = reader.readLine()) != null) {
//            Object object = tempString;
//            System.out.println(object);
            //inputHandler.send(new Object[]{tempString});
            //int i = 0;
            String[] s = line.split(" ");
//            obj[0] = s[0];
////            obj[1] = Float.parseFloat(s[1]);
////            obj[2] = Long.parseLong(s[2]);
            obj[0] = Integer.parseInt(s[0]);

//            for(String str : s){
//                obj[i++] = str;
//            }
            //System.out.println(obj);
            inputHandler.send(obj);

        }
        //Thread.sleep(500);

        //siddhiAppRuntime.shutdown();
        //siddhiManager.shutdown();
    }
}
