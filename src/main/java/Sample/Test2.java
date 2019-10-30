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



//一个输入流被多次使用，封装成一个方法即可；




public class Test2 {
    public static void main(String[] args) throws InterruptedException, IOException {
        SiddhiManager siddhiManager = new SiddhiManager();


        String siddhiApp =
                "define stream InputStream (weight int);\n" +
                        "@info(name='HelloWorldQuery')\n" +
                        "from InputStream#window.length(3)\n" +
                        "select weight, avg(weight) as averageWeight\n" +
                        "insert into OutputStream;\n";



        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);


        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });


        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");


        siddhiAppRuntime.start();


        File file = new File("/Users/lou/Desktop/1.txt");
        FileReader f = new FileReader(file);
        BufferedReader reader = new BufferedReader(f);
        String line = null;
        //Object[] obj = new Object[3];
        Object[] obj = new Object[1];

        while ((line = reader.readLine()) != null) {

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

        siddhiAppRuntime.shutdown();
        String siddhiApp2 =
                "define stream InputStream (weight int);\n" +
                        "@info(name='HelloWorldQuery')\n" +
                        "from InputStream#window.length(3)\n" +
                        "select weight\n" +
                        "insert into OutputStream;\n";


        siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp2);


        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });


         inputHandler = siddhiAppRuntime.getInputHandler("InputStream");


        siddhiAppRuntime.start();


        file = new File("/Users/lou/Desktop/1.txt");
         f = new FileReader(file);
        reader = new BufferedReader(f);
         line = null;
        //Object[] obj = new Object[3];
        obj = new Object[1];

        while ((line = reader.readLine()) != null) {

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

        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();


    }
}