package Sample;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;

import java.io.*;
import java.lang.reflect.Array;
import java.util.Arrays;


//一个输入流被多次使用，封装成一个方法即可；


public class Test3 {


    public static void main(String[] args) throws InterruptedException, IOException {

        File file = new File("/Users/lou/Desktop/1.txt");
        File Write_file = new File("/Users/lou/Desktop/2.txt");
        FileReader f = new FileReader(file);
        FileWriter wf = new FileWriter(Write_file);
        BufferedReader reader = new BufferedReader(f);
        final BufferedWriter writer = new BufferedWriter(wf);

//        writer.write("jkashdjkashdaskdjh");
//        writer.close();

        SiddhiManager siddhiManager = new SiddhiManager();

//        String siddhiApp =
//                "define stream InputStream (weight int);\n" +
//                        "@info(name='HelloWorldQuery')\n" +
//                        "from InputStream#window.length(3)\n" +
//                        "select weight, avg(weight) as averageWeight\n" +
//                        "insert into OutputStream;\n";



        String siddhiApp = "define stream InputStream (amount int);\n" +
                "from InputStream#window.lengthBatch(3)\n" +
                "select createSet(amount) as set_proc\n" +
                "insert into evt1;\n" +
                "\n" +
                "from evt1#window.lengthBatch(3)\n" +
                "select unionSet(set_proc) as a\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                //EventPrinter.print(toMap(events));
//                System.out.println(Arrays.deepToString(events));
                Event ev = new Event();
                ev.getData();

                try {
                    writer.write(Arrays.deepToString(toMap(events)));
                    writer.newLine();
                }
                catch (IOException e){
                    //System.out.println("noob");
                    e.printStackTrace();
                }
            }
        });


        siddhiAppRuntime.start();

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
        reader.close();




        siddhiAppRuntime.shutdown();
        writer.close();
        siddhiManager.shutdown();

    }
}