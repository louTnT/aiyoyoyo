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
/**
 * source不可用
 * sink可用 text和json格式均能输出
 *
 */

public class test5 {


    public static void main(String[] args) throws InterruptedException, IOException {

        File file = new File("/Users/lou/Desktop/1.txt");
//        File Write_file = new File("/Users/lou/Desktop/2.txt");
        FileReader f = new FileReader(file);
//        FileWriter wf = new FileWriter(Write_file);
        BufferedReader reader = new BufferedReader(f);
//        final BufferedWriter writer = new BufferedWriter(wf);

//        writer.write("jkashdjkashdaskdjh");
//        writer.close();

        SiddhiManager siddhiManager = new SiddhiManager();

//        String siddhiApp =
//                "define stream InputStream (weight int);\n" +
//                        "@info(name='HelloWorldQuery')\n" +
//                        "from InputStream#window.length(3)\n" +
//                        "select weight, avg(weight) as averageWeight\n" +
//                        "insert into OutputStream;\n";

//        @sink(type="file", file.uri="<STRING>", append="<BOOL>", add.line.separator="<BOOL>", @map(...)))
//
//        @sink(type='file', @map(type='json'), append='false', file.uri='/Users/lou/Desktop/3.txt') define stream BarStream (symbol string, price float, volume long);
//
//        @source(type="file", dir.uri="<STRING>", file.uri="<STRING>", mode="<STRING>", tailing="<BOOL>", action.after.process="<STRING>", action.after.failure="<STRING>", move.after.process="<STRING>", move.after.failure="<STRING>", begin.regex="<STRING>", end.regex="<STRING>", file.polling.interval="<STRING>", dir.polling.interval="<STRING>", timeout="<STRING>", @map(...)))
//
//        @source(type='file',
//                mode='text.full',
//                tailing='false'
//                dir.uri='file://abc/xyz',
//                action.after.process='delete',
//                @map(type='json'))
//        define stream FooStream (symbol string, price float, volume long);


        String siddhiApp = ""+
                //"@source(type='file',mode='text.full',tailing='false',file.uri='/Users/lou/Desktop/3.txt',action.after.process='delete',@map(type='text'))\n"+
                "define stream InputStream (amount int);\n" +
                "@sink(type='file', append='false', file.uri='/Users/lou/Desktop/3.txt', @map(type='json'))\n" +
                "define stream OutputStream (amount int);\n" +
                "from InputStream#window.lengthBatch(3)\n" +
                "select amount\n" +
                "insert into  OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);

        InputHandler inputHandler = siddhiAppRuntime.getInputHandler("InputStream");

        siddhiAppRuntime.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                //EventPrinter.print(toMap(events));
//                System.out.println(Arrays.deepToString(events));

//                try {
//                    writer.write(Arrays.deepToString(toMap(events)));
//                    writer.newLine();
//                }
//                catch (IOException e){
//                    //System.out.println("noob");
//                    e.printStackTrace();
//                }
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
        //writer.close();
        siddhiManager.shutdown();

    }
}