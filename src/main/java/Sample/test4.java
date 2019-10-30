package Sample;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.function.Script;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;


public class test4 {
    public static void main(String[] args) throws InterruptedException, IOException {
        SiddhiManager siddhiManager = new SiddhiManager();

//        String siddhiApp =
//                "define stream InputStream (weight int);\n" +
//                        "@info(name='HelloWorldQuery')\n" +
//                        "from InputStream#window.length(3)\n" +
//                        "select weight, avg(weight) as averageWeight\n" +
//                        "insert into OutputStream;\n";


//        String siddhiApp = "" +
//                "define function diff[scala] return int {\n" +
//                "    var set1 = data[0]\n" +
//                "    val set2 = Set(1,2)\n" +
//                "    if (set1.diff(set2) isEmpty )\n" +
//                "      return 0\n" +
//                "    else\n" +
//                "      return 1\n" +
//                "};\n" +
//                "from InputStream\n" +
//                "select createSet(amount) as set_proc\n" +
//                "insert into evt1;\n" +
//                "\n" +
//                "from evt1#window.lengthBatch(3)\n" +
//                "select unionSet(set_proc) as a\n" +
//                "having diff(a) > 0" +
//                "insert into OutputStream;\n";
//                "\n" +
//                "from Out#window.lengthBatch(1)\n" +
//                "select unionSet(a) as aa\n" +
//                "insert into OutputStream;";

        String siddhiApp = "define function diff[javascript] return int {\n" +
                "var set1 = new Set([1,2]),\n" +
                "    subset = new Set([2,3]);\n"+
//                "  subset = data[0];\n" +
                "for (var elem in subset) {\n" +
                "        if (!set1.has(elem)) {\n" +
                "            return 1;\n" +
                "        }\n" +
                "    }\n" +
                "    return 0;"+
                "};\n" +

                "define stream InputStream (amount int);\n" +
                "from InputStream\n" +
                "select createSet(amount) as set_proc\n" +
                "insert into evt1;\n" +
                "\n" +
                "from evt1#window.lengthBatch(3)\n" +
                "select unionSet(set_proc) as a\n" +
                "having diff > 0" +
                "insert into OutputStream;\n";

//        String siddhiApp = "define function concatFn[javascript] return string {\n" +
//                "    var str1 = data[0];\n" +
//                "    var responce = str1  * 10\n" +
//                "if (str1 >5 ) return responce;"+
////                "    return responce;\n" +
//                "};\n" +
//                "\n" +
//                "define stream InputStream(roomNo int);\n" +
//                "\n" +
//                "from InputStream\n" +
//                "select concatFn(roomNo) as id\n" +
//                "insert into OutputStream;";

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
        siddhiManager.shutdown();


    }
}