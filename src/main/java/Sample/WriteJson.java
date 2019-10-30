package Sample;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteJson {
    public static void main(String[] args) throws IOException {
        File file = new File("/Users/lou/Desktop/4/4.txt");
//        File Write_file = new File("/Users/lou/Desktop/2.txt");
//        FileReader f = new FileReader(file);
        FileWriter f = new FileWriter(file);
//        BufferedReader reader = new BufferedReader(f);
//        final BufferedWriter writer = new BufferedWriter(wf);
        String description = "[\n"
                + "{\"event\":{\"symbol\":\"WSO2\",\"price\":55.6,\"volume\":100}},\n"
                + "{\"event\":{\"symbol\":\"WSO2\",\"price\":56.6,\"volume\":200}},\n"
                + "{\"event\":{\"symbol\":\"WSO2\",\"price\":57.6,\"volume\":300}}\n"
                + "]\n\n";
        f.write(description);
        f.close();
    }
}
