

import java.io.PrintWriter;

public class CharsetTest {

    public static void main(String [] args) throws Exception
    {
        String line1="This is test string ÄÖü";
        PrintWriter writer = new PrintWriter("C://tmp//test3.txt");
        writer.println(line1);
        writer.println("The second line");
        writer.close();
    }
}
