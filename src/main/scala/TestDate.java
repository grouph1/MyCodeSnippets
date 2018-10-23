import javax.xml.crypto.Data;
import java.util.Date;

public class TestDate {


    public static void main(String [] args)
    {
        System.out.println(new Date(2018,10,11).getTime());
        System.out.println(new Date(2017,10,11));
        System.out.println(new Date().getTime());
        System.out.println(new Date(1535666374894L));

    }

}
