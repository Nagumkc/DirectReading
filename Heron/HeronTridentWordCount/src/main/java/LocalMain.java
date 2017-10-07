import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by Mayanka on 22-Sep-15.
 */
public class LocalMain {
    public static void main(String args[])
    {
        insertIntoMongoDB("12344",23);

    }

    public static void insertIntoMongoDB(String word,Integer count)
    {
        try {
            URL url = new URL("https://api.mongolab.com/api/1/databases/cs5543/collections/TwitterWordCount?apiKey=FqMHhDW_NfxEBuo6BZ67IlskGbAAdr2Z");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");

            String input = "{\"word\":\""+word+"\",\"count\":\""+count+"\",\"time\":\""+System.currentTimeMillis()+"\"}";

            OutputStream os = conn.getOutputStream();
            os.write(input.getBytes());
            os.flush();

            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                System.out.println("The code is "+ conn.getResponseMessage());
                throw new RuntimeException("Failed : HTTP error code : "
                        + conn.getResponseCode());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(
                    (conn.getInputStream())));

            String output;
            System.out.println("Output from Server .... \n");
            while ((output = br.readLine()) != null) {
                System.out.println(output);
            }

            conn.disconnect();
        }
            catch (Exception e) {

            e.printStackTrace();

        }

    }


}
