import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

/**
 * Created by VenkatNag on 10/14/2017.
 */
public class recgspout extends BaseRichSpout {
    static String rec="false";
    SpoutOutputCollector _collector;
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("test"));
    }
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
    }
    public void nextTuple() {
        try {
            BufferedReader br = new BufferedReader(new FileReader("E:\\UMKC\\Sum_May\\KDM\\Week 5\\Yahoo-Question-testdata.csv"));
            String s="";

             while ((s=br.readLine())!=null)
             {
               //  String[] country = s.split(",");
            _collector.emit(new Values(s));}

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
