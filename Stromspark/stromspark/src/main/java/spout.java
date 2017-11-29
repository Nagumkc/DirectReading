import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.Map;

/**
 * Created by VenkatNag on 9/29/2017.
 */
public class spout extends BaseRichSpout {
static int i=0;
static int size=0;
    SpoutOutputCollector _collector;
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("Status"));
    }
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        _collector = spoutOutputCollector;
    }
    public void nextTuple() {
        try {
          //  BufferedReader br = new BufferedReader(new FileReader("data/input"));
            String s="E:\\UMKC\\Sum_May\\KDM\\Week 5\\Yahoo-Questions-traindata.csv";

              if(i==0)
              {
                  _collector.emit(new Values(s));
                  System.out.println(i);
                  i++;
              }

        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
