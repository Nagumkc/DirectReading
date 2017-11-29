import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by VenkatNag on 11/27/2017.
 */
public class strspout extends BaseRichSpout
{
    static int j=0;
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
            String s="C:\\Users\\VenkatNag\\Documents\\GitHub\\ml-with-spark\\data\\golf_data.csv";

            if(j==0)
            {
                _collector.emit(new Values(s));
                j++;
            }

        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

}
