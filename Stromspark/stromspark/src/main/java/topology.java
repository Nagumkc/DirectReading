import org.apache.log4j.BasicConfigurator;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import scala.Serializable;

/**
 * Created by VenkatNag on 10/7/2017.
 */
public class topology  {
    public static void main(String args[]) {


        BasicConfigurator.configure();

        if (args != null && args.length > 0) {
            try {
                StormSubmitter.submitTopology(args[0],createConfig(false),createTopology());
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            try{
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("spark-storm",createConfig(true),createTopology());
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                cluster.shutdown();
            }catch (Exception e)
            {
                e.printStackTrace();
            }

        }


    }


    private static StormTopology createTopology()
    {

        TopologyBuilder topology = new TopologyBuilder();

/**Sample Text Spout**/

        //  topology.setSpout("SampleSpout", new StormSpout(), 4);

        // topology.setBolt("SplitSentence", new SplitSentenceBolt(), 4).shuffleGrouping("SampleSpout");

/**Twitter Spout**/
     /*  topology.setSpout("Spout",new spout());
        topology.setBolt("Bolt", new decisionbolt()).globalGrouping("Spout");
      topology.setSpout("recg",new recgspout());
      topology.setBolt("Recognition", new recogbolt()).globalGrouping("recg");
*/
     topology.setSpout("Spout",new strspout());
     topology.setBolt("Bolt",new csvdatamodel()).globalGrouping("Spout");
     topology.setSpout("recgspout",new strrecgspout());
     topology.setBolt("recgbolt",new csvrecg()).globalGrouping("recgspout");
        // topology.setBolt("WordCount",new WordCountBolt(),4).shuffleGrouping("SplitSentence");

        return topology.createTopology();
    }

    private static Config createConfig(boolean local)
    {
        int workers = 1;
        Config conf = new Config();
        conf.setDebug(true);
        if (local)
            conf.setMaxTaskParallelism(workers);
        else
            conf.setNumWorkers(workers);
        return conf;
    }

}

