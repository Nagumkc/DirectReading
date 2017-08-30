import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.log4j.BasicConfigurator;

/**
 * Created by Mayanka on 16-Sep-15.
 */
public class StormMain {
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
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count",createConfig(true),createTopology());
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            cluster.shutdown();
        }
    }


    private static StormTopology createTopology()
    {

        TopologyBuilder topology = new TopologyBuilder();

/**Sample Text Spout**/

        //  topology.setSpout("SampleSpout", new StormSpout(), 4);

        // topology.setBolt("SplitSentence", new SplitSentenceBolt(), 4).shuffleGrouping("SampleSpout");

/**Twitter Spout**/
        topology.setSpout("TwitterSpout",new TwitterSpout(),4);

        topology.setBolt("SplitSentence", new SplitSentenceBolt(), 4).shuffleGrouping("TwitterSpout");


        topology.setBolt("WordCount",new WordCountBolt(),4).shuffleGrouping("SplitSentence");

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
