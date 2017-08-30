import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by Mayanka on 16-Sep-15.
 */
public class TwitterSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    LinkedBlockingQueue<Status> queue = null;
    TwitterStream _twitterStream;
    String _username;
    String _pwd;
    String consumer_key="bjnYTbDvOd61oShbWWyaD2leP";
    String consumer_secret="mCy4iONQfevhJKmfqK1Kt0GjHb1uG0z2f3VUrhN3upmPZMXgsp";
    String access_token="434908233-C896jBM6t1Iq7ta0FJ93YQeD3GP2439T1lidFXyR";
    String token_secret="JTPffFFLE6Qk1j4c2YTlDeeF8kkjop1Vtw4ANanVS1ivx";
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("Status"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = spoutOutputCollector;
        StatusListener listener = new StatusListener() {

            public void onException(Exception e) {

            }

            public void onStatus(Status status) {
                queue.offer(status);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onTrackLimitationNotice(int i) {

            }

            public void onScrubGeo(long l, long l1) {

            }

            public void onStallWarning(StallWarning stallWarning) {

            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();
        TwitterStreamFactory fact = new TwitterStreamFactory();
        _twitterStream = fact.getInstance();
        _twitterStream.setOAuthConsumer(consumer_key, consumer_secret);
        _twitterStream.setOAuthAccessToken(new AccessToken(access_token, token_secret));
        _twitterStream.addListener(listener);
        _twitterStream.sample();
    }

    public void nextTuple() {
        Status ret = queue.poll();
        if(ret==null) {
            Utils.sleep(50);
        } else {
           String s=ret.getText();
            // emit tuple to next bolt
            _collector.emit(new Values(s));
        }
    }
}
