package com.cjun.nontransactional;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.state.StateFactory;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * @author xuer
 * @date 2014-9-28 - 下午4:14:06
 * @Description 非事务型Trident,这个例子根本没有用到storm trident的实质
 */
public class StormTopo {

  /**
   * @Title: main
   * @Description: TODO
   * @param args
   * @return: void
   */
  public static void main(String[] args) {
    Config config = new Config();
    config.setMaxSpoutPending(20);
    if (args == null || args.length <= 0) {
      LocalCluster localCluster = new LocalCluster();
      localCluster.submitTopology("localModel", config, buildTopology());
    } else {
      try {
        StormSubmitter.submitTopology("clusterModel", config, buildTopology());
      } catch (AlreadyAliveException | InvalidTopologyException e) {
        e.printStackTrace();
      }
    }
  }

  public static StormTopology buildTopology() {

    TridentTopology topo = new TridentTopology();
    MongodbComponent mongodbComponent =
        new MongodbComponent("192.168.0.90", 27017, "admin", "admin", "admin");

    TridentSpout spout = new TridentSpout();
    StateFactory stateFactory = new MyStateFactory(mongodbComponent);

    topo.newStream("spout1", spout).each(new Fields("Log"), new EmitDate(), new Fields("date"))
        .project(new Fields("Log", "date")).groupBy(new Fields("date"))
        .persistentAggregate(stateFactory, new Count(), new Fields("count"));
    // 这里只吧groupBy字段传递到get，put当做条件,所以想要条件多一点，在groupBy后面需多加一点条件，
    // 但是如果加了条件的话，Log是个对象，会导致所有的分组都不一样

    return topo.build();
  }
}
