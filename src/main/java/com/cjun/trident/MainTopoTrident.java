package com.cjun.trident;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

/**
 * @author xuer
 * @date 2014-9-23 - 上午9:25:56
 * @Description storm trident 自定义state,存储mongodb数据库
 */
public class MainTopoTrident {
  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException,
      InterruptedException {
    Config config = new Config();
    // 同时活跃的batch数量：你必须设置同时处理的batch数量。你可以通过”topology.max.spout.pending” 来指定， 如果你不指定，默认是1。
    config.setMaxSpoutPending(20);
    if (args != null && args.length <= 0) {
      LocalCluster localCluster = new LocalCluster();
      LocalDRPC drpc = new LocalDRPC();
      localCluster.submitTopology("myTridentstate", config, bulidStormTopo(drpc));
      Thread.sleep(5000);
      for (int i = 0; i < 100; i++) {
        System.err.println("调用drpc结果" + i + ":" + drpc.execute("count", "20131201000001"));
      }
    } else {
      StormSubmitter.submitTopology("myTridentstate", config, bulidStormTopo(null));
    }
  }

  public static StormTopology bulidStormTopo(LocalDRPC localDRPC) {

    TridentTopology topo = new TridentTopology();

    RabbitMQComponent rabbitMQComponent =
        new RabbitMQComponent("192.168.0.33", 5672, "root", "123456", "log_monitor_mq");

    RabbitmqSpout spout = new RabbitmqSpout(new Fields("log"), rabbitMQComponent, "logqueue_cjun");
    // MyLogSpout spout = new MyLogSpout(new Fields("log"), rabbitMQComponent, "logqueue_cjun");

    MongodbComponent mongodbComponent =
        new MongodbComponent("192.168.0.33", 27017, "admin", "admin", "admin");

    StateFactory stateFactory = MongodbState.opaque(mongodbComponent);
    // StateFactory stateFactory = MongodbState.transactional(mongodbComponent);
    // StateFactory stateFactory = MongodbState.noTransactional(mongodbComponent);

    // 这里的理解就是：在newStream("iTridentSpout",
    // spout).parallelismHint(16)时，new了一个stream，并行度是16，在newStream这个node处理完了之后，
    // 交给后面的又是一个stream,然后执行.each(new Fields("log"), new MySplitFunction("#"), new
    // Fields("date")).parallelismHint(16)，也是一个each的node，
    // 在这个node中时也可以设置.parallelismHint(16),如果没有设置，那么就用默认的并发度

    // 如果spout的并行度是16，spout不是从同一个数据源取，那么就会有16个独立的spout发数据，而不是你想象的只会发送一份数据
    TridentState state = topo.newStream("iTridentSpout", spout)
    // .parallelismHint(5)
        .each(new Fields("log"), new MySplitFunction("#"), new Fields("date")).parallelismHint(10)// 这里设置并发，对each和newStream起作用
        // each函数会把输出的字段放到输入的字段后面一起输出
        .groupBy(new Fields("date"))
        // new Fields("log"),
        .persistentAggregate(stateFactory, new Count(), new Fields("count")).parallelismHint(20);

    topo.newDRPCStream("count", localDRPC)
    // .each(new Fields("args"), new MySplitFunction(" "), new Fields("date"))
    // 把传递进来的需要查询的日期先分组，在求数量
    // .groupBy(new Fields("date"))
    // 查询出每个日期的结果
        .stateQuery(state, new Fields("args"), new MapGet(), new Fields("count"));
    // .each(new Fields("count"), new FilterNull());// 如果这里没有filter，那么会导致nullpointexception
    // 每个日期的结果求和
    // .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

    return topo.build();
  }
}
