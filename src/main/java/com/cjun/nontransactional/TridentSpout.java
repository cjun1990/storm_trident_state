package com.cjun.nontransactional;

import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author xuer
 * @date 2014-9-28 - 上午10:29:20
 * @Description spout，注意此处的泛型是Long
 */
public class TridentSpout implements ITridentSpout<Long> {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  public boolean flag = true;

  @Override
  public storm.trident.spout.ITridentSpout.BatchCoordinator<Long> getCoordinator(String txStateId,
      Map conf, TopologyContext context) {
    return new MyCoordinator();
  }

  @Override
  public storm.trident.spout.ITridentSpout.Emitter<Long> getEmitter(String txStateId, Map conf,
      TopologyContext context) {
    return new MyEmitter();
  }

  @Override
  public Map getComponentConfiguration() {
    return null;
  }

  /**
   * @Title: getOutputFields
   * @Description: TODO
   * @return spout发射的字段
   */
  @Override
  public Fields getOutputFields() {
    return new Fields("Log");
  }

  class MyCoordinator implements ITridentSpout.BatchCoordinator<Long> {

    /**
     * @Title: initializeTransaction
     * @Description: 初始化事务方法，这里直接返回null，不知可以否
     */
    @Override
    public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {
      return null;
    }

    @Override
    public void success(long txid) {}

    @Override
    public boolean isReady(long txid) {
      return true;
    }

    @Override
    public void close() {}
  }

  class MyEmitter implements ITridentSpout.Emitter<Long> {

    @Override
    public void emitBatch(TransactionAttempt tx, Long coordinatorMeta, TridentCollector collector) {
      if (flag) {
        flag = false;
        for (int i = 0; i <= 8; i++) {
          Log log = radamLog(0);
          collector.emit(new Values(log));
        }
      }
      // for (int i = 0; i <= 10; i++) {
      // collector.emit(new Values(radamLog(1)));
      // }
    }

    @Override
    public void success(TransactionAttempt tx) {}

    @Override
    public void close() {}

  }

  /**
   * @Title: radamDateLog
   * @Description: TODO
   * @return
   * @return: 生成随机日期的函数
   */
  public Log radamLog(int i) {
    String[] date =
        {"20140101000000", "20140102000000", "20140103000000", "20140104000000", "20140105000000",
            "20140106000000", "20140107000000", "20140108000000", "20140109000000"};
    String log =
        "BL##ERROR#日期#cmszmonc#pboss#mon#upay_monAbnormalPay.sh##upay_monAbnormalPay#00000000#0###tttt##LB";
    return new Log(date[i], log);
  }

}
