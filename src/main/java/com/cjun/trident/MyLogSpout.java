package com.cjun.trident;

import java.util.Map;
import java.util.Random;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.rabbitmq.client.QueueingConsumer;

public class MyLogSpout implements ITridentSpout<MyMetadata> {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  public int flag = 1;

  /**
   * @fieldName: _offset
   * @fieldType: int
   * @Description: 数据发送的偏移量,每批次处理条数
   */
  private static final int _offset = 2;

  private Fields _fields;

  private RabbitMQComponent _rabbitMQComponent;

  private String _queueName;

  public static int count = 1;

  public MyLogSpout() {}

  /**
   * @Title:MyLogSpout
   * @Description:TODO
   * @param fields
   * @param rabbitMQComponent
   * @param queueName 此spout去消费的queue名称
   */
  public MyLogSpout(Fields fields, RabbitMQComponent rabbitMQComponent, String queueName) {
    this._fields = fields;
    this._rabbitMQComponent = rabbitMQComponent;
    this._queueName = queueName;
  }

  @Override
  public BatchCoordinator<MyMetadata> getCoordinator(String txStateId, Map conf,
      TopologyContext context) {
    return new MyCoordinator();
  }

  @Override
  public Emitter<MyMetadata> getEmitter(String txStateId, Map conf, TopologyContext context) {
    // 前面设置了多少并发，这里就会执行多少遍
    return new MyEmitter();
  }

  @Override
  public Map getComponentConfiguration() {
    return null;
  }

  @Override
  public Fields getOutputFields() {
    return _fields;
  }

  /**
   * @author xuer
   * @date 2014-9-19 - 上午10:31:56
   * @Description spout的Coordinator
   */
  public class MyCoordinator implements ITridentSpout.BatchCoordinator<MyMetadata> {

    /**
     * @Title: initializeTransaction
     * @Description: TODO
     * @param txid
     * @param prevMetadata
     * @param currMetadata
     * @return 当前事务处理的元数据
     */
    @Override
    public MyMetadata initializeTransaction(long txid, MyMetadata prevMetadata,
        MyMetadata currMetadata) {
      if (prevMetadata == null || prevMetadata.getIndex() <= 0) {
        currMetadata = new MyMetadata();
        currMetadata.setIndex(0);
        currMetadata.setOffset(_offset);
      } else {
        long newIndex = prevMetadata.getIndex() + prevMetadata.getOffset();
        currMetadata.setIndex(newIndex);
        currMetadata.setOffset(_offset);
      }
      return currMetadata;
    }

    @Override
    public void success(long txid) {}

    @Override
    public boolean isReady(long txid) {
      try {
        Thread.sleep(40000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (count > 2) {
        return false;
      } else {
        count++;
        return true;
      }
      // return true;
    }

    @Override
    public void close() {}

  }

  private QueueingConsumer amqpConsumer;

  /**
   * @author xuer
   * @date 2014-9-19 - 上午10:42:28
   * @Description spout的emitter
   */
  public class MyEmitter implements ITridentSpout.Emitter<MyMetadata> {

    @Override
    public void emitBatch(TransactionAttempt tx, MyMetadata coordinatorMeta,
        TridentCollector collector) {
      for (int i = 0; i < coordinatorMeta.getOffset(); i++) {
        collector.emit(new Values(radamDateLog()));
      }
      // if (amqpConsumer == null) {
      // return;
      // }
      // QueueingConsumer.Delivery delivery;
      // try {
      // for (int i = 0; i < coordinatorMeta.getOffset(); i++) {
      // delivery = amqpConsumer.nextDelivery(1L);
      // if (delivery == null) {
      // continue;
      // }
      // final byte[] message = delivery.getBody();
      // String logInfo = new String(message, Charset.forName("utf-8"));
      // byte[] msgByte = null;
      // try {
      // msgByte = Base64Util.decode(logInfo);
      // } catch (UnsupportedEncodingException e) {
      // Log.error("RabbitMQSpout数据解密异常..." + e);
      // continue;
      // }
      // String logMsg;
      // try {
      // logMsg = new String(msgByte, "utf-8");
      // } catch (UnsupportedEncodingException e) {
      // e.printStackTrace();
      // continue;
      // }
      //
      // collector.emit(new Values(logMsg));
      // }
      // } catch (ShutdownSignalException | ConsumerCancelledException | InterruptedException e) {
      // System.out.println("定义QueueingConsumer.Delivery时异常");
      // e.printStackTrace();
      // }

    }

    @Override
    public void success(TransactionAttempt tx) {
      System.err.println("TransactionAttempt" + tx);
    }

    @Override
    public void close() {}
  }

  public int get_offset() {
    return _offset;
  }

  /**
   * @Title: radamDateLog
   * @Description: TODO
   * @return
   * @return: 生成随机日期的函数
   */
  public String radamDateLog() {
    String[] date =
        {"20140101000000", "20140102000000", "20140103000000", "20140104000000", "20140105000000",
            "20140106000000", "20140107000000", "20140108000000", "20140109000000"};

    Random random = new Random();

    String log =
        "BL##ERROR#"
            + date[random.nextInt(9)]
            + "#cmszmonc#pboss#mon#upay_monAbnormalPay.sh##upay_monAbnormalPay#00000000#0###tttt##LB";

    return log;
  }

  public Fields get_fields() {
    return _fields;
  }

  public void set_fields(Fields _fields) {
    this._fields = _fields;
  }
}
