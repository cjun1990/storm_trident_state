package com.cjun.trident;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

public class Send {

  private static final String TASK_QUEUE_NAME = "logqueue_cjun";// "logqueue";

  @SuppressWarnings("unused")
  public static void main(String[] argv) throws Exception {

    RabbitMQComponent rabbitMQComponent =
        new RabbitMQComponent("192.168.0.33", 5672, "root", "123456", "log_monitor_mq");

    Channel channel = rabbitMQComponent.getChannel();

    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, false, null);

    String noFile =
        "BL##WARNING#20131224160235#cmszmonc#upay#mon#uPaymonFileCnt.sh##uPaymonFileCnt#00000000#11555011###222##LB";// getMessage(argv);
    String hebing =
        "BL##ERROR#20131201000001#cmszmonc#upay#mon#upay_monAbnormalPay.sh##upay_monAbnormalPay#00000000#0###合并测试1##LB";// 合并
                                                                                                                        // 合并测试
    String hebing2 =
        "BL##ALARM#20131201000001#cmszmonc#pboss#mon#upay_monAbnormalPay.sh##upay_monAbnormalPay#00000000#0###abcdefg##LB";
    String isfile =
        "BL##ERROR#20131201000001#cmszmonc#pboss#mon#upay_monAbnormalPay.sh##upay_monAbnormalPay#00000000#0###tttt##LB";// 过滤
    String error = "发的发大幅度23jfdkfdjkfdkdfdkfjdfdsdf123121edfdf";
    long start = System.currentTimeMillis();
    int count = 10;
    for (int i = 0; i <= count; i++) {
      // Thread.sleep(100);
      // int c = new Random().nextInt(2);
      // if (i < 10) {
      String msg = hebing;
      channel.basicPublish("", TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, Base64Util
          .encode(msg.getBytes("utf-8")).getBytes());
      /*
       * } else if (i >= 10 && i < 20) { String msg = noFile; channel.basicPublish("",
       * TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, msg.getBytes("GBK")); } else {
       * String msg = noFile; channel.basicPublish("", TASK_QUEUE_NAME,
       * MessageProperties.PERSISTENT_TEXT_PLAIN, msg.getBytes("GBK")); }
       */

    }
    // channel.queuePurge(TASK_QUEUE_NAME);
    // channel.queuePurge(TASK_QUEUE_NAME);
    // System.out.println(System.currentTimeMillis());
    System.out.println("发送【" + count + "】条消息耗时:" + (System.currentTimeMillis() - start) + "ms");

    channel.close();
  }

}
