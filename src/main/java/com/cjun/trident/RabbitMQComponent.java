package com.cjun.trident;

import java.io.IOException;
import java.io.Serializable;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConnectionParameters;
import com.rabbitmq.client.QueueingConsumer;

public class RabbitMQComponent implements Serializable {

  private static final long serialVersionUID = -154748059618033665L;
  private String host;
  private int port;
  private String username;
  private String password;
  private String vhost;
  private QueueingConsumer consumer;
  private Connection connection;

  public RabbitMQComponent(String host, int port, String username, String password, String vhost) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.vhost = vhost;
  }

  public Connection getConnection() {
    if (connection == null || !connection.isOpen()) {
      ConnectionParameters cp = new ConnectionParameters();
      cp.setUsername(username);
      cp.setPassword(password);
      cp.setVirtualHost(vhost);
      ConnectionFactory connectionFactory = new ConnectionFactory();
      try {
        connection = connectionFactory.newConnection(host, port);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return connection;
  }

  public Channel getChannel() throws IOException {
    if (connection == null || !connection.isOpen()) {
      connection = getConnection();
    }
    return connection.createChannel();
  }

  public QueueingConsumer getConsumer(String queueName) {
    if (consumer == null) {
      try {
        Channel channel = this.getChannel();
        // channel.basicQos(5);
        channel.queueDeclare(queueName, true, false, false, false, null);
        consumer = new QueueingConsumer(channel);
        channel.basicConsume(queueName, false, consumer);
      } catch (IOException e) {
        System.out.println("无法建立对" + queueName + "队列的消费者,将1s后重新建立连接");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        getConsumer(queueName);
      }
      return consumer;
    } else {
      return consumer;
    }
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getVhost() {
    return vhost;
  }

  public void setVhost(String vhost) {
    this.vhost = vhost;
  }

}
