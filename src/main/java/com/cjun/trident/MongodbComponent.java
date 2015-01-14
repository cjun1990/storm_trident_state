package com.cjun.trident;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.Serializer;
import storm.trident.state.StateType;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * @author xuer
 * @date 2014-9-23 - 上午10:18:40
 * @Description 传递到自定义state中的配置文件
 */
public class MongodbComponent<T> implements Serializable {

  private static final long serialVersionUID = 7460259406145029096L;

  // 传递到自定义state的配置文件中需要有这两个属性，具体什么用处还不是很清楚
  private final int localCacheSize = 1000;
  /*
   * 此处用到了泛型，对应关系如下： noTransactional:object Transactional:TransactionalValue opaque:OpaqueValue
   * 这个对应关系是通过MemcachedState观察得到的
   */
  private Serializer<T> serializer;

  private DB mongoDB;
  private final String mongoHost;
  private final int mongoPort;
  private final String mongoDbName;
  private final String username;
  private final String password;

  public static final Map<StateType, Serializer> DEFAULT_SERIALIZER =
      new HashMap<StateType, Serializer>() {
        /**
         * @fieldName: serialVersionUID
         * @fieldType: long
         * @Description: TODO
         */
        private static final long serialVersionUID = 1L;

        {
          put(StateType.OPAQUE, new JSONOpaqueSerializer());
          put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
          put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
        }
      };

  public MongodbComponent(String mongoHost, int mongoPort, String mongoDbName, String username,
      String password) {
    this.mongoHost = mongoHost;
    this.mongoPort = mongoPort;
    this.mongoDbName = mongoDbName;
    this.username = username;
    this.password = password;
  }

  public DB getMongoDB() {
    if (mongoDB == null) {
      try {
        mongoDB = new Mongo(mongoHost, mongoPort).getDB(mongoDbName);
        boolean isAuth = mongoDB.authenticate(username, password.toCharArray());
        if (!isAuth) {
          throw new RuntimeException("没有访问mongodb的权限：[mongoHost:" + mongoHost + "]" + "[mongoPort:"
              + mongoPort + "]" + "[mongoDbName:" + mongoDbName + "]" + "[username:" + username
              + "]" + "[password:" + password + "]");
        }

      } catch (UnknownHostException e) {
        throw new RuntimeException("连接MongoDB数据库错误", e);
      }
    }
    return mongoDB;
  }

  public void loadCache() {
    if (getMongoDB() != null) {
      mongoDB.getCollection(mongoDbName);
    }
  }

  public Serializer<T> getSerializer() {
    return serializer;
  }

  public void setSerializer(Serializer<T> serializer) {
    this.serializer = serializer;
  }

  public int getLocalCacheSize() {
    return localCacheSize;
  }
}
