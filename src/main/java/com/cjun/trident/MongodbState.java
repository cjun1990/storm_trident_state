package com.cjun.trident;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class MongodbState<T> implements IBackingMap<T> {

  private DB db;

  private Serializer _serializer;

  public MongodbState() {}

  public MongodbState(MongodbComponent mongodbComponent) {
    this._serializer = mongodbComponent.getSerializer();
    db = mongodbComponent.getMongoDB();
  }

  public static StateFactory opaque(MongodbComponent<OpaqueValue> mongodbComponent) {
    return new MongodbFactory(mongodbComponent, StateType.OPAQUE);
  }

  public static StateFactory transactional(MongodbComponent<TransactionalValue> mongodbComponent) {
    return new MongodbFactory(mongodbComponent, StateType.TRANSACTIONAL);
  }

  public static StateFactory noTransactional(MongodbComponent<Object> mongodbComponent) {
    return new MongodbFactory(mongodbComponent, StateType.NON_TRANSACTIONAL);
  }

  /**
   * @Title: multiGet
   * @Description: TODO
   * @param keys 此处的key是做group的时候的key，如果按多个字段group，List<Object>就会有0,1,2……
   * @return
   */
  @Override
  public List<T> multiGet(List<List<Object>> keys) {
    String date;
    T result = null;
    DBObject dbObject = new BasicDBObject();
    List<T> retList = new ArrayList<T>(keys.size());
    for (int i = 0; i < keys.size(); i++) {
      date = (String) keys.get(i).get(0);
      DBCursor dbCursor = db.getCollection("sum").find(new BasicDBObject("date", date));
      if (dbCursor.hasNext()) {
        dbObject = dbCursor.next();
        result = (T) _serializer.deserialize((byte[]) dbObject.get("count"));
      }
      retList.add(result);
    }
    return retList;
  }

  @Override
  public void multiPut(List<List<Object>> keys, List<T> vals) {
    DBCollection col = db.getCollection("sum");
    for (int i = 0; i < keys.size(); i++) {
      DBCursor dbCursor = col.find(new BasicDBObject("date", keys.get(i).get(0)));
      if (dbCursor.hasNext()) {// 如果数据库中已经存在
        DBObject dbObject = dbCursor.next();
        dbObject.put("count", _serializer.serialize(vals.get(i)));
        dbObject.put("realCount", ((OpaqueValue) vals.get(i)).getCurr());// 正真的value值
        col.save(dbObject, new WriteConcern(1));
      } else {
        DBObject dbObject = new BasicDBObject();
        dbObject.put("date", keys.get(i).get(0));
        dbObject.put("count", _serializer.serialize(vals.get(i)));
        dbObject.put("realCount", ((OpaqueValue) vals.get(i)).getCurr());
        col.save(dbObject, new WriteConcern(1));
      }
    }
  }

  /**
   * @Title: toSingleStringValue
   * @Description: TODO
   * @param objList
   * @return
   * @return: 将List<Object>取第一个值，并且转化成字符串型
   */
  public String toSingleStringValue(List<Object> objList) {
    if (objList != null && objList.size() == 1) {
      return (String) objList.get(0);
    } else {
      throw new RuntimeException("Memcached state does not support compound keys");
    }
  }

  /**
   * @Title: ObjectToByte
   * @Description: TODO
   * @param obj
   * @return
   * @return: 将对象转换成byte数组
   */
  public byte[] ObjectToByte(java.lang.Object obj) {
    byte[] bytes = null;
    try {
      ByteArrayOutputStream bo = new ByteArrayOutputStream();
      ObjectOutputStream oo = new ObjectOutputStream(bo);
      oo.writeObject(obj);
      bytes = bo.toByteArray();
      bo.close();
      oo.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return bytes;
  }

}
