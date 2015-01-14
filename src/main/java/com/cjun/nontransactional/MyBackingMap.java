package com.cjun.nontransactional;

import java.util.ArrayList;
import java.util.List;

import storm.trident.state.map.IBackingMap;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class MyBackingMap implements IBackingMap<Long> {

  public DBCollection col;

  public MyBackingMap() {}

  public MyBackingMap(MongodbComponent mongodbComponent) {
    col = mongodbComponent.getMongoDB().getCollection("count");
  }

  @Override
  public List<Long> multiGet(List<List<Object>> keys) {
    List<Long> retList = new ArrayList<Long>(keys.size());
    long count = 0;
    for (int i = 0; i < keys.size(); i++) {
      DBCursor dbCursor = col.find(new BasicDBObject("date", keys.get(i).get(0)));
      while (dbCursor.hasNext()) {
        DBObject dbObject = dbCursor.next();
        count = (long) dbObject.get("count");
      }
      retList.add(count);

      System.err.println("multiGet方法" + keys.get(i).get(0) + ":" + "根据前面传递的条件，查询数据库，返回值:" + count);
    }
    return retList;
  }

  @Override
  public void multiPut(List<List<Object>> keys, List<Long> vals) {
    for (int i = 0; i < keys.size(); i++) {
      DBObject dbObject = new BasicDBObject();
      DBCursor dbCursor = col.find(new BasicDBObject("date", keys.get(i).get(0)));
      while (dbCursor.hasNext()) {
        dbObject = dbCursor.next();
      }
      dbObject.put("date", keys.get(i).get(0));
      dbObject.put("count", vals.get(i));
      col.save(dbObject, new WriteConcern(1));
      System.err.println("multiPut方法,根据前面的条件，把最后的值插入数据库" + keys.get(i).get(0) + ":" + vals.get(i));
    }
  }
}
