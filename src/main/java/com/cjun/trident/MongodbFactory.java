package com.cjun.trident;

import java.util.Map;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;

public class MongodbFactory<T> implements StateFactory {

  private MongodbComponent<T> _mongodbComponent;

  private StateType _stateType;

  public MongodbFactory(MongodbComponent<T> mongodbComponent, StateType stateType) {
    this._mongodbComponent = mongodbComponent;
    this._stateType = stateType;
    if (mongodbComponent.getSerializer() == null) {
      _mongodbComponent.setSerializer(MongodbComponent.DEFAULT_SERIALIZER.get(stateType));
    }
  }

  @Override
  public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
    MongodbState mongodbState = new MongodbState(_mongodbComponent);
    CachedMap cachedMap = new CachedMap(mongodbState, _mongodbComponent.getLocalCacheSize());
    MapState ms = null;
    if (_stateType == StateType.OPAQUE) {
      ms = OpaqueMap.build(cachedMap);
    } else if (_stateType == StateType.TRANSACTIONAL) {
      ms = TransactionalMap.build(cachedMap);
    } else if (_stateType == StateType.NON_TRANSACTIONAL) {
      ms = NonTransactionalMap.build(cachedMap);
    } else {
      throw new RuntimeException("Unknown state type: " + _stateType);
    }
    return new SnapshottableMap(ms, new Values("$GLOBAL$"));
  }

  public MongodbComponent<T> get_mongodbComponent() {
    return _mongodbComponent;
  }

  public void set_mongodbComponent(MongodbComponent<T> _mongodbComponent) {
    this._mongodbComponent = _mongodbComponent;
  }

  public StateType get_stateType() {
    return _stateType;
  }

  public void set_stateType(StateType _stateType) {
    this._stateType = _stateType;
  }

}
