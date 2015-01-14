package com.cjun.nontransactional;

import java.util.Map;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import backtype.storm.task.IMetricsContext;

public class MyStateFactory implements StateFactory {

  private MongodbComponent _mongodbComponent;

  public MyStateFactory(MongodbComponent MongodbComponent) {
    this._mongodbComponent = MongodbComponent;
  }

  @Override
  public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
    return new MyTridentState(new MyBackingMap(_mongodbComponent));
  }

  public MongodbComponent get_mongodbComponent() {
    return _mongodbComponent;
  }

  public void set_mongodbComponent(MongodbComponent _mongodbComponent) {
    this._mongodbComponent = _mongodbComponent;
  }

}
