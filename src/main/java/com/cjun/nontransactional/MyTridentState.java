package com.cjun.nontransactional;

import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;

public class MyTridentState extends NonTransactionalMap<Long> {
  protected MyTridentState(IBackingMap<Long> backing) {
    super(backing);
  }
}
