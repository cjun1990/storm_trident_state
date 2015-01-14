package com.cjun.nontransactional;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class EmitDate extends BaseFunction {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    Log log = (Log) tuple.get(0);
    List<Object> emitList = new ArrayList<Object>();
    emitList.add(log.get_date());
    collector.emit(emitList);
  }

}
