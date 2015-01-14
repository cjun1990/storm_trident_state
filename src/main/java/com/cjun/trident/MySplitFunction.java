package com.cjun.trident;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class MySplitFunction extends BaseFunction {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: 自定义split
   */
  private static final long serialVersionUID = 1L;

  private String _splitFlag;

  public MySplitFunction() {}

  public MySplitFunction(String splitFlag) {
    this._splitFlag = splitFlag;
  }

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    String tupleStr = tuple.getString(0);
    if (tupleStr != null) {
      if (_splitFlag != null && _splitFlag.equals("#")) {
        String[] strArr = tupleStr.split(_splitFlag);
        // System.err.println(strArr[3]);
        collector.emit(new Values(strArr[3]));
      }
    }
  }

  public String get_splitFlag() {
    return _splitFlag;
  }

  public void set_splitFlag(String _splitFlag) {
    this._splitFlag = _splitFlag;
  }
}
