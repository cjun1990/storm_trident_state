package com.cjun.trident;

import java.io.Serializable;

/**
 * @author xuer
 * @date 2014-9-19 - 上午10:30:10
 * @Description 记录事务的元数据
 */
public class MyMetadata implements Serializable {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  private long index;

  private int offset;

  public long getIndex() {
    return index;
  }

  public void setIndex(long index) {
    this.index = index;
  }

  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }
}
