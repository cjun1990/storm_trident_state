package com.cjun.nontransactional;

import java.io.Serializable;

public class Log implements Serializable {

  /**
   * @fieldName: serialVersionUID
   * @fieldType: long
   * @Description: TODO
   */
  private static final long serialVersionUID = 1L;

  private String _date;

  private String _content;

  public Log(String date, String content) {
    this._date = date;
    this._content = content;
  }

  public String get_date() {
    return _date;
  }

  public void set_date(String _date) {
    this._date = _date;
  }

  public String get_content() {
    return _content;
  }

  public void set_content(String _content) {
    this._content = _content;
  }

}
