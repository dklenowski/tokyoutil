package com.orbious.util.tokyo;

public class HDBWrapperException extends Exception {

  private static final long serialVersionUID = 1387622819682487355L;

  public HDBWrapperException(String msg) {
    super(msg);
  }

  public HDBWrapperException(String msg, String errstr) {
    super(msg + ": " + errstr);
  }

  public HDBWrapperException(String msg, int errcode, String errstr) {
    super(msg + ": " + errstr + " (" + errcode + ")");
  }

  public HDBWrapperException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public HDBWrapperException(String msg, String errstr, Throwable cause) {
    super(msg + ": " + errstr, cause);
  }

  public HDBWrapperException(String msg, int errcode, String errstr, Throwable cause) {
    super(msg + ": " + errstr + " (" + errcode + ")" , cause);
  }
}
