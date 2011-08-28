package com.orbious.util.tokyo;

public class WrapperException extends Exception {

  private static final long serialVersionUID = 1387622819682487355L;

  public WrapperException(String msg) {
    super(msg);
  }

  public WrapperException(String msg, String errstr) {
    super(msg + ": " + errstr);
  }

  public WrapperException(String msg, int errcode, String errstr) {
    super(msg + ": " + errstr + " (" + errcode + ")");
  }

  public WrapperException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public WrapperException(String msg, String errstr, Throwable cause) {
    super(msg + ": " + errstr, cause);
  }

  public WrapperException(String msg, int errcode, String errstr, Throwable cause) {
    super(msg + ": " + errstr + " (" + errcode + ")" , cause);
  }
}
