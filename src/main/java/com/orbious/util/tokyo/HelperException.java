package com.orbious.util.tokyo;

public class HelperException extends Exception {

  private static final long serialVersionUID = 1387622819682487355L;

  public HelperException(String msg) {
    super(msg);
  }

  public HelperException(String msg, String errstr) {
    super(msg + ": " + errstr);
  }

  public HelperException(String msg, int errcode, String errstr) {
    super(msg + ": " + errstr + " (" + errcode + ")");
  }

  public HelperException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public HelperException(String msg, String errstr, Throwable cause) {
    super(msg + ": " + errstr, cause);
  }

  public HelperException(String msg, int errcode, String errstr, Throwable cause) {
    super(msg + ": " + errstr + " (" + errcode + ")" , cause);
  }
}
