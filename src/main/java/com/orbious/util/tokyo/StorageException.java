package com.orbious.util.tokyo;

public class StorageException extends Exception {

  private static final long serialVersionUID = 1387622819682487355L;

  public StorageException(String msg) {
    super(msg);
  }

  public StorageException(String msg, String errstr) {
    super(msg + ": " + errstr);
  }

  public StorageException(String msg, int errcode, String errstr) {
    super(msg + ": " + errstr + " (" + errcode + ")");
  }

  public StorageException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public StorageException(String msg, String errstr, Throwable cause) {
    super(msg + ": " + errstr, cause);
  }

  public StorageException(String msg, int errcode, String errstr, Throwable cause) {
    super(msg + ": " + errstr + " (" + errcode + ")" , cause);
  }
}
