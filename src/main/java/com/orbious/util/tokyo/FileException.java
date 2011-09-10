package com.orbious.util.tokyo;

public class FileException extends Exception {

  private static final long serialVersionUID = 1L;

  public FileException(String msg) {
    super(msg);
  }

  public FileException(String msg, Throwable cause) {
    super(msg, cause);
  }
}


