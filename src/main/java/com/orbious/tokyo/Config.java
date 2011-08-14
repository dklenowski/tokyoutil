package com.orbious.tokyo;

public enum Config {

  VERSION("1.0"),

  LOGGER_REALM("tokyoutil");

  private String value;

  private Config(String value) {
    this.value = value;
  }

  public String asStr() {
    return value;
  }
}
