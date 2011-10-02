package com.orbious.util.tokyo;

import com.orbious.util.config.IConfig;

public interface IFileStorage {
  public void open() throws StorageException;
  public String cfg(IConfig key);
  public void writecfg() throws StorageException;
  public void close() throws StorageException;
  public String cfgstr();
}
