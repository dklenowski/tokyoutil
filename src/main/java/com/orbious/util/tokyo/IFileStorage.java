package com.orbious.util.tokyo;

public interface IFileStorage {
  public void open() throws StorageException;
  public void writecfg() throws StorageException;
  public void close() throws StorageException;
  public String cfgstr();
}
