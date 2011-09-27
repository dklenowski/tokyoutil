package com.orbious.util.tokyo;

public interface IStorage {
  public boolean exists();
  public boolean isopen();
  public boolean readOnly();
  public String path();
  public void open() throws StorageException;
  public long size();
  public void clear();
  public void close() throws StorageException;
}
