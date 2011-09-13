package com.orbious.util.tokyo;

import java.io.File;

import com.orbious.util.Bytes;
import com.orbious.util.config.Config;

public class FDBFile extends FDBStorage implements FileStorage {

  private int index;

  public FDBFile(File filestore, boolean readOnly) {
    super(filestore, readOnly);
  }

  public int index() {
    return index;
  }

  /*
   * Interface methods.
   */
  @Override
  public void open() throws StorageException {
    super.open();
    index = readInt(Config.config_fdb_idx);
    if ( index == -1 ) {
      index = 2;
    }
  }

  /*
   * We dont write the config since the max size of the
   * value is 255 bytes and increasing the size increases
   * the size for each row.
   */
  public void writecfg() throws StorageException {
    write(Config.config_fdb_idx, index);
  }

  @Override
  public void close() throws StorageException {
    StorageException e = null;

    if ( !readOnly ) {
      try {
        writecfg();
      } catch ( StorageException se ) {
        e = se;
      }
    }

    super.close();

    if ( e != null ) {
      throw e;
    }
  }

  public String cfgstr() {
    return Integer.toString(index);
  }

  /*
   * The following methods update the indexes while the inherited methods dont!
   */
  public int put(Object value) throws StorageException {
    int idx;
    idx = index++;

    write(idx, value);
    return idx;
  }

  public Object getObject(int idx) {
    return this.readObject(idx);
  }

  public int put(byte[] value) throws StorageException {
    int idx;
    idx = index++;

    write(idx, value);
    return idx;
  }

  public byte[] get(int idx) {
    return this.read( Bytes.strToBytes(Integer.toString(idx)) );
  }

  public int put(int value) throws StorageException {
    int idx;
    idx = index++;

    write(idx, value);
    return idx;
  }

  public int getInt(int idx) {
    return readInt( Integer.toString(idx) );
  }
  public int put(long value) throws StorageException {
    int idx;
    idx = index++;

    write(idx, value);
    return idx;
  }

  public long getLong(int idx) {
    return readLong(idx);
  }

  public int put(double value) throws StorageException {
    int idx;
    idx = index++;

    write(idx, value);
    return idx;
  }

  public double getDouble(int idx) {
    return readDouble(idx);
  }
}
