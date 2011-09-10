package com.orbious.util.tokyo;

import java.io.File;
import java.util.Arrays;
import com.orbious.util.Bytes;
import tokyocabinet.FDB;
import tokyocabinet.Util;

public class FDBStorage extends Storage {

  public FDBStorage(File filestore, boolean readOnly) {
    super(filestore, -1, readOnly);
  }

  public void open() throws StorageException {
    super.open(FDB.class);
  }

  public static String read(File file, String key) throws StorageException {
    return Storage.read(FDB.class, file, key);
  }

  /*
   * Abstract methods.
   */

  public void write(int key, Object obj) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    // for fdb we need to convert the int to a string ..
    bkey = Bytes.strToBytes( Integer.toString(key) );
    bval = Util.serialize(obj);

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), ecode(), errmsg());
    }
  }

  public Object readObject(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.strToBytes(Integer.toString(key));
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Util.deserialize(bval);
  }

  public void write(int key, long val) throws StorageException {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.strToBytes(Integer.toString(key));
    bval = Bytes.longToBytes(val);

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), ecode(), errmsg());
    }
  }

  public long readLong(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.strToBytes(Integer.toString(key));
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return -1L;
    }

    return Bytes.bytesToLong(bval);
  }

  public void write(int key, double val) throws StorageException {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.strToBytes(Integer.toString(key));
    bval = Bytes.doubleToBytes(val);

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), ecode(), errmsg());
    }
  }

  public double readDouble(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.strToBytes(Integer.toString(key));
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return -1L;
    }

    return Bytes.bytesToDouble(bval);
  }

  // long keys
  public void write(long key, Object obj) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.strToBytes(Long.toString(key));
    bval = Util.serialize(obj);

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), ecode(), errmsg());
    }
  }

  public Object readObject(long key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.strToBytes(Long.toString(key));
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Util.deserialize(bval);
  }

  // double keys
  public void write(double key, Object obj) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.strToBytes(Double.toString(key));
    bval = Util.serialize(obj);

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), ecode(), errmsg());
    }
  }

  public Object readObject(double key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.strToBytes(Double.toString(key));
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Util.deserialize(bval);
  }


}
