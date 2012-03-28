package com.orbious.util.tokyo;

import java.io.File;
import java.util.Arrays;
import com.orbious.util.Bytes;
import com.orbious.util.config.Config;

import tokyocabinet.FDB;
import tokyocabinet.Util;

public class FDBStorage extends Storage {

  public FDBStorage(File filestore, boolean readOnly) {
    super(filestore, FDB.class, -1, readOnly);
  }

  // these should be overridden.
  public void setDefaultFields() {
    fields.set(Config.config_fdb_idx);
  }

  public void updateFields() { }

  public void open() throws StorageException {
    super.open();
  }

  public static String read(File file, String key) throws StorageException {
    return Storage.read(file, FDB.class, key);
  }

  public void write(int key, Object obj) throws StorageException {
    byte[] bkey = Bytes.strToBytes(Integer.toString(key));

    byte[] bval;
    if ( obj instanceof byte[] )
      bval = (byte[])obj;
    else
      bval = Util.serialize(obj);

    byte[] orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) )
      return;

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public Object readObject(int key) {
    byte[] bkey = Bytes.strToBytes(Integer.toString(key));
    byte[] bval = dbm.get(bkey);

    if ( bval == null ) return null;
    return Util.deserialize(bval);
  }

  public void write(int key, int val) throws StorageException {
    byte[] bkey = Bytes.strToBytes(Integer.toString(key));
    byte[] bval = Bytes.intToBytes(val);

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public int readInt(int key) {
    byte[] bkey = Bytes.strToBytes(Integer.toString(key));
    byte[] bval = dbm.get(bkey);

    if ( bval == null ) return -1;
    return Bytes.bytesToInt(bval);
  }

  public void write(int key, long val) throws StorageException {
    byte[] bkey = Bytes.strToBytes(Integer.toString(key));
    byte[] bval = Bytes.longToBytes(val);

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public long readLong(int key) {
    byte[] bkey = Bytes.strToBytes(Integer.toString(key));
    byte[] bval = dbm.get(bkey);

    if ( bval == null ) return -1L;
    return Bytes.bytesToLong(bval);
  }

  public void write(int key, double val) throws StorageException {
    byte[] bkey = Bytes.strToBytes(Integer.toString(key));
    byte[] bval = Bytes.doubleToBytes(val);

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public double readDouble(int key) {
    byte[] bkey = Bytes.strToBytes(Integer.toString(key));
    byte[] bval = dbm.get(bkey);

    if ( bval == null ) return -1L;
    return Bytes.bytesToDouble(bval);
  }

  // long keys
  public void write(long key, Object obj) throws StorageException {
    byte[] bkey = Bytes.strToBytes(Long.toString(key));

    byte[] bval;
    if ( obj instanceof byte[] )
      bval = (byte[])obj;
    else
      bval = Util.serialize(obj);

    byte[] orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) )
      return;

    if  (!dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public Object readObject(long key) {
    byte[] bkey = Bytes.strToBytes(Long.toString(key));
    byte[] bval = dbm.get(bkey);

    if ( bval == null ) return null;
    return Util.deserialize(bval);
  }

  // double keys
  public void write(double key, Object obj) throws StorageException {
    byte[] bkey = Bytes.strToBytes(Double.toString(key));

    byte[] bval;
    if ( obj instanceof byte[] )
      bval = (byte[])obj;
    else
      bval = Util.serialize(obj);

    byte[] orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) )
      return;

    if  (!dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public Object readObject(double key) {
    byte[] bkey = Bytes.strToBytes(Double.toString(key));
    byte[] bval = dbm.get(bkey);

    if ( bval == null ) return null;
    return Util.deserialize(bval);
  }
}
