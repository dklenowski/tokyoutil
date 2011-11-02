package com.orbious.util.tokyo;

import java.io.File;
import java.util.Arrays;
import com.orbious.util.Bytes;
import com.orbious.util.config.Config;

import tokyocabinet.HDB;

public class HDBStorage extends Storage {

  public HDBStorage(File filestore, int tokyoSize, boolean readOnly) {
    super(filestore, HDB.class, tokyoSize, readOnly);
  }

  public static String read(File file, String key) throws StorageException {
    return Storage.read(file, HDB.class, key);
  }

  // these should be overridden.
  public void setDefaultFields() {
    fields.set(Config.config_hdb_key);
  }

  public void updateFields() { }

  // int keys
  public void write(int key, Object obj) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.intToBytes(key);
    if ( obj instanceof byte[] ) {
      bval = (byte[])obj;
    } else {
      bval = Bytes.serialize(obj);
    }

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public Object readObject(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Bytes.deserialize(bval);
  }

  public void write(int key, int val) throws StorageException {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
    bval = Bytes.intToBytes(val);

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public int readInt(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return Bytes.bytesToInt(bval);
  }

  public void write(int key, long val) throws StorageException {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
    bval = Bytes.longToBytes(val);

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public long readLong(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return -1L;
    }

    return Bytes.bytesToLong(bval);
  }

  public void write(int key, double val) throws StorageException {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
    bval = Bytes.doubleToBytes(val);

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public double readDouble(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
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

    bkey = Bytes.longToBytes(key);
    if ( obj instanceof byte[] ) {
      bval = (byte[])obj;
    } else {
      bval = Bytes.serialize(obj);
    }

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public Object readObject(long key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.longToBytes(key);
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Bytes.deserialize(bval);
  }

  // double keys
  public void write(double key, Object obj) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.doubleToBytes(key);
    if ( obj instanceof byte[] ) {
      bval = (byte[])obj;
    } else {
      bval = Bytes.serialize(obj);
    }

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public Object readObject(double key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.doubleToBytes(key);
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Bytes.deserialize(bval);
  }
}
