package com.orbious.tokyo;

import java.io.File;
import java.util.Arrays;
import tokyocabinet.HDB;
import tokyocabinet.Util;

/*
                key       value
write           int       Object
readObject      int       Object

write           long      Object
readObject      long      Object

write           double    Object
readObject      double    Object

write           Object    int
readInt         Object    int

write           Object    long
readLong        Object    long

write           Object    double
readDouble      Object    double

write           int       long
readLong        int       long

write           int       double
readDouble      int       double
*/

public class HDBWrapper {

  protected HDB hdb;

  public HDBWrapper() { }

  private void init(File file, int maxSize, int mode) throws WrapperException {
    hdb = new HDB();
    if ( maxSize != -1 ) {
      hdb.setcache(maxSize);
    }

    if ( !hdb.open(file.toString(), mode) ) {
      throw new WrapperException("Failed to initialize tokyo file " +
          file.toString(), hdb.ecode(), HDB.errmsg(hdb.ecode()));
    }
  }

  public void initReader(File file, int maxSize) throws WrapperException {
    init(file, maxSize, HDB.OREADER | HDB.ONOLCK );
  }

  public void initWriter(File file, int maxSize) throws WrapperException {
    init(file, maxSize, HDB.OWRITER | HDB.OCREAT );
  }

  // sometimes it will be necessary to retreive hdb ..
  // as long as the calling class does not close it !
  public HDB hdb() {
    return hdb;
  }

  public void close() throws WrapperException {
    if ( hdb == null ) {
      return;
    }

    if ( !hdb.close() ) {
      throw new WrapperException("Close failed", hdb.ecode(),
          HDB.errmsg(hdb.ecode()));
    }

    hdb = null;
  }

  // for one off stuff, e.g. to read specific keys from files ..
  public static String read(File file, String key) throws WrapperException {
    byte[] bkey;
    byte[] bval;
    HDB hdb;

    hdb = new HDB();
    hdb.setcache(1);

    if ( !hdb.open(file.toString(), HDB.OREADER | HDB.ONOLCK) ) {
      throw new WrapperException("Failed to initialize tokyo file " +
          file.toString(), hdb.ecode(), HDB.errmsg(hdb.ecode()));
    }

    bkey = ByteUtils.strToBytes(key);
    bval = hdb.get(bkey);

    if ( bval == null ) {
      throw new WrapperException("Failed to find type key " + key + " in " +
          file.toString());
    }

    return ByteUtils.bytesToStr(bval);
  }

  public void write(int key, Object obj) throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = ByteUtils.intToBytes(key);
    bval = Util.serialize(obj);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key, hdb.ecode(),
        HDB.errmsg(hdb.ecode()));
    }
  }

  public Object readObject(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = ByteUtils.intToBytes(key);
    bval = hdb.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Util.deserialize(bval);
  }

  public void write(long key, Object obj) throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = ByteUtils.longToBytes(key);
    bval = Util.serialize(obj);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key,
        HDB.errmsg(hdb.ecode()));
    }
  }

  public Object readObject(long key) {
    byte[] bkey;
    byte[] bval;

    bkey = ByteUtils.longToBytes(key);
    bval = hdb.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Util.deserialize(bval);
  }

  public void write(double key, Object obj) throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = ByteUtils.doubleToBytes(key);
    bval = Util.serialize(obj);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key,
        HDB.errmsg(hdb.ecode()));
    }
  }

  public Object readObject(double key) {
    byte[] bkey;
    byte[] bval;

    bkey = ByteUtils.doubleToBytes(key);
    bval = hdb.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Util.deserialize(bval);
  }

  public void write(Object key, int value) throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    if ( key instanceof String ) {
      bkey = ByteUtils.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = Util.packint(value);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key, hdb.ecode(),
        HDB.errmsg(hdb.ecode()));
    }
  }

  public int readInt(Object key) {
    byte[] bkey;
    byte[] bval;

    if ( key instanceof String ) {
      bkey = ByteUtils.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = hdb.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return ByteUtils.bytesToInt(bval);
  }

  public void write(Object key, long value) throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    if ( key instanceof String ) {
      bkey = ByteUtils.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = ByteUtils.longToBytes(value);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key, hdb.ecode(),
        HDB.errmsg(hdb.ecode()));
    }
  }

  public long readLong(Object key) {
    byte[] bkey;
    byte[] bval;

    if ( key instanceof String ) {
      bkey = ByteUtils.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = hdb.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return ByteUtils.bytesToLong(bval);
  }

  public void write(Object key, double value) throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    if ( key instanceof String ) {
      bkey = ByteUtils.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = ByteUtils.doubleToBytes(value);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key, hdb.ecode(),
        HDB.errmsg(hdb.ecode()));
    }
  }

  public double readDouble(Object key) {
    byte[] bkey;
    byte[] bval;

    if ( key instanceof String ) {
      bkey = ByteUtils.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = hdb.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return ByteUtils.bytesToDouble(bval);
  }

  public void write(int key, long val) throws WrapperException {
    byte[] bkey;
    byte[] bval;

    bkey = ByteUtils.intToBytes(key);
    bval = ByteUtils.longToBytes(val);

    if ( !hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key, hdb.ecode(),
          HDB.errmsg(hdb.ecode()));
    }
  }

  public long readLong(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = ByteUtils.intToBytes(key);
    bval = hdb.get(bkey);
    if ( bval == null ) {
      return -1L;
    }

    return ByteUtils.bytesToLong(bval);
  }

  public void write(int key, double val) throws WrapperException {
    byte[] bkey;
    byte[] bval;

    bkey = ByteUtils.intToBytes(key);
    bval = ByteUtils.doubleToBytes(val);

    if ( !hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key, hdb.ecode(),
          HDB.errmsg(hdb.ecode()));
    }
  }

  public double readDouble(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = ByteUtils.intToBytes(key);
    bval = hdb.get(bkey);
    if ( bval == null ) {
      return -1L;
    }

    return ByteUtils.bytesToDouble(bval);
  }
}
