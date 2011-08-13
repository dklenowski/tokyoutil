package com.orbious.tokyo.util;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import tokyocabinet.HDB;
import tokyocabinet.Util;

public class Wrapper {

  private Wrapper() { }

  private static HDB init(File file, int maxSize, int mode)
      throws WrapperException {
    HDB hdb = new HDB();

    if ( maxSize != -1 ) {
      hdb.setcache(maxSize);
    }

    if ( !hdb.open(file.toString(), mode) ) {
      throw new WrapperException("Failed to initialize tokyo file " +
          file.toString(), HDB.errmsg(hdb.ecode()));
    }

    return hdb;
  }

  public static HDB initReader(File file, int maxSize)
      throws WrapperException {
    return init(file, maxSize, HDB.OREADER | HDB.ONOLCK );
  }

  public static HDB initWriter(File file, int maxSize)
      throws WrapperException {
    return init(file, maxSize, HDB.OWRITER | HDB.OCREAT );
  }

  public static void close(HDB hdb) throws WrapperException {
    if ( !hdb.close() ) {
      throw new WrapperException("Close failed", HDB.errmsg(hdb.ecode()));
    }
  }

  public static Class<?> read(File file, String key)
      throws WrapperException {
    byte[] bkey;
    byte[] value;
    Class<?> type;
    String str;
    HDB hdb;

    hdb = new HDB();
    hdb.setcache(1);

    if ( !hdb.open(file.toString(), HDB.OREADER | HDB.ONOLCK) ) {
      throw new WrapperException("Failed to initialize tokyo file " +
          file.toString(), HDB.errmsg(hdb.ecode()));
    }

    bkey = ByteOps.strToBytes(key);
    value = hdb.get(bkey);

    if ( value == null ) {
      throw new WrapperException("Failed to find type key " + key + " in " +
          file.toString());
    }

    str = ByteOps.bytesToStr(value);
    try {
      type = Class.forName(str);
    } catch ( ClassNotFoundException cnfe ) {
      throw new WrapperException("Error instantiating type " + str, cnfe);
    }

    if ( !hdb.close() ) {
      throw new WrapperException("Close failed", HDB.errmsg(hdb.ecode()));
    }

    return type;
  }

  public static void write(HDB hdb, int key, ArrayList<?> list)
      throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = ByteOps.intToBytes(key);
    bval = Util.serialize(list);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key,
        HDB.errmsg(hdb.ecode()));
    }
  }

  public static void write(HDB hdb, long key, ArrayList<?> list)
      throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = ByteOps.longToBytes(key);
    bval = Util.serialize(list);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key,
        HDB.errmsg(hdb.ecode()));
    }
  }

  public static void write(HDB hdb, int key, Object obj)
      throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = ByteOps.intToBytes(key);
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

  public static void write(HDB hdb, long key, Object obj)
      throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = ByteOps.longToBytes(key);
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

  public static void write(HDB hdb, String key, int value)
      throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = ByteOps.strToBytes(key);
    bval = Util.packint(value);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key,
        HDB.errmsg(hdb.ecode()));
    }
  }

  public static void write(HDB hdb, String key, long value)
      throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = ByteOps.strToBytes(key);
    bval = ByteOps.longToBytes(value);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key,
        HDB.errmsg(hdb.ecode()));
    }
  }

  public static void write(HDB hdb, String key, String value)
      throws WrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = key.getBytes();
    bval = value.getBytes();

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key,
        HDB.errmsg(hdb.ecode()));
    }
  }

  public static void write(HDB hdb, int key, double val)
      throws WrapperException {
    byte[] bkey;
    byte[] bval;

    bkey = ByteOps.intToBytes(key);
    bval = ByteOps.doubleToBytes(val);

    if ( !hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key,
          HDB.errmsg(hdb.ecode()));
    }
  }

  public static void write(HDB hdb, long key, double val)
      throws WrapperException {
    byte[] bkey;
    byte[] bval;

    bkey = ByteOps.longToBytes(key);
    bval = ByteOps.doubleToBytes(val);

    if ( !hdb.put(bkey, bval) ) {
      throw new WrapperException("Failed to write key=" + key,
          HDB.errmsg(hdb.ecode()));
    }
  }
}
