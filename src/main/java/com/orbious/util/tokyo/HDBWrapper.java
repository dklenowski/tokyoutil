package com.orbious.util.tokyo;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Vector;

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
  private final File filestore;
  private final int maxSize;
  private boolean readOnly;

  public HDBWrapper(File filestore, int maxSize) {
    this.filestore = filestore;
    this.maxSize = maxSize;
  }

  private void init(int mode) throws HDBWrapperException {
    hdb = new HDB();
    if ( maxSize != -1 ) {
      hdb.setcache(maxSize);
    }

    if ( !hdb.open(filestore.toString(), mode) ) {
      throw new HDBWrapperException("Failed to initialize tokyo file " +
          filestore.toString(), hdb.ecode(), HDB.errmsg(hdb.ecode()));
    }
  }

  public void initReader() throws HDBWrapperException {
    readOnly = true;
    init(HDB.OREADER | HDB.ONOLCK );
  }

  public void initWriter() throws HDBWrapperException {
    readOnly = false;
    init(HDB.OWRITER | HDB.OCREAT );
  }

  public void iterinit() {
    hdb.iterinit();
  }

  public byte[] iternext() {
    return hdb.iternext();
  }

  public boolean readOnly() {
    return readOnly;
  }

  public void close() throws HDBWrapperException {
    if ( hdb == null ) {
      return;
    }

    if ( !hdb.close() ) {
      throw new HDBWrapperException("Close failed", hdb.ecode(),
          HDB.errmsg(hdb.ecode()));
    }

    hdb = null;
  }

  // for one off stuff, e.g. to read specific keys from files ..
  public static String read(File file, String key) throws HDBWrapperException {
    byte[] bkey;
    byte[] bval;
    HDB hdb;

    hdb = new HDB();
    hdb.setcache(1);

    if ( !hdb.open(file.toString(), HDB.OREADER | HDB.ONOLCK) ) {
      throw new HDBWrapperException("Failed to initialize tokyo file " +
          file.toString(), hdb.ecode(), HDB.errmsg(hdb.ecode()));
    }

    bkey = Bytes.strToBytes(key);
    bval = hdb.get(bkey);

    if ( bval == null ) {
      throw new HDBWrapperException("Failed to find type key " + key + " in " +
          file.toString());
    }

    return Bytes.bytesToStr(bval);
  }

  public Vector<byte[]> keys() throws HDBWrapperException {
    Vector<byte[]> keys;
    byte[] bytes;

    System.out.println("Size=" + hdb.fsiz());

    keys = new Vector<byte[]>((int)hdb.fsiz());

    hdb.iterinit();
    while ( (bytes = hdb.iternext()) != null ) {
      keys.add(bytes);
    }

    return keys;
  }

  public byte[] read(byte[] key) {
    return hdb.get(key);
  }

  public void write(byte[] key, byte[] value ) {
    hdb.put(key, value);
  }

  // generic methods, slower because Objects are involved
  public Object read(Class<?> keyclazz, Object key, Class<?> valueclazz)
      throws UnsupportedEncodingException {
    byte[] kbytes = Bytes.convert(keyclazz, key);
    return read(kbytes, valueclazz);
  }

  public Object read(byte[] key, Class<?> valueclazz)
      throws UnsupportedEncodingException {
    byte[] value;

    value = hdb.get(key);
    if ( value == null ) {
      return value;
    }

    return Bytes.convert(valueclazz, value);
  }

  public void write(Class<?> keyclazz, Object key, Class<?> valueclazz, Object value) {
    byte[] kbytes;
    byte[] vbytes;

    kbytes = Bytes.convert(keyclazz, key);
    vbytes = Bytes.convert(valueclazz, value);

    hdb.put(kbytes, vbytes);
  }


  // optimized methods
  public void write(int key, Object obj) throws HDBWrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.intToBytes(key);
    bval = Util.serialize(obj);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!hdb.put(bkey, bval) ) {
      throw new HDBWrapperException("Failed to write key=" + key, hdb.ecode(),
        HDB.errmsg(hdb.ecode()));
    }
  }

  public Object readObject(String key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.strToBytes(key);
    bval = hdb.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Util.deserialize(bval);
  }

  public void write(String key, Object obj) throws HDBWrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.strToBytes(key);
    bval = Util.serialize(obj);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!hdb.put(bkey, bval) ) {
      throw new HDBWrapperException("Failed to write key=" + key,
        HDB.errmsg(hdb.ecode()));
    }
  }

  public Object readObject(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
    bval = hdb.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Util.deserialize(bval);
  }

  public void write(long key, Object obj) throws HDBWrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.longToBytes(key);
    bval = Util.serialize(obj);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!hdb.put(bkey, bval) ) {
      throw new HDBWrapperException("Failed to write key=" + key,
        HDB.errmsg(hdb.ecode()));
    }
  }

  public Object readObject(long key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.longToBytes(key);
    bval = hdb.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Util.deserialize(bval);
  }

  public void write(double key, Object obj) throws HDBWrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.doubleToBytes(key);
    bval = Util.serialize(obj);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if  (!hdb.put(bkey, bval) ) {
      throw new HDBWrapperException("Failed to write key=" + key,
        HDB.errmsg(hdb.ecode()));
    }
  }

  public Object readObject(double key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.doubleToBytes(key);
    bval = hdb.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Util.deserialize(bval);
  }

  public void write(Object key, int value) throws HDBWrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    if ( key instanceof String ) {
      bkey = Bytes.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = Util.packint(value);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !hdb.put(bkey, bval) ) {
      throw new HDBWrapperException("Failed to write key=" + key, hdb.ecode(),
        HDB.errmsg(hdb.ecode()));
    }
  }

  public int readInt(Object key) {
    byte[] bkey;
    byte[] bval;

    if ( key instanceof String ) {
      bkey = Bytes.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = hdb.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return Bytes.bytesToInt(bval);
  }

  public void write(Object key, long value) throws HDBWrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    if ( key instanceof String ) {
      bkey = Bytes.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = Bytes.longToBytes(value);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !hdb.put(bkey, bval) ) {
      throw new HDBWrapperException("Failed to write key=" + key, hdb.ecode(),
        HDB.errmsg(hdb.ecode()));
    }
  }

  public long readLong(Object key) {
    byte[] bkey;
    byte[] bval;

    if ( key instanceof String ) {
      bkey = Bytes.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = hdb.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return Bytes.bytesToLong(bval);
  }

  public void write(Object key, double value) throws HDBWrapperException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    if ( key instanceof String ) {
      bkey = Bytes.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = Bytes.doubleToBytes(value);

    orig = hdb.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !hdb.put(bkey, bval) ) {
      throw new HDBWrapperException("Failed to write key=" + key, hdb.ecode(),
        HDB.errmsg(hdb.ecode()));
    }
  }

  public double readDouble(Object key) {
    byte[] bkey;
    byte[] bval;

    if ( key instanceof String ) {
      bkey = Bytes.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = hdb.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return Bytes.bytesToDouble(bval);
  }

  public void write(int key, long val) throws HDBWrapperException {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
    bval = Bytes.longToBytes(val);

    if ( !hdb.put(bkey, bval) ) {
      throw new HDBWrapperException("Failed to write key=" + key, hdb.ecode(),
          HDB.errmsg(hdb.ecode()));
    }
  }

  public long readLong(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
    bval = hdb.get(bkey);
    if ( bval == null ) {
      return -1L;
    }

    return Bytes.bytesToLong(bval);
  }

  public void write(int key, double val) throws HDBWrapperException {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
    bval = Bytes.doubleToBytes(val);

    if ( !hdb.put(bkey, bval) ) {
      throw new HDBWrapperException("Failed to write key=" + key, hdb.ecode(),
          HDB.errmsg(hdb.ecode()));
    }
  }

  public double readDouble(int key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.intToBytes(key);
    bval = hdb.get(bkey);
    if ( bval == null ) {
      return -1L;
    }

    return Bytes.bytesToDouble(bval);
  }
}
