package com.orbious.util.tokyo;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Vector;
import com.orbious.util.Bytes;
import tokyocabinet.DBM;
import tokyocabinet.FDB;
import tokyocabinet.HDB;
import tokyocabinet.Util;

public abstract class Storage {

  protected final File filestore;
  protected final int tokyoSize;
  protected final boolean readOnly;
  protected DBM dbm;

  public abstract void write(int key, Object obj) throws StorageException;
  public abstract Object readObject(int key);
  public abstract void write(int key, long val) throws StorageException;
  public abstract long readLong(int key);
  public abstract void write(int key, double val) throws StorageException;
  public abstract double readDouble(int key);
  public abstract void write(long key, Object obj) throws StorageException;
  public abstract Object readObject(long key);
  public abstract void write(double key, Object obj) throws StorageException;
  public abstract Object readObject(double key);

  public Storage(File filestore, int tokyoSize, boolean readOnly) {
    this.filestore = filestore;
    this.tokyoSize = tokyoSize;
    this.readOnly = readOnly;
  }

  public Storage(String filename, int tokyoSize, boolean readOnly) {
    this(new File(filename), tokyoSize, readOnly);
  }

  public boolean exists() {
    if ( filestore == null ) {
      return false;
    }
    return filestore.exists();
  }

  public boolean readOnly() {
    return readOnly;
  }

  public void iterinit() throws StorageException {
    if ( dbm == null ) {
      throw new StorageException("Tokyo storage has not been initialized?");
    }

    dbm.iterinit();
  }

  public byte[] iternext() {
    return dbm.iternext();
  }

  public void vanish() {
    if ( dbm == null ) {
      return;
    }

    if ( dbm instanceof HDB ) {
      ((HDB)dbm).vanish();
    } else if ( dbm instanceof FDB ) {
      ((FDB)dbm).vanish();
    } else {
      throw new UnsupportedOperationException(
          "Unsupported storage type for tokyo storage");
    }
  }

  protected void open(Class<?> clazz) throws StorageException {
    if ( clazz == HDB.class ) {
      dbm = openhdb(filestore, tokyoSize, readOnly);
    } else if ( clazz == FDB.class ) {
      dbm = openfdb(filestore, readOnly);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported storage type for tokyo storage");
    }
  }

  public static HDB openhdb(File file, int size, boolean read)
      throws StorageException {
    HDB hdb = new HDB();
    if ( size != -1 ) {
      hdb.setcache(size);
    }

    int mode;
    if ( read ) {
      mode = HDB.OREADER | HDB.ONOLCK;
    } else {
      mode = HDB.OWRITER | HDB.OCREAT;
    }

    if ( !hdb.open(file.toString(), mode) ) {
      throw new StorageException("Failed to initialize hdb storage " +
          file.toString(), hdb.ecode(), HDB.errmsg(hdb.ecode()));
    }

    return hdb;
  }

  public static FDB openfdb(File file, boolean read) throws StorageException {
    FDB fdb;
    int mode;

    fdb = new FDB();

    if ( read ) {
      mode = FDB.OREADER | FDB.ONOLCK;
    } else {
      mode = FDB.OWRITER | FDB.OCREAT;
    }

    if ( !fdb.open(file.toString(), mode) ) {
      throw new StorageException("Failed to initialize fdb storage " +
          file.toString(), fdb.ecode(), FDB.errmsg(fdb.ecode()));
    }

    return fdb;
  }

  public void close() throws StorageException {
    if ( dbm == null ) {
      return;
    }

    boolean error = false;

    if ( dbm instanceof HDB ) {
      if ( !((HDB)dbm).close() ) {
        error = true;
      }
    } else if ( dbm instanceof FDB ) {
      if ( !((FDB)dbm).close() ) {
        error = true;
      }
    } else {
      throw new UnsupportedOperationException(
          "Unsupported storage type for tokyo storage");
    }

    dbm = null;

    if ( error ) {
      throw new StorageException("Failed to close tokyo storage " +
          filestore.toString(), ecode(), errmsg());
    }
  }

  // for one off stuff, e.g. to read specific keys from files ..
  protected static String read(Class<?> clazz, File file, String key)
      throws StorageException {
    DBM dbm;

    if ( clazz == HDB.class ) {
      dbm = openhdb(file, -1, true);
    } else if ( clazz == FDB.class ) {
      dbm = openfdb(file, true);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported storage type for tokyo storage");
    }

    byte[] bkey;
    byte[] bval;

    bkey = Bytes.strToBytes(key);
    bval = dbm.get(bkey);

    if ( bval == null ) {
      return null;
    }

    return Bytes.bytesToStr(bval);
  }

  protected int ecode() {
    if ( dbm instanceof HDB ) {
      return ((HDB)dbm).ecode();
    } else if ( dbm instanceof FDB ) {
      return ((FDB)dbm).ecode();
    }

    return -1;
  }

  protected String errmsg() {
    if ( dbm instanceof HDB ) {
      return ((HDB)dbm).errmsg();
    } else if ( dbm instanceof FDB ) {
      return ((FDB)dbm).errmsg();
    }

    return "";
  }


  public byte[] read(byte[] key) {
    return dbm.get(key);
  }

  public void write(byte[] key, byte[] val) throws StorageException {
    if ( !dbm.put(key, val) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), ecode(), errmsg());
    }
  }

  public Vector<byte[]> keys() throws StorageException {
    Vector<byte[]> keys;
    byte[] key;
    long sz;

    sz = (int)dbm.fsiz();
    if ( sz > Integer.MAX_VALUE ) {
      sz = Integer.MAX_VALUE;
    }

    keys = new Vector<byte[]>((int)sz);
    dbm.iterinit();

    while ( (key = dbm.iternext()) != null ) {
      keys.add(key);
    }

    return keys;
  }

  /*
   * The following methods are generic methods, therefore slower ..
   */
  public Object read(Class<?> keyclazz, Object key, Class<?> valueclazz)
      throws UnsupportedEncodingException {
    byte[] bkey = Bytes.convert(keyclazz, key);
    return read(bkey, valueclazz);
  }

  public Object read(byte[] key, Class<?> valueclazz)
      throws UnsupportedEncodingException {
    byte[] value;

    value = dbm.get(key);
    if ( value == null ) {
      return value;
    }

    return Bytes.convert(valueclazz, value);
  }

  public void write(Class<?> keyclazz, Object key, Class<?> valueclazz, Object value) {
    dbm.put(Bytes.convert(keyclazz, key), Bytes.convert(valueclazz, value));
  }

  /*
   * The following methods operate on primitive/object types
   * therefore are faster
   */

  // string keys
  public Object readObject(String key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.strToBytes(key);
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return null;
    }

    return Util.deserialize(bval);
  }

  public void write(String key, Object obj) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.strToBytes(key);
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

  // object keys
  public void write(Object key, int value) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    if ( key instanceof String ) {
      bkey = Bytes.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = Util.packint(value);

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), ecode(), errmsg());
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

    bval = dbm.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return Bytes.bytesToInt(bval);
  }

  public void write(Object key, long value) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    if ( key instanceof String ) {
      bkey = Bytes.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = Bytes.longToBytes(value);

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), ecode(), errmsg());
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

    bval = dbm.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return Bytes.bytesToLong(bval);
  }

  public void write(Object key, double value) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    if ( key instanceof String ) {
      bkey = Bytes.strToBytes((String)key);
    } else {
      bkey = Util.serialize(key);
    }

    bval = Bytes.doubleToBytes(value);

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), ecode(), errmsg());
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

    bval = dbm.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return Bytes.bytesToDouble(bval);
  }
}