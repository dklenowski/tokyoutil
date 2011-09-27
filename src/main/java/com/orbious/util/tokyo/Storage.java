package com.orbious.util.tokyo;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.orbious.util.Bytes;
import com.orbious.util.Loggers;
import tokyocabinet.DBM;
import tokyocabinet.FDB;
import tokyocabinet.HDB;
import tokyocabinet.Util;

public abstract class Storage implements IStorage {

  protected final File filestore;
  protected final Class<?> filetype;
  protected final int tokyoSize;
  protected final boolean readOnly;
  protected DBM dbm;
  protected Logger logger;

  public abstract void write(int key, Object obj) throws StorageException;
  public abstract Object readObject(int key);
  public abstract void write(int key, int val) throws StorageException;
  public abstract int readInt(int key);
  public abstract void write(int key, long val) throws StorageException;
  public abstract long readLong(int key);
  public abstract void write(int key, double val) throws StorageException;
  public abstract double readDouble(int key);
  public abstract void write(long key, Object obj) throws StorageException;
  public abstract Object readObject(long key);
  public abstract void write(double key, Object obj) throws StorageException;
  public abstract Object readObject(double key);

  public Storage(File filestore, Class<?> filetype, int tokyoSize, boolean readOnly) {
    this.filestore = filestore;
    this.filetype = filetype;
    this.tokyoSize = tokyoSize;
    this.readOnly = readOnly;
    logger = Loggers.logger();
  }

  public Storage(String filename, Class<?> filetype, int tokyoSize, boolean readOnly) {
    this(new File(filename), filetype, tokyoSize, readOnly);
  }

  public boolean exists() {
    if ( filestore == null ) {
      return false;
    }
    return filestore.exists();
  }

  public boolean isopen() {
    if ( dbm != null ) {
      return true;
    }

    return false;
  }

  public boolean readOnly() {
    return readOnly;
  }

  public String path() {
    return filestore.toString();
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

  public void open() throws StorageException {
    try {
      dbm = Helper.open(filestore, filetype, tokyoSize, readOnly);
    } catch ( HelperException he ) {
      throw new StorageException("Error opening " + filestore.toString(), he);
    }
  }

  public long size() {
    return dbm.fsiz();
  }

  public void clear() {
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

  public void close() throws StorageException {
    if ( dbm == null ) {
      return;
    }

    try {
      Helper.close(dbm);
      dbm = null;
    } catch ( HelperException he ) {
      throw new StorageException("Error closing " + filestore.toString(), he);
    }
  }

  // for one off stuff, e.g. to read specific keys from files ..
  protected static String read(File file, Class<?> filetype, String key)
      throws StorageException {
    DBM dbm;

    try {
      dbm = Helper.open(file, filetype, -1, true);
    } catch ( HelperException he ) {
      throw new StorageException("Error opening " + file.toString(), he);
    }

    byte[] bkey;
    byte[] bval;

    bkey = Bytes.strToBytes(key);
    bval = dbm.get(bkey);

    if ( bval == null ) {
      return null;
    }

    try {
      Helper.close(dbm);
    } catch ( HelperException he ) {
      throw new StorageException("Error closing " + file.toString(), he);
    }

    return Bytes.bytesToStr(bval);
  }


  public byte[] read(byte[] key) {
    return dbm.get(key);
  }

  public void write(byte[] key, byte[] val) throws StorageException {
    if ( !dbm.put(key, val) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
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
  public Object read(Object key, Class<?> keyclazz, Class<?> valueclazz)
      throws UnsupportedEncodingException {
    byte[] bkey = Bytes.convert(key, keyclazz);
    if ( bkey == null ) {
      return null;
    }

    return read(bkey, valueclazz);
  }

  public Object read(byte[] key, Class<?> valueclazz)
      throws UnsupportedEncodingException {
    byte[] value;

    value = dbm.get(key);
    if ( value == null ) {
      return value;
    }

    return Bytes.convert(value, valueclazz);
  }

  public void write(Object key, Class<?> keyclazz, Object value, Class<?> valueclazz) {
    dbm.put(Bytes.convert(key, keyclazz), Bytes.convert(value, valueclazz));
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

    return Bytes.deserialize(bval);
  }

  public void write(String key, Object obj) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.strToBytes(key);
    if ( obj instanceof byte[] ) {
      bval = (byte[])obj;
    } else {
      bval = Util.serialize(obj);
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

  // object keys
  public void write(Object key, short value) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.serialize(key);
    bval = Bytes.shortToBytes(value);

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public short readShort(Object key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.serialize(key);
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return Bytes.bytesToShort(bval);
  }

  public void write(Object key, int value) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.serialize(key);
    bval = Bytes.intToBytes(value);

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public int readInt(Object key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.serialize(key);
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

    bkey = Bytes.serialize(key);
    bval = Bytes.longToBytes(value);

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public long readLong(Object key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.serialize(key);
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

    bkey = Bytes.serialize(key);
    bval = Bytes.doubleToBytes(value);

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public double readDouble(Object key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.serialize(key);
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return Bytes.bytesToDouble(bval);
  }

  public void write(Object key, Object value) throws StorageException {
    byte[] bkey;
    byte[] bval;
    byte[] orig;

    bkey = Bytes.serialize(key);
    bval = Bytes.serialize(value);

    orig = dbm.get(bkey);
    if ( (orig != null) && Arrays.equals(bval, orig) ) {
      return;
    }

    if ( !dbm.put(bkey, bval) ) {
      throw new StorageException("Failed to write key " + key + " to " +
          filestore.toString(), Helper.ecode(dbm), Helper.errmsg(dbm));
    }
  }

  public Object readObject(Object key) {
    byte[] bkey;
    byte[] bval;

    bkey = Bytes.serialize(key);
    bval = dbm.get(bkey);
    if ( bval == null ) {
      return -1;
    }

    return Bytes.deserialize(bval);
  }
}
