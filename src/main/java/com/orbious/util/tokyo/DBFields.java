package com.orbious.util.tokyo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.orbious.util.Bytes;
import com.orbious.util.Loggers;

public class DBFields {

  private HashMap<String, String> entries;
  private HashMap<KeyBytes, String> keys;

  private static final Logger logger = Loggers.logger();

  public DBFields() {
    entries = new HashMap<String, String>();
    keys = new HashMap<KeyBytes, String>();
  }

  public HashMap<KeyBytes, byte[]> entries() {
    HashMap<KeyBytes, byte[]> hm = new HashMap<KeyBytes, byte[]>();

    Iterator<KeyBytes> it = keys.keySet().iterator();

    KeyBytes key;
    String val;
    while ( it.hasNext() ) {
      key = it.next();
      val = entries.get( keys.get(key) );
      if ( val != null )
        hm.put(key, Bytes.strToBytes(val));
    }

    return hm;
  }

  public void set(String key) {
    logger.info("Setting key " + key);
    keys.put(new KeyBytes(key), key);
  }

  public void set(String key, String value) {
    logger.info("Setting key " + key + " with value " + value);
    entries.put(key, value);
    keys.put(new KeyBytes(key), key);
  }

  public void set(byte[] key, byte[] value) {
    if ( (key == null) || (value == null) ) return;

    String keystr = Bytes.bytesToStr(key);
    String valuestr = Bytes.bytesToStr(value);
    logger.info("Setting key " + keystr + " with value " + valuestr);

    entries.put(keystr, valuestr);
    keys.put(new KeyBytes(key), keystr);
  }


  public String get(String key) {
    return entries.get(key);
  }

  public boolean contains(byte[] b) {
    if ( keys.get(new KeyBytes(b)) == null )
      return false;

    return true;
  }

  //
  //
  //
  class KeyBytes {
    private final byte[] buffer;

    public KeyBytes(String str) {
      this.buffer = Bytes.strToBytes(str);
    }

    public KeyBytes(byte[] b) {
      this.buffer = b;
    }

    public byte[] buffer() {
      return buffer;
    }

    @Override
    public boolean equals(Object obj) {
      if ( obj == null )
        return false;
      else if ( !(obj instanceof KeyBytes) )
        return false;

      return Arrays.equals(buffer, ((KeyBytes)obj).buffer());
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(buffer);
    }
  }
}
