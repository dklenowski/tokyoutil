package com.orbious.util.tokyo;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.collections15.map.AbstractLinkedMap;
import org.apache.log4j.Logger;
import com.orbious.util.Bytes;
import com.orbious.util.Loggers;
import com.orbious.util.tokyo.apache.LRURndMap;

public class HDBLRUMap<K, V> extends LRURndMap<K, V> {

  private static final long serialVersionUID = 1L;

  private final File filestore;
  private final Class<K> kclazz;
  private final Class<V> vclazz;
  private final boolean readOnly;

  private final int tokyoSize;

  private HDBStorage hdbs;
  private Logger logger;

  public HDBLRUMap(File filestore, Class<K> keyclazz, Class<V> valueclazz,
      int mapSize, int tokyoSize, boolean readOnly) {
    super(mapSize);

    this.filestore = filestore;
    this.kclazz = keyclazz;
    this.vclazz = valueclazz;
    this.tokyoSize = tokyoSize;
    this.readOnly = readOnly;
    this.logger = Loggers.logger();
  }

  public void load() throws HDBLRUMapException {
    if ( readOnly && !filestore.exists() ) {
      throw new HDBLRUMapException("Cannot open " + filestore.toString() +
          " readOnly, file does not exist?");
    }

    hdbs = new HDBStorage(filestore, tokyoSize, readOnly);
    try {
      hdbs.open();
    } catch ( StorageException se ) {
      throw new HDBLRUMapException("Failed to initialise HDB", se);
    }
  }

  public String filename() {
    return filestore.toString();
  }

  @Override
  public void clear() {
    super.clear();
    if ( readOnly ) return;
    hdbs.clear();
  }

  public void close() throws HDBLRUMapException {
    if ( hdbs == null ) return;

    logger.info("Closing " + filestore.toString());

    if ( !readOnly )
      closewrite();

    try {
      hdbs.close();
    } catch ( StorageException se ) {
      throw new HDBLRUMapException("Error closing " + filestore.toString() +
          ", please check the error log for more details.", se);
    }
  }

  private void closewrite() {
    if ( readOnly ) return;

    Object[] keys = this.keySet().toArray();
    logger.info("Writing " + keys.length + " keys in memory");

    K key;
    for ( int i = 0; i < keys.length; i++ ) {
      key = kclazz.cast(keys[i]);
      write(key, this.get(key));
    }
  }

  // the following 2 methods are used to store configuration info ..
  // write config info
  public void writecfg(String key, String value) throws HDBLRUMapException {
    if ( readOnly ) return;

    try {
      hdbs.write( Bytes.strToBytes(key), Bytes.strToBytes(value) );
    } catch ( StorageException se ) {
      throw new HDBLRUMapException("Failed to write cfg key (" +
          key + ") with value (" + value + ")", se);
    }
  }

  // read config info
  public String readcfg(String key) {
    byte[] bval = hdbs.read(Bytes.strToBytes(key));
    if ( bval == null )
      return null;
    return Bytes.bytesToStr(bval);
  }

  @Override
  public V get(Object key) {
    if ( this.containsKey(key) )
      return super.get(key);
    return read( kclazz.cast(key) );
  }

  private V read(K key) {
    byte[] bkey = Bytes.convert(key, kclazz);
    byte[] bval = hdbs.read(bkey);

    if ( bval == null )
      return null;

    V val = null;
    try {
      val = vclazz.cast(Bytes.convert(bval, vclazz));
    } catch ( UnsupportedEncodingException uee ) {
      logger.fatal("Failed to convert value for key '" + key.toString() + "'");
      return null;
    }

    super.put(key, val);
    return val;
  }

  private void write(K key, V value)  {
    if ( readOnly ) return;
    hdbs.write(key, kclazz, value, vclazz);
  }

  // TODO
  // removed the uue exception code, since we are writing String key/value
  // configuration pairs.
  // need to implement something like DBFields.

  public ArrayList<K> keys(String[] keystoskip) throws HDBLRUMapException {
    try {
      hdbs.iterinit();
    } catch ( StorageException se ) {
      throw new HDBLRUMapException("Error initializing iterator for key retreival", se);
    }

    ArrayList<K> keys = new ArrayList<K>();

    byte[][] skip = new byte[keystoskip.length][];
    for ( int i = 0; i < keystoskip.length; i++ )
      skip[i] = Bytes.strToBytes(keystoskip[i]);

    byte[] bkey;
    K key;
    while ( (bkey = hdbs.iternext()) != null ) {
      try {
        for ( int i = 0; i < skip.length; i++ ) {
          if ( Arrays.equals(bkey, skip[i]) ) {
            bkey = null;
            break;
          }
        }

        if ( bkey == null )
          continue;

        key = kclazz.cast( Bytes.convert(bkey, kclazz) );
        if ( !this.containsKey(key) ) keys.add(key);
      } catch ( UnsupportedEncodingException uee ) {
        logger.warn("Error casting key to " + kclazz, uee);
      }
    }

    // add everything in memory
    Object[] memkeys = this.keySet().toArray();
    for ( int i = 0; i < memkeys.length; i++ )
      keys.add(kclazz.cast(memkeys[i]));

    return keys;
  }

  // faster
  public ArrayList<K> keys() throws HDBLRUMapException {
    try {
      hdbs.iterinit();
    } catch ( StorageException se ) {
      throw new HDBLRUMapException("Error initializing iterator for key retreival", se);
    }

    ArrayList<K> keys = new ArrayList<K>();

    byte[] bkey;
    K key;
    while ( (bkey = hdbs.iternext()) != null ) {
      try {
        key = kclazz.cast( Bytes.convert(bkey, kclazz) );
        if ( !this.containsKey(key) ) keys.add(key);
      } catch ( UnsupportedEncodingException uee ) {
        logger.warn("Error casting key to " + kclazz, uee);
      }
    }

    // add everything in memory
    Object[] memkeys = this.keySet().toArray();
    for ( int i = 0; i < memkeys.length; i++ )
      keys.add(kclazz.cast(memkeys[i]));

    return keys;
  }

  public ArrayList<K> keysFromStore(String[] keystoskip) throws HDBLRUMapException {
    try {
      hdbs.iterinit();
    } catch ( StorageException se ) {
      throw new HDBLRUMapException("Error initializing iterator for key retreival", se);
    }

    ArrayList<K> keys = new ArrayList<K>();

    byte[][] skip = new byte[keystoskip.length][];
    for ( int i = 0; i < keystoskip.length; i++ )
      skip[i] = Bytes.strToBytes(keystoskip[i]);

    byte[] bkey;
    K key;
    while ( (bkey = hdbs.iternext()) != null ) {
      try {
        for ( int i = 0; i < skip.length; i++ ) {
          if ( Arrays.equals(bkey, skip[i]) ) {
            bkey = null;
            break;
          }
        }

        if ( bkey == null )
          continue;

        key = kclazz.cast( Bytes.convert(bkey, kclazz) );
        keys.add(key);
      } catch ( UnsupportedEncodingException uee ) {
        logger.warn("Error casting key to " + kclazz, uee);
      }
    }

    return keys;
  }

  // faster
  public ArrayList<K> keysFromStore() throws HDBLRUMapException {
    try {
      hdbs.iterinit();
    } catch ( StorageException se ) {
      throw new HDBLRUMapException("Error initializing iterator for key retreival", se);
    }

    ArrayList<K> keys = new ArrayList<K>();

    byte[] bkey;
    K key;
    while ( (bkey = hdbs.iternext()) != null ) {
      try {
        key = kclazz.cast( Bytes.convert(bkey, kclazz) );
        keys.add(key);
      } catch ( UnsupportedEncodingException uee ) {
        logger.warn("Error casting key to " + kclazz, uee);
      }
    }

    return keys;
  }

  public ArrayList<K> uniqueKeysFromStore(String[] keystoskip) throws HDBLRUMapException {
    try {
      hdbs.iterinit();
    } catch ( StorageException se ) {
      throw new HDBLRUMapException("Error initializing iterator for key retreival", se);
    }

    ArrayList<K> keys = new ArrayList<K>();

    byte[][] skip = new byte[keystoskip.length][];
    for ( int i = 0; i < keystoskip.length; i++ )
      skip[i] = Bytes.strToBytes(keystoskip[i]);

    byte[] bkey;
    K key;
    while ( (bkey = hdbs.iternext()) != null ) {
      try {
        for ( int i = 0; i < skip.length; i++ ) {
          if ( Arrays.equals(bkey, skip[i]) ) {
            bkey = null;
            break;
          }
        }

        if ( bkey == null )
          continue;

        key = kclazz.cast( Bytes.convert(bkey, kclazz) );
        if ( !this.containsKey(key) ) keys.add(key);
      } catch ( UnsupportedEncodingException uee ) {
        logger.warn("Error casting key to " + kclazz, uee);
      }
    }

    return keys;
  }

  public ArrayList<K> uniqueKeysFromStore() throws HDBLRUMapException {
    try {
      hdbs.iterinit();
    } catch ( StorageException se ) {
      throw new HDBLRUMapException("Error initializing iterator for key retreival", se);
    }

    ArrayList<K> keys = new ArrayList<K>();

    byte[] bkey;
    K key;
    while ( (bkey = hdbs.iternext()) != null ) {
      try {
        key = kclazz.cast( Bytes.convert(bkey, kclazz) );
        if ( !this.containsKey(key) ) keys.add(key);
      } catch ( UnsupportedEncodingException uee ) {
        logger.warn("Error casting key to " + kclazz, uee);
      }
    }

    return keys;
  }

  @Override
  protected boolean removeLRU(AbstractLinkedMap.LinkEntry<K, V> entry) {
    if ( readOnly ) return true;

    write(entry.getKey(), entry.getValue());
    return true;
  }
}
