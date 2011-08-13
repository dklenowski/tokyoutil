package com.orbious.tokyo.util;

import java.io.UnsupportedEncodingException;
import tokyocabinet.Util;

public class ByteOps {

  public static byte[] intToBytes(int i) {
    return Util.packint(i);
  }

  public static int bytesToInt(byte[] bytes) {
    return Util.unpackint(bytes);
  }

  public static byte[] longToBytes(long value) {
    byte[] array = new byte[8];

    array[0] = (byte)(0xff & (value >> 56));
    array[1] = (byte)(0xff & (value >> 48));
    array[2] = (byte)(0xff & (value >> 40));
    array[3] = (byte)(0xff & (value >> 32));
    array[4] = (byte)(0xff & (value >> 24));
    array[5] = (byte)(0xff & (value >> 16));
    array[6] = (byte)(0xff & (value >> 8));
    array[7] = (byte)(0xff & value);

    return array;
  }

  public static long bytesToLong(byte[] array) {
    return
      ((long)(array[0] & 0xff) << 56) |
      ((long)(array[1] & 0xff) << 48) |
      ((long)(array[2] & 0xff) << 40) |
      ((long)(array[3] & 0xff) << 32) |
      ((long)(array[4] & 0xff) << 24) |
      ((long)(array[5] & 0xff) << 16) |
      ((long)(array[6] & 0xff) << 8) |
      ((long)(array[7] & 0xff));
  }

  public static byte[] doubleToBytes(double d) {
    return longToBytes(Double.doubleToRawLongBits(d));
  }

  public static double bytesToDouble(byte[] array) {
    return Double.longBitsToDouble(bytesToLong(array));
  }

  public static byte[] strToBytes(String str) {
    return str.getBytes();
  }

  public static String bytesToStr(byte[] bytes) {
    try {
      return new String(bytes, "UTF-8");
    } catch ( UnsupportedEncodingException uee ) {
      return new String(bytes);
    }
  }
}
