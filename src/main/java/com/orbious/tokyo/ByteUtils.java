package com.orbious.tokyo;

import java.io.UnsupportedEncodingException;
import tokyocabinet.Util;

public class ByteUtils {

  public static byte[] intToBytes(int i) {
    return Util.packint(i);
  }

  public static int bytesToInt(byte[] b) {
    return Util.unpackint(b);
  }

  public static byte[] longToBytes(long l) {
    byte[] array = new byte[8];

    array[0] = (byte)(0xff & (l >> 56));
    array[1] = (byte)(0xff & (l >> 48));
    array[2] = (byte)(0xff & (l >> 40));
    array[3] = (byte)(0xff & (l >> 32));
    array[4] = (byte)(0xff & (l >> 24));
    array[5] = (byte)(0xff & (l >> 16));
    array[6] = (byte)(0xff & (l >> 8));
    array[7] = (byte)(0xff & l);

    return array;
  }

  public static long bytesToLong(byte[] b) {
    return
      ((long)(b[0] & 0xff) << 56) |
      ((long)(b[1] & 0xff) << 48) |
      ((long)(b[2] & 0xff) << 40) |
      ((long)(b[3] & 0xff) << 32) |
      ((long)(b[4] & 0xff) << 24) |
      ((long)(b[5] & 0xff) << 16) |
      ((long)(b[6] & 0xff) << 8) |
      ((long)(b[7] & 0xff));
  }

  public static byte[] doubleToBytes(double d) {
    return longToBytes(Double.doubleToRawLongBits(d));
  }

  public static double bytesToDouble(byte[] b) {
    return Double.longBitsToDouble(bytesToLong(b));
  }

  public static byte[] strToBytes(String str) {
    return str.getBytes();
  }

  public static String bytesToStr(byte[] b) {
    try {
      return new String(b, "UTF-8");
    } catch ( UnsupportedEncodingException uee ) {
      return new String(b);
    }
  }
}
