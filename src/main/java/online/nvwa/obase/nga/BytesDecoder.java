package online.nvwa.obase.nga;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2010</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class BytesDecoder extends BytesCodec {
    private DataInputStream dis;
    byte[] data;

    public BytesDecoder(byte[] data) {
        this.data = data;
        dis = new DataInputStream(new ByteArrayInputStream(data));
    }

    public int size() {
        return data.length;
    }

    public byte[] toByteArray() {
        return data;
    }

    public boolean readBoolean() {
        try {
            return dis.readBoolean();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public byte readByte() {
        try {
            return dis.readByte();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public short readShort() {
        try {
            return dis.readShort();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public int readInt() {
        try {
            return dis.readInt();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public long readLong() {
        try {
            return dis.readLong();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public float readFloat() {
        try {
            return dis.readFloat();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public double readDouble() {
        try {
            return dis.readDouble();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * 字符串转换
     * Java:
     * 1.构造一个新的 String，方法是使用指定的字符集解码指定的字节数组。
     * String(byte[] bytes, String charsetName)
     * 2.使用指定的字符集将此 String 解码为字节序列，并将结果存储到一个新的字节数组中。
     * byte[] 	getBytes(String charsetName)
     * 3. Charset       Description
     *    US-ASCII 	Seven-bit ASCII, a.k.a. ISO646-US, a.k.a. the Basic Latin block of the Unicode character set
     *    ISO-8859-1   	ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1
     *    UTF-8 	Eight-bit UCS Transformation Format
     *    UTF-16BE 	Sixteen-bit UCS Transformation Format, big-endian byte order
     *    UTF-16LE 	Sixteen-bit UCS Transformation Format, little-endian byte order
     *    UTF-16 	Sixteen-bit UCS Transformation Format, byte order identified by an optional byte-order mark
     * C#:
     * 1.下面的示例演示如何使用 UnicodeEncoding 将 Unicode 字符串编码为字节数组。
     * UnicodeEncoding unicode = new UnicodeEncoding();
     * Byte[] encodedBytes = unicode.GetBytes(unicodeString);
     * 2.然后将该字节数组解码为字符串，以表明没有丢失数据。
     * String decodedString = unicode.GetString(encodedBytes,0, encodedBytes.Length);
     * 3.通过检索由 Unicode 或 BigEndianUnicode 属性返回的 UnicodeEncoding 对象。
     * 前一个属性返回的编码对象使用 Little-endian 字节顺序，而后一个属性返回的编码对象
     * 则使用 Big-endian 字节顺序。
     * 4. 通过调用 GetEncoding 方法，其 name 参数的值设置为"utf-16"（对于 Little-endian
     *  字节顺序）或"utf-16BE"（对于 Big-endian 字节顺序）。
     */
    public String readUTF8String() {
        try {
            int len = dis.readShort();
            if (len < 0) {
                throw new IOException();
            } else if (len == 0) {
                return null;
            } else {
                byte[] data = new byte[len];
                dis.read(data);
                return new String(data, charsetUTF8);
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public byte[] readBytes() {
        try {
            int len = dis.readInt();
            if (len < 0) {
                throw new IOException();
            } else if (len == 0) {
                return null;
            } else {
                byte[] data = new byte[len];
                dis.read(data);
                return data;
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
