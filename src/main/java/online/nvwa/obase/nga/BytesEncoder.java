package online.nvwa.obase.nga;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * <p>
 * Title:
 * </p>
 *
 * <p>
 * Description:
 * </p>
 *
 * <p>
 * Copyright: Copyright (c) 2010
 * </p>
 *
 * <p>
 * Company:
 * </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class BytesEncoder extends BytesCodec {
	private ByteArrayOutputStream baos;
	private DataOutputStream dos;

	public BytesEncoder() {
		baos = new ByteArrayOutputStream();
		dos = new DataOutputStream(baos);
	}

	public int size() {
		return baos.size();
	}

	public byte[] toByteArray() {
		return baos.toByteArray();
	}

	public void writeBoolean(boolean v) {
		try {
			dos.writeBoolean(v);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void writeByte(int v) {
		try {
			dos.writeByte(v);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void writeShort(int v) {
		try {
			dos.writeShort(v);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void writeInt(int v) {
		try {
			dos.writeInt(v);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void writeLong(long v) {
		try {
			dos.writeLong(v);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void writeFloat(float v) {
		try {
			dos.writeFloat(v);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void writeDouble(double v) {
		try {
			dos.writeDouble(v);
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * 字符串转换 Java: 1.构造一个新的 String，方法是使用指定的字符集解码指定的字节数组。 String(byte[] bytes,
	 * String charsetName) 2.使用指定的字符集将此 String 解码为字节序列，并将结果存储到一个新的字节数组中。 byte[]
	 * getBytes(String charsetName) 3. Charset Description US-ASCII Seven-bit
	 * ASCII, a.k.a. ISO646-US, a.k.a. the Basic Latin block of the Unicode
	 * character set ISO-8859-1 ISO Latin Alphabet No. 1, a.k.a. ISO-LATIN-1
	 * UTF-8 Eight-bit UCS Transformation Format UTF-16BE Sixteen-bit UCS
	 * Transformation Format, big-endian byte order UTF-16LE Sixteen-bit UCS
	 * Transformation Format, little-endian byte order UTF-16 Sixteen-bit UCS
	 * Transformation Format, byte order identified by an optional byte-order
	 * mark C#: 1.下面的示例演示如何使用 UnicodeEncoding 将 Unicode 字符串编码为字节数组。
	 * UnicodeEncoding unicode = new UnicodeEncoding(); Byte[] encodedBytes =
	 * unicode.GetBytes(unicodeString); 2.然后将该字节数组解码为字符串，以表明没有丢失数据。 String
	 * decodedString = unicode.GetString(encodedBytes,0, encodedBytes.Length);
	 * 3.通过检索由 Unicode 或 BigEndianUnicode 属性返回的 UnicodeEncoding 对象。
	 * 前一个属性返回的编码对象使用 Little-endian 字节顺序，而后一个属性返回的编码对象 则使用 Big-endian 字节顺序。 4.
	 * 通过调用 GetEncoding 方法，其 name 参数的值设置为"utf-16"（对于 Little-endian
	 * 字节顺序）或"utf-16BE"（对于 Big-endian 字节顺序）。
	 */
	public void writeUnicodeString(String text) {
		try {
			if (text == null || text == "") {
				dos.writeShort(0);
			} else {
				byte[] data = text.getBytes(charsetUnicodes);
				dos.writeShort(data.length);
				dos.write(data);
			}
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void writeUTF8String(String text) {
		try {
			if (text == null || text == "") {
				dos.writeShort(0);
			} else {
				byte[] data = text.getBytes(charsetUTF8);
				dos.writeShort(data.length);
				dos.write(data);
			}
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void writeBytes(byte[] bytes) {
		try {
			if (bytes == null) {
				dos.writeInt(0);
			} else {
				dos.writeInt(bytes.length);
				dos.write(bytes);
			}
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}

	public void writeRaw(byte[] bytes) {
		try {
			if (bytes == null) {
			} else {
				dos.write(bytes);
			}
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		}
	}
}
