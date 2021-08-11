package online.nvwa.obase.nga;

import java.nio.charset.Charset;

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
public abstract class BytesCodec {
    public static Charset charsetUnicodes = Charset.forName("UTF-16LE");
    public static Charset charsetUTF8 = Charset.forName("UTF-8");
//    public static Charset charsetUTF8 = Charset.forName("gb2312");

    abstract public int size();

    abstract public byte[] toByteArray();

    public boolean compress(){
        throw new RuntimeException("Method not implemented.");
    }
    public boolean uncompress(){
        throw new RuntimeException("Method not implemented.");
    }

    public boolean readBoolean() {
        throw new RuntimeException("Method not implemented.");
    }

    public byte readByte() {
        throw new RuntimeException("Method not implemented.");
    }

    public short readShort() {
        throw new RuntimeException("Method not implemented.");
    }

    public int readInt() {
        throw new RuntimeException("Method not implemented.");
    }

    public long readLong() {
        throw new RuntimeException("Method not implemented.");
    }

    public float readFloat() {
        throw new RuntimeException("Method not implemented.");
    }

    public double readDouble() {
        throw new RuntimeException("Method not implemented.");
    }

    public byte[] readBytes() {
        throw new RuntimeException("Method not implemented.");
    }

    public String readUnicodeString() {
        throw new RuntimeException("Method not implemented.");
    }

    public String readUTF8String() {
        throw new RuntimeException("Method not implemented.");
    }

    public void writeBoolean(boolean v) {
        throw new RuntimeException("Method not implemented.");
    }

    public void writeByte(int v) {
        throw new RuntimeException("Method not implemented.");
    }

    public void writeShort(int v) {
        throw new RuntimeException("Method not implemented.");
    }

    public void writeInt(int v) {
        throw new RuntimeException("Method not implemented.");
    }

    public void writeLong(long v) {
        throw new RuntimeException("Method not implemented.");
    }

    public void writeFloat(float v) {
        throw new RuntimeException("Method not implemented.");
    }

    public void writeDouble(double v) {
        throw new RuntimeException("Method not implemented.");
    }

    public void writeBytes(byte[] bytes) {
        throw new RuntimeException("Method not implemented.");
    }

    public void writeRaw(byte[] bytes) {
        throw new RuntimeException("Method not implemented.");
    }

    public void writeUnicodeString(String text) {
        throw new RuntimeException("Method not implemented.");
    }

    public void writeUTF8String(String text) {
        throw new RuntimeException("Method not implemented.");
    }

}
