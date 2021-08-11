package online.nvwa.obase.nga;

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
public class DataStream {
    /**
     * size|protocol|data
     * protocol 4 byte: enum
     * size 4 byte: int
     * data: byte[]
     */
    //从用户端发过来的NGAPackage头长9字节，不包括 发送者4字节，这个由服务器根据接收者ID赋予。
    //从服务器发往客户端的包头长13字节，包含发送者id 4字节.这样主要避免发送者在客户端造假.
    private BytesCodec bytesCodec;

    public DataStream(byte[] data) {
        this.bytesCodec = new BytesDecoder(data);
    }

    public DataStream() {
        this.bytesCodec = new BytesEncoder();
    }
    
    public int size() {
        return bytesCodec.size();
    }

    public byte[] toBytes() {
        return bytesCodec.toByteArray();
    }
    public boolean readBoolean() {
        return bytesCodec.readBoolean();
    }

    public byte readByte() {
        return bytesCodec.readByte();
    }

    public short readShort() {
        return bytesCodec.readShort();
    }

    public int readInt() {
        return bytesCodec.readInt();
    }

    public long readLong() {
        return bytesCodec.readLong();
    }

    public float readFloat() {
        return bytesCodec.readFloat();
    }

    public double readDouble() {
        return bytesCodec.readDouble();
    }
    
    /**
     * 返回NULL或非0长度字串。
     **/
    public String readString() {
        return bytesCodec.readUTF8String();
    }

    public byte[] readBytes() {
        return bytesCodec.readBytes();
    }

    public DataStream writeBoolean(boolean v) {
        bytesCodec.writeBoolean(v);
        return this;
    }

    public DataStream writeByte(int v) {
        bytesCodec.writeByte(v);
        return this;
    }

    public DataStream writeShort(int v) {
        bytesCodec.writeShort(v);
        return this;
    }

    public DataStream writeInt(int v) {
        bytesCodec.writeInt(v);
        return this;
    }

    public DataStream writeLong(long v) {
        bytesCodec.writeLong(v);
        return this;
    }

    public DataStream writeFloat(float v) {
        bytesCodec.writeFloat(v);
        return this;
    }

    public DataStream writeDouble(double v) {
        bytesCodec.writeDouble(v);
        return this;
    }

    public DataStream writeString(String text) {
        bytesCodec.writeUTF8String(text);
        return this;
    }

    public DataStream writeBytes(byte[] bytes) {
        bytesCodec.writeBytes(bytes);
        return this;
    }
}
