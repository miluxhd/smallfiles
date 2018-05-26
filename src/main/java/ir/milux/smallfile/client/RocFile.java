package ir.milux.smallfile.client;

import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;

public class RocFile implements Serializable{

    public byte[] binary;
    public String name;
    public String tag;

    public RocFile(String name, byte[] binary, String tag){
        this.name = name;
        this.binary = binary;
        this.tag = tag;
    }

    public byte[] getBinary() {
        return binary;
    }

    public void setBinary(byte[] binary) {
        this.binary = binary;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
    public static byte[] serialize(RocFile rocFile){  return SerializationUtils.serialize(rocFile); }
    public  byte[] serialize(){  return SerializationUtils.serialize(this); }
    public static RocFile deserialize(byte[] bytes){  return (RocFile) SerializationUtils.deserialize(bytes); }

}
