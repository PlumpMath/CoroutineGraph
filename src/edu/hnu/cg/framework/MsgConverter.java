package edu.hnu.cg.framework;

import edu.hnu.cg.graph.datablocks.BytesToValueConverter;
import edu.hnu.cg.graph.datablocks.LongConverter;
import edu.hnu.cg.graph.datablocks.MsgBytesTovalueConverter;

public class MsgConverter<MsgValueType> implements MsgBytesTovalueConverter<MsgValueType>{
	
	private BytesToValueConverter<MsgValueType> msgValueBytesToValueConverter;
	
	
	public MsgConverter(BytesToValueConverter<MsgValueType> c){
		msgValueBytesToValueConverter = c;
	}
	
	private long getMmid(byte[] msg){
		byte[] mmid = new byte[8];
		System.arraycopy(msg, 0, mmid, 0, 8);
		return new LongConverter().getValue(mmid);
	}
	
	public int getFrom(byte[] msg){
		long mmid = getMmid(msg);
		return (int)(mmid >>32);
	}
	public int getTo(byte[] msg){
		long mmid = getMmid(msg);
		return (int) (mmid & 0x00000000ffffffffL);
	}
	
	@Override
	public int sizeOf() {
		return 8 + msgValueBytesToValueConverter.sizeOf();
	}
	
	@Override
	public MsgValueType getValue(byte[] msg) {
		byte[] value = new byte[msgValueBytesToValueConverter.sizeOf()];
		System.arraycopy(msg, 8, value, 0, msgValueBytesToValueConverter.sizeOf());
		return msgValueBytesToValueConverter.getValue(value);
	}
	
	@Override
	public void setValue(byte[] msg, MsgValueType val) {
		byte[] value = new byte[msgValueBytesToValueConverter.sizeOf()];
		msgValueBytesToValueConverter.setValue(value, val);
		System.arraycopy(value, 0, msg, 8, value.length);
	}

}


