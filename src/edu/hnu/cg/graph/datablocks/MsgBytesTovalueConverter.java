package edu.hnu.cg.graph.datablocks;

public interface MsgBytesTovalueConverter<MsgValueType> extends BytesToValueConverter<MsgValueType> {
	
	int getFrom(byte[] msg);
	int getTo(byte[] msg);
}
