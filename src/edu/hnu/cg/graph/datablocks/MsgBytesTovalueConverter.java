package edu.hnu.cg.graph.datablocks;

public interface MsgBytesTovalueConverter<MsgValueType> extends BytesToValueConverter<MsgValueType> {
	
	int getSender();
	int getReceiver();
}
