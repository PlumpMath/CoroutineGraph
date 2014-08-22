package edu.hnu.cg.framework;

public interface Handler<VertexValueType,EdgeValueType,MsgValueType> {
	VertexValueType compute(int from , int to , VertexValueType val ,MsgValueType msg);
	byte[] genMessage(int from,int to,VertexValueType val,EdgeValueType weight);
}
