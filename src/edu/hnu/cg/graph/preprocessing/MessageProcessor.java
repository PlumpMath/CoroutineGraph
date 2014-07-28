package edu.hnu.cg.graph.preprocessing;

public interface MessageProcessor<MsgValueType> {
	MsgValueType receiveMessageValue(int from ,int to,String token);
}


