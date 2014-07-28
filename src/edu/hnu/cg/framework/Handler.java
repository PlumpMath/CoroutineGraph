package edu.hnu.cg.framework;

public interface Handler<MsgValueType> {
	void compute(MsgValueType msg);
}
