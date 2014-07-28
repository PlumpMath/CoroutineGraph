package edu.hnu.cg.framework;

public class Msg<MsgValueType> {
	long mmid;
	MsgValueType val;
	
	public Msg(int from,int to, MsgValueType val){
		mmid =  ((long) from << 32) + to;
		this.val = val;
	}
	
	int getFrom(){
		return (int)(mmid >>32);
	}
	
	int getTo(){
		return (int) (mmid & 0x00000000ffffffffL);
	}
	
}
