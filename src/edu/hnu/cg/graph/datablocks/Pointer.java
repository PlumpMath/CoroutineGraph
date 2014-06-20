package edu.hnu.cg.graph.datablocks;

public class Pointer {
	public int blockId;
	public int offset;
	public Pointer(int blockId,int offset){
		this.blockId = blockId;
		this.offset = offset;
	}

}
