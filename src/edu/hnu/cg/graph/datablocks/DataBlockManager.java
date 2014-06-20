package edu.hnu.cg.graph.datablocks;

import java.util.ArrayList;

public class DataBlockManager {
	private ArrayList<byte[]> blocks = new ArrayList<byte[]>(32768);
	
	public DataBlockManager(){}
	
	public int allocateBlock(int numBytes){
		byte[] dataBlock = new byte[numBytes];
		
		synchronized(this){
			int blockId = blocks.size();
			blocks.add(blockId, dataBlock);
			return blockId;
		}
	}
	
	public byte[] getRawBlock(int blockId){
		byte[] rb = blocks.get(blockId);
		
		if(rb == null){
			throw new IllegalStateException("Null-reference!");
		}
		
		return rb;
	}
	
	public void reset(){
		for(int i=0;i<blocks.size();i++){
			if(blocks.get(i)!=null){
				throw new RuntimeException("Tried to reset block manager,but it was non-empty at index : " + i);
			}
		}
		
		blocks.clear();
	}
	
	public boolean empty(){
		for(int i=0;i<blocks.size();i++){
			if(blocks.get(i)!=null)
				return false;
		}
		return true;
		
	}
	
	public void release(int blockId){
		blocks.set(blockId, null);
	}
	
	public <T> T dereference(Pointer ptr,BytesToValueConverter<T> conv){
		if(ptr == null){
			throw new IllegalStateException("Tried to dereference a null pointer");
		}
		
		byte[] arr = new byte[conv.sizeOf()];
		System.arraycopy(getRawBlock(ptr.blockId), ptr.offset, arr, 0, arr.length);
		return conv.getValue(arr);
	}
	
	public <T> void writeValue(Pointer ptr,BytesToValueConverter<T> conv,T value){
		if(ptr == null){
			throw new IllegalStateException("Tried to dereference a null pointer");
		}
		
		byte[] arr = new byte[conv.sizeOf()];
		conv.setValue(arr, value);
		System.arraycopy(arr, 0, getRawBlock(ptr.blockId), ptr.offset, arr.length);
		
	}
	
	public <T> void writeValue(Pointer ptr,byte[] arr){
		if(ptr == null){
			throw new IllegalStateException("Tried to dereference a null pointer");
		}
		System.arraycopy(arr, 0, getRawBlock(ptr.blockId), ptr.offset, arr.length);
	}

}











