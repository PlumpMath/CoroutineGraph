package edu.hnu.cg.graph.datablocks;

public interface BytesToValueConverter<T> {
	
	public int sizeOf();
	public T getValue(byte[] array);
	public void setValue(byte[] array,T val);

}
