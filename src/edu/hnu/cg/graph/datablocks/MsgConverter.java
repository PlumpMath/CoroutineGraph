package edu.hnu.cg.graph.datablocks;

public interface MsgConverter<MsgValueType> extends BytesToValueConverter<MsgValueType>{
	
	public int getFrom();
	public int getTo();

}

class Msg implements MsgConverter<byte[]>{

	@Override
	public int sizeOf() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] getValue(byte[] array) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setValue(byte[] array, byte[] val) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getFrom() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getTo() {
		// TODO Auto-generated method stub
		return 0;
	}
	
}


