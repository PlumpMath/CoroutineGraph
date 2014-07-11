package edu.hnu.cg.graph.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class TestMapedByteBuffer {

	public static void main(String[] args) throws IOException{
		FileChannel fChannel = new RandomAccessFile("test", "rw").getChannel();
		
		MappedByteBuffer mbb = fChannel.map(MapMode.READ_WRITE, 0, 1024);
		
		byte[] content = new byte[1024];
		for(int i=0;i<1024;i++)
			content[i] = (byte) i;
		
		mbb.put(content);
		
	
		fChannel.close();
		mbb = null;
	
	}
}
