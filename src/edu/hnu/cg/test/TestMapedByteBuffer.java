package edu.hnu.cg.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class TestMapedByteBuffer {

	private static void clean(final MappedByteBuffer buffer) {
		AccessController.doPrivileged(new PrivilegedAction<Object>() {
			@Override
			public Object run() {
				try {
					Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);
					getCleanerMethod.setAccessible(true);
					sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(buffer, new Object[0]);
					cleaner.clean();
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			}
		});
	}
	public static void main(String[] args) throws IOException{
		RandomAccessFile raf = new RandomAccessFile("test", "rw");
		FileChannel fChannel = raf.getChannel();
		/*byte[] l = new byte[1024];
		int len = l.length;
		for(int i=0;i<l.length;i++) l[i] = (byte)(len--);
		
		MappedByteBuffer mbb = fChannel.map(MapMode.READ_WRITE, 0, 1024);
		mbb.put(l);
	
		clean(mbb);
		fChannel.close();
		raf.close();*/
		
		byte[] l = new byte[(int)raf.length()];
		
		ByteBuffer bb = ByteBuffer.wrap(l);
		fChannel.read(bb);
		
		for(int i=0;i<l.length;i++){
			System.out.println(l[i]);
		}
	}
}
