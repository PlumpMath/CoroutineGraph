package edu.hnu.cg.util;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;

import sun.misc.Unsafe;
@SuppressWarnings("restriction")
public class Tools {

	public static Unsafe getUnsafe(){
		return THE_UNSAFE;
	}
	
	private static Unsafe THE_UNSAFE;
	static {
		try{
			@SuppressWarnings("unused")
			final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>() {
				@Override
				public Unsafe run() throws Exception {
					Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
					theUnsafe.setAccessible(true);
					return (Unsafe)theUnsafe.get(null);
				}
			};
			
		}catch(Exception e){
			throw new RuntimeException("Unable to load unsafe",e);
		}
	}
}
