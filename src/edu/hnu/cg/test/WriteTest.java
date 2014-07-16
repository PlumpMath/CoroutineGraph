package edu.hnu.cg.test;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import edu.hnu.cg.graph.Filename;
import edu.hnu.cg.graph.Section;
import edu.hnu.cg.graph.userdefine.GraphConfig;
import edu.hnu.cg.util.BufferedDataInputStream;

public class WriteTest {
	
	private static Random random = new Random();
	private static int partition(long[] arr ,int left ,int right){
		int i = left, j = right;
		long tmp;
		long pivot = arr[left + random.nextInt(right - left + 1)];

		while (i <= j) {
			while (arr[i] < pivot)
				i++;
			while (arr[j] > pivot)
				j--;

			if (i <= j) {
				// 交换edge
				tmp = arr[i];
				arr[i] = arr[j];
				arr[j] = tmp;
				// 同时交换发生交换的边所对应的value

				i++;
				j--;
			}

		}

		return i;
	}
	
	public static void quickSort(long[] arr,int left,int right){
		if(left < right){
			int index = partition(arr,left,right);
			if(left < index-1){
				quickSort(arr,left,index-1);
			}
			if(index < right){
				quickSort(arr,index,right);
			}
		}
	}

	static long reverseOffset(long offset) {
		return (~offset) + 1;
	}

	static long pack(int a, int b) {
		return ((long) a << 32) + b;
	}

	static int getFirst(long e) {
		return (int) (e >> 32);
	}
	
	static int getSecond(long e) {
		return (int) (e & 0x00000000ffffffffL);
	}
	
	public static byte[] intToByteArray(int i) {
		byte[] result = new byte[4];
		// 由高位到低位
		result[0] = (byte) ((i >> 24) & 0xFF);
		result[1] = (byte) ((i >> 16) & 0xFF);
		result[2] = (byte) ((i >> 8) & 0xFF);
		result[3] = (byte) (i & 0xFF);
		return result;
	}
	
	/**
	 * byte[]转int
	 * 
	 * @param bytes
	 * @return
	 */
	public static int byteArrayToInt(byte[] bytes) {
		int value = 0;
		// 由高位到低位
		for (int i = 0; i < 4; i++) {
			int shift = (4 - 1 - i) * 8;
			value += (bytes[i] & 0x000000FF) << shift;// 往高位游
		}
		return value;
	}

	public static byte[] longToByteArray(long val) {
		byte[] array = new byte[8];

		array[0] = (byte) ((val >>> 56) & 0xff);
		array[1] = (byte) ((val >>> 48) & 0xff);
		array[2] = (byte) ((val >>> 40) & 0xff);
		array[3] = (byte) ((val >>> 32) & 0xff);
		array[4] = (byte) ((val >>> 24) & 0xff);
		array[5] = (byte) ((val >>> 16) & 0xff);
		array[6] = (byte) ((val >>> 8) & 0xff);
		array[7] = (byte) (val & 0xff);

		return array;
	}

	

	public static long byteArrayToLong(byte[] array) {
		return ((long) (array[0] & 0xff) << 56) + ((long) (array[1] & 0xff) << 48) + ((long) (array[2] & 0xff) << 40) + ((long) (array[3] & 0xff) << 32)
				+ ((long) (array[4] & 0xff) << 24) + ((long) (array[5] & 0xff) << 16) + ((long) (array[6] & 0xff) << 8) + ((long) array[7] & 0xff);
	}

	public static void main(String[] args) throws IOException{
		 // 边上的值的大小
		// int msgSize = (msgValueTypeBytesToValueConverter !=null ?
		// msgValueTypeBytesToValueConverter.sizeOf() : 16); // id id value
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("test")));
		
		int cachelineSize = GraphConfig.cachelineSize;

		byte[] offsetTypeTemplate = new byte[8];

		
			File shovelFile = new File("google.0.shovel");

			long[] edges = new long[(int) shovelFile.length() / 8];
			

			// 处理边
			BufferedDataInputStream in = new BufferedDataInputStream(new FileInputStream(shovelFile));
			for (int k = 0; k < edges.length; k++) {
				long l = in.readLong();
				edges[k] = l;
			}
			in.close();
			quickSort(edges, 0, edges.length - 1);

			long currentInSectionOffset = 0;
			long currentOutSectionOffset = 0;
			long valueOffset = 0;
			long fetchIndex = 0;
			int curvid = 0;
			int isstart = 0;
			int currentPos = 0;
			int valuePos = 0;

			// int outSectionEdgeCounter = outSectionEdgeCounters[i];

			long[] inedgeIndexer = new long[edges.length];
			byte[] inedgeValue = new byte[inedgeIndexer.length*  8];

			currentInSectionOffset = 0;
			for (int k = 0; k < edges.length; k++) {
					inedgeIndexer[k] = pack(getSecond(edges[k]), getFirst(edges[k]));
					System.arraycopy(longToByteArray((long) k * cachelineSize), 0, inedgeValue, (k *  8) , 8);
			}

			quickSort(inedgeIndexer,  0, inedgeIndexer.length - 1);

			/*
			 * for(int k=0;k<inSectionEdgeCounter;k++){
			 * sectioEDataWriter[i].writeLong(inedgeIndexer[k]); }
			 */

			// 从边构建邻接表
			for (int s = 0; s < edges.length; s++) {
				int from = getFirst(edges[s]);

				if (from != curvid) {
//					System.out.println("Processing vertex : " + curvid);
					int tmp = currentPos;
					int count = s - isstart;
					
					dos.writeInt(curvid);
					
					for(int p = isstart ;p<s;p++){
						dos.writeInt(getSecond(edges[p]));
					}
					
					dos.writeInt(0xffffffff);

					/*while (curvid == getFirst(inedgeIndexer[currentPos])) {
						count++;
						currentPos++;
					}

					int length = count * 12 + 16;
					byte[] record = new byte[length];
					int curstart = 0;
					// 写入record的头信息 : 长度 顶点id 顶点value的offset
					System.arraycopy(intToByteArray(length), 0, record, curstart, 4);
					curstart += 4;
					//sectionAdjWriter[i].writeInt(length);
					System.arraycopy(intToByteArray(curvid), 0	, record, curstart, 4);
					curstart += 4;
//					sectionAdjWriter[i].writeInt(curvid);
					System.arraycopy(longToByteArray(valueOffset), 0, record, curstart, 8);
					curstart += 8;
//					sectionAdjWriter[i].writeLong(valueOffset);


					// 写入在本分区内的入边的信息 id weight offset
					while (tmp < currentPos) {
						System.arraycopy(intToByteArray(getSecond(inedgeIndexer[tmp])),0,record,curstart,4);
						curstart += 4;
//						sectionAdjWriter[i].writeInt(getSecond(inedgeIndexer[currentPos]));
//						sectionAdjWriter[i].write(edgeValueTemplate);
						System.arraycopy(inedgeValue, (tmp * ( 8) ), record, curstart, 8);
						curstart += 8;
//						sectionAdjWriter[i].writeLong(reverseOffset(byteArrayToLong(offsetTypeTemplate)));
						tmp++;
					}

					// 写入record的出边信息 id 权重 边offset
					for (int p = isstart; p < s; p++) {

						// 写入邻接边id
						System.arraycopy(intToByteArray(getSecond(edges[p])), 0, record, curstart, 4); curstart += 4;
//						sectionAdjWriter[i].writeInt(Integer.reverseBytes(getSecond(edges[p])));
						// 写入邻接边的权值
//						System.arraycopy(edgeValues, p * sizeof, record, curstart, sizeof); curstart += sizeof;
//						sectionAdjWriter[i].write(edgeValueTemplate);
						// 写入邻接边的offset
						System.arraycopy(longToByteArray(currentInSectionOffset), 0, record, curstart, 8); curstart += 8;
//							sectionAdjWriter[i].writeLong(currentInSectionOffset);
						currentInSectionOffset += cachelineSize;
					}

					dos.write(record);
					

					
					valueOffset += cachelineSize;
*/
					curvid = from;
					isstart = s;
				}

			}

		

			dos.flush();
			dos.close();
		
		
	}
}
