package edu.hnu.cg.framework;

import java.nio.MappedByteBuffer;
import java.util.Arrays;

import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.Section;
import edu.hnu.cg.graph.datablocks.BytesToValueConverter;
import edu.hnu.cg.graph.datablocks.IntConverter;
import edu.hnu.cg.graph.datablocks.MsgBytesTovalueConverter;

public class Worker<VertexValueType, EdgeValueType, MsgValueType> extends Thread {

	private int id;

	private int k;

	private byte[] record;

	// buffer for msgs which comes from other section of the previous superstep
	private byte[] msgsCurrent;
	private byte[] msgsNext; // buffer msgs comes from other section used for
								// next superstep

	private Manager mgr;
	private Section currentSection;
	private Handler handler;
	private MsgBytesTovalueConverter<MsgValueType> msgValueTypeBytesToValueConverter;
	private BytesToValueConverter<VertexValueType> vertexValueTypeBytesToValueConverter;
	private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;

	private MappedByteBuffer dataBuffer;
	private MappedByteBuffer eDataBuffer;
	private MappedByteBuffer vertexInfoBuffer;
	private MappedByteBuffer indexBuffer;

	private boolean running = true;

	

	

	/***/

	public int computeOffset(int i) {
		return id * 8 + i * 32 * 8;
	}

	public void resetK() {
		k = 0;
	}

	public void run() {

		while (running) {
			if (currentSection == null) {
				currentSection = mgr.getCurrentSection();
			}
			record = getRecord();
			assert (record != null);
			extractRecord(record);
			
			for(int i=0;i<inSectionInDegree;i++){
				MsgValueType msg = getMsgType(insectionEdges[2*i+1]);
				handler.compute(msg);
			}
			
			for(int i=0;i<msgsCurrent.length;i++){
				
			}

			swapBuffer();
		}

	}

	public byte[] getRecord() {
		byte[] data = null;
		int offset = computeOffset(k);
		int fetchIndex = 0;
		if (offset < currentSection.getIndexBuffer().capacity()) {
			fetchIndex = currentSection.getIndexBuffer().getInt(offset);
		}
		int len = currentSection.getVertexInformationBuffer().getInt(fetchIndex);
		data = new byte[len];
		currentSection.getVertexInformationBuffer().get(record, fetchIndex, len);
		return data;
	}
	
	/* 顶点信息* */
	// record : length , vid , valueoffset  len1-> vid , weight , offset -> len2 ...
	private int vid;
	private int valueOffset;
	private VertexValueType val;
	
	private int inSectionInDegree;
	private int[] insectionEdges; // id offset id offset
	private EdgeValueType[] ievts;
	
	private int outDegree;
	private int[] outEdges;
	private EdgeValueType[] oevts;
	
	@SuppressWarnings("unchecked")
	private void extractRecord(byte[] rd) {
		int sizeof = edgeValueTypeBytesToValueConverter == null ? 0 : edgeValueTypeBytesToValueConverter.sizeOf();
		IntConverter inc = new IntConverter();
		
		int len = rd.length;
		int i = 0;
		if (len > 12) {
			int validate = readRecord(rd, i, 4, inc); i += 4;
			
			assert (validate == len);
			len = validate;
			
			vid = readRecord(rd, i, 4, inc); i += 4;
			valueOffset = readRecord(rd,i,4,inc); i+=4;
			val = getVertexValue(valueOffset);
			inSectionInDegree = readRecord(rd, i, 4, inc);
			
			insectionEdges = new int[inSectionInDegree*2];// vid offset
			if(sizeof == 0) ievts = (EdgeValueType[]) new Object[0];
			else ievts =(EdgeValueType[]) new Object[inSectionInDegree];
			
			int p = 0;
			int q = 0;
			while (q < inSectionInDegree) {
				readEdgeRecord(rd, i, 8+sizeof,sizeof,p,inc,insectionEdges,ievts);
				p+=2;
				i+=(8+sizeof);
				q++;
			}
			
			outDegree = readRecord(rd,i,4,inc);
			outEdges = new int[outDegree*2];
			if(sizeof == 0) oevts = (EdgeValueType[]) new Object[0];
			else oevts =(EdgeValueType[]) new Object[outDegree];
			
			p = 0 ;
			q = 0;
			
			while(q < outDegree){
				readEdgeRecord(rd, i, 8+sizeof,sizeof,p,inc,outEdges,oevts);
				p+=2;
				i+=(8+sizeof);
				q++;
			}

		}

	}
	
/*	private int readInt(byte[] rd,int index){
		byte[] tmp = new byte[4];
		
	}
	
	private float readFloat(byte[] rd,int index){
		
	}*/
	
	private  void  readEdgeRecord(byte[] rd,int pos,int len0,int len1,int ts,IntConverter converter,int[] d,EdgeValueType[] v){
		byte[] tmp = new byte[4];
		byte[] edge = new byte[len1] ;
		System.arraycopy(rd, pos, tmp, 0, 4); pos+=4;
		d[ts++] = converter.getValue(tmp);
		if(len1!=0){
			System.arraycopy(rd, pos, edge, 0, len1); pos+=len1;
			v[ts/2] = edgeValueTypeBytesToValueConverter.getValue(edge);
		}
		System.arraycopy(rd, pos, tmp, 0, 4); pos+=4;
		d[ts] = converter.getValue(tmp);
	}

	private <T> T readRecord(byte[] arr, int pos, int len, BytesToValueConverter<T> converter) {
		byte[] tmp = new byte[len];
		System.arraycopy(arr, pos, tmp, 0, len);
		T val = converter.getValue(tmp);
		return val;
	}

	private VertexValueType getVertexValue(int offset) {
		byte[] valueTemplate = new byte[vertexValueTypeBytesToValueConverter.sizeOf()];
		MappedByteBuffer DataBuffer = currentSection.getVertexDataBuffer();
		DataBuffer.get(valueTemplate, offset, valueTemplate.length);
		VertexValueType val = vertexValueTypeBytesToValueConverter.getValue(valueTemplate);
		return val;
	}
	
	
	private MsgValueType readMessage(byte[] buf,int pos ){
		
		
	}

	private MsgValueType getMsgType(int offset){
		byte[] msg = getMsg(offset);
		if(msgValueTypeBytesToValueConverter!= null){
			return msgValueTypeBytesToValueConverter.getValue(msg);
		}
		
		return null;
	}
	
	private byte[] getMsg(int offset) {
		int len = (msgValueTypeBytesToValueConverter != null ? msgValueTypeBytesToValueConverter.sizeOf() : 8);
		byte[] msg = new byte[len];
		if (currentSection != null) {
			if (currentSection.getEdgeDataBuffer() != null) {
				currentSection.getEdgeDataBuffer().get(msg, offset, len);
			}
		}
		return msg;

	}
	


	private void swapBuffer() {
		byte[] tmp = msgsCurrent;
		msgsCurrent = msgsNext;
		msgsNext = tmp;
	}
}
