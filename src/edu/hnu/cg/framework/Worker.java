package edu.hnu.cg.framework;

import java.nio.MappedByteBuffer;

import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.Section;
import edu.hnu.cg.graph.datablocks.BytesToValueConverter;
import edu.hnu.cg.graph.datablocks.DataBlockManager;
import edu.hnu.cg.graph.datablocks.IntConverter;
import edu.hnu.cg.graph.datablocks.MsgBytesTovalueConverter;
import edu.hnu.cg.graph.userdefine.GraphConfig;

public class Worker<VertexValueType, EdgeValueType, MsgValueType> extends Thread {

	private byte[] valueTemplate;

	private int id; // worker id
	private int k; // worker : vertices
	private byte[] record;

	private Manager<?, ?, ?> mgr;
	private Section currentSection;
	@SuppressWarnings("rawtypes")
	private Handler handler;
	private MsgBytesTovalueConverter<MsgValueType> msgConverter;
	private BytesToValueConverter<VertexValueType> vertexValueTypeBytesToValueConverter;
	private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;

	private MappedByteBuffer dataBuffer;
	private MappedByteBuffer eDataBuffer;
	private MappedByteBuffer vertexInfoBuffer;
	private MappedByteBuffer indexBuffer;

	private DataBlockManager dataBlockManager;

	/* 顶点信息* */
	// record : length , vid , valueoffset len1-> vid , weight , offset -> len2
	// ...
	private int vid;
	private VertexValueType val;
	private int valueOffset;

	private int inSectionInDegree;
	private int[] insectionEdges; // id offset id offset
	private EdgeValueType[] ievts;

	private int outDegree;
	private int[] outEdges;
	private EdgeValueType[] oevts;

	public Worker(int id, Manager<?, ?, ?> mgr,
			Handler<VertexValueType, EdgeValueType, MsgValueType> handler,
			MsgBytesTovalueConverter<MsgValueType> msgConverter,
			BytesToValueConverter<VertexValueType> vertexValueTypeBytesToValueConverter,
			BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter,
			DataBlockManager dataBlockManager) {
		this.id = id;
		this.mgr = mgr;
		this.handler = handler;

		this.msgConverter = msgConverter;
		this.vertexValueTypeBytesToValueConverter = vertexValueTypeBytesToValueConverter;
		this.edgeValueTypeBytesToValueConverter = edgeValueTypeBytesToValueConverter;
		if (vertexValueTypeBytesToValueConverter != null) {
			valueTemplate = new byte[vertexValueTypeBytesToValueConverter.sizeOf()];
		}
	}

	private boolean running = true;

	@SuppressWarnings("unchecked")
	public void run() {

		while (running) {
			if (currentSection == null) {
				currentSection = mgr.getCurrentSection();
			}

			record = getRecord();
			assert (record != null);

			extractRecord(record); // 解析二进制数据

			// 计算本section内的消息
			for (int i = 0; i < inSectionInDegree; i++) {
				byte[] msg = getMsg(insectionEdges[2 * i + 1]);
				val = (VertexValueType) handler.compute(msgConverter.getFrom(msg),
						msgConverter.getTo(msg), val, msgConverter.getValue(msg));
			}

			// 计算来自section外的消息
			byte[] buff = dataBlockManager.getRawBlock(id);
			if (buff != null) {
				int current = -1;
				for (int i = 0; i < buff.length;) {
					byte[] msg = new byte[msgConverter.sizeOf()];
					System.arraycopy(buff, i, msg, 0, msgConverter.sizeOf());

					int from = msgConverter.getFrom(msg);
					int to = msgConverter.getTo(msg);

					if (i == 0)
						current = from;

					if (from == current && to == vid) {
						val = (VertexValueType) handler.compute(from, to, val,
								msgConverter.getValue(msg));
						i += msgConverter.sizeOf();
					} else {
						if (i != 0) {
							if (from != current) {
								if (i % GraphConfig.cachelineSize == 0) {
									val = (VertexValueType) handler.compute(from, to, val,
											msgConverter.getValue(msg));
									i += msgConverter.sizeOf();
									current = from;
								} else {
									i = (i / 64 + 1) * 64;
									System.arraycopy(buff, i, msg, 0, msgConverter.sizeOf());
									from = msgConverter.getFrom(msg);
									to = msgConverter.getTo(msg);
									val = (VertexValueType) handler.compute(from, to, val,
											msgConverter.getValue(msg));
									i += msgConverter.sizeOf();
								}
							}
						}
					}

				}
			}

			// 更新顶点的value值
			vertexValueTypeBytesToValueConverter.setValue(valueTemplate, val);
			dataBuffer.put(valueTemplate, valueOffset, valueTemplate.length);

			// 同步点
			// 代码TO DO

			// 发送消息
			for (int i = 0; i < outDegree; i++) {
				int to = outEdges[2 * i];
				byte[] msg = handler.genMessage(vid, to, val, oevts[i]);
				if (forward(vid) == forward(to))
					eDataBuffer.put(msg, outEdges[2 * i + 1], msg.length);
				else {
					int workerId = location(to);
					mgr.getWorker(workerId).putMessage(workerId, msg, outEdges[2 * i + 1]);
				}
			}
		}

		// 卸载分区，载入下一个分区

	}

	private void putMessage(int workerId, byte[] msg, int offset) {
		byte[] buff = dataBlockManager.getRawBlock(workerId);
		System.arraycopy(msg, 0, buff, offset, msg.length);
	}

	// vertex : worker = 1 : 1
	// k always 0
	// 如果vertex : worker = n ： 1 太复杂，不想搞了
	public int computeOffset(int i) {
		return id * GraphConfig.verticesToWoker * Graph.offsetWidth + k * 4;
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
		currentSection.getVertexInformationBuffer().get(data, fetchIndex, len);
		return data;
	}

	@SuppressWarnings("unchecked")
	private void extractRecord(byte[] rd) {

		int sizeof = edgeValueTypeBytesToValueConverter == null ? 0
				: edgeValueTypeBytesToValueConverter.sizeOf();
		IntConverter inc = new IntConverter();

		int len = rd.length;
		int i = 0;
		if (len > 12) {
			int validate = readRecord(rd, i, 4, inc);
			i += 4;
			assert (validate == len);
			len = validate;

			vid = readRecord(rd, i, 4, inc);
			i += 4;
			valueOffset = readRecord(rd, i, 4, inc);
			i += 4;
			val = getVertexValue(valueOffset);
			inSectionInDegree = readRecord(rd, i, 4, inc);

			insectionEdges = new int[inSectionInDegree * 2];// vid offset
			if (sizeof == 0)
				ievts = (EdgeValueType[]) new Object[0];
			else
				ievts = (EdgeValueType[]) new Object[inSectionInDegree];

			int p = 0;
			int q = 0;
			while (q < inSectionInDegree) {
				readEdgeRecord(rd, i, sizeof, p, inc, insectionEdges, ievts);
				p += 2;
				i += (8 + sizeof);
				q++;
			}

			outDegree = readRecord(rd, i, 4, inc);
			outEdges = new int[outDegree * 2];
			if (sizeof == 0)
				oevts = (EdgeValueType[]) new Object[0];
			else
				oevts = (EdgeValueType[]) new Object[outDegree];

			p = 0;
			q = 0;

			while (q < outDegree) {
				readEdgeRecord(rd, i, sizeof, p, inc, outEdges, oevts);
				p += 2;
				i += (8 + sizeof);
				q++;
			}
		}

	}

	/**
	 * 读取 vid -> edge_wight -> offset
	 * 
	 * @param rd
	 *            : record
	 * @param pos
	 *            : 当前的record读取位置
	 * @param ts
	 *            : 写入的数组的位置
	 * 
	 * */

	private void readEdgeRecord(byte[] rd, int pos, int len, int ts, IntConverter converter,
			int[] d, EdgeValueType[] v) {
		byte[] tmp = new byte[4];
		byte[] edge = new byte[len];

		// 读取vid
		System.arraycopy(rd, pos, tmp, 0, 4);
		pos += 4;
		d[ts++] = converter.getValue(tmp);

		// 读取edge_weight
		if (len != 0) {
			System.arraycopy(rd, pos, edge, 0, len);
			pos += len;
			v[ts / 2] = edgeValueTypeBytesToValueConverter.getValue(edge);
		}

		// 读取offset
		System.arraycopy(rd, pos, tmp, 0, 4);
		pos += 4;
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

	private byte[] getMsg(int offset) {
		int len = (msgConverter != null ? msgConverter.sizeOf() : 8);
		byte[] msg = new byte[len];
		if (currentSection != null) {
			if (currentSection.getEdgeDataBuffer() != null) {
				currentSection.getEdgeDataBuffer().get(msg, offset, len);
			}
		}
		return msg;

	}

	private int forward(int id) {
		return id / GraphConfig.sectionSize;
	}

	private int location(int id) {
		return id % GraphConfig.sectionSize % GraphConfig.numWorkers;
	}

}
