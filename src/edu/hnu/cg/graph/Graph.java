package edu.hnu.cg.graph;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

//import edu.hnu.cg.framework.Worker;
import edu.hnu.cg.graph.datablocks.BytesToValueConverter;
//import edu.hnu.cg.graph.datablocks.FloatConverter;
import edu.hnu.cg.graph.preprocessing.EdgeProcessor;
import edu.hnu.cg.graph.preprocessing.VertexProcessor;
import edu.hnu.cg.graph.userdefine.GraphConfig;
import edu.hnu.cg.util.BufferedDataInputStream;

public class Graph<VertexValueType extends Number, EdgeValueType extends Number, MsgValueType> {

	public enum graphFormat {
		EDGELIST, ADJACENCY
	};

	private String graphFilename;
	private graphFormat format;
	private long numEdges = 0;
	private VertexProcessor<VertexValueType> vertexProcessor;
	private EdgeProcessor<EdgeValueType> edgeProcessor;
	private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;
	private BytesToValueConverter<VertexValueType> verterxValueTypeBytesToValueConverter;
	private BytesToValueConverter<MsgValueType> msgConverter;

	private byte[] vertexValueTemplate;
	private byte[] edgeValueTemplate;

	public static int offsetWidth;
	public static int sectionSize;
	public static int numVertices;
	public static int numSections;
	public static byte[] cachelineTemplate;
	public static int[] lengthsOfWorkerMsgsPool;

	public Section[] sections;

	static {

		offsetWidth = 4; // 4 或者 8 个字节
		sectionSize = GraphConfig.sectionSize;
		numVertices = GraphConfig.numVertices;

		if (numVertices % sectionSize == 0)
			numSections = numVertices / sectionSize;
		else
			numSections = numVertices / sectionSize + 1;

		cachelineTemplate = new byte[GraphConfig.cachelineSize];
		lengthsOfWorkerMsgsPool = new int[GraphConfig.numWorkers];

	}

	private DataOutputStream[] sectionAdjWriter; // section的邻接表文件输出流
	private DataOutputStream[] sectionVDataWriter;// 存储本section内顶点的value
	// private DataOutputStream[] sectioEDataWriter;// 存储本section内与边相关的消息的文件
	private DataOutputStream[] sectionShovelWriter;
	private DataOutputStream[] sectionVertexValueShovelWriter;
	private DataOutputStream[] sectionFetchIndexWriter;

	private int[] inSectionEdgeCounters;
	private int[] outSectionEdgeCounters;

	public Graph(String filename, String _format, BytesToValueConverter<EdgeValueType> _edgeValueTypeBytesToValueConverter,
			BytesToValueConverter<VertexValueType> _verterxValueTypeBytesToValueConverter, VertexProcessor<VertexValueType> _vertexProcessor,
			EdgeProcessor<EdgeValueType> _edgeProcessor) throws FileNotFoundException {

		graphFilename = filename;
		if (_format.equals("edgelist"))
			format = graphFormat.EDGELIST;
		else if (_format.equals("adjacency"))
			format = graphFormat.ADJACENCY;

		vertexProcessor = _vertexProcessor;
		edgeProcessor = _edgeProcessor;
		edgeValueTypeBytesToValueConverter = _edgeValueTypeBytesToValueConverter;
		verterxValueTypeBytesToValueConverter = _verterxValueTypeBytesToValueConverter;

		sections = new Section[numSections];

		sectionAdjWriter = new DataOutputStream[numSections];
		sectionVDataWriter = new DataOutputStream[numSections];
		// sectioEDataWriter = new DataOutputStream[numSections];
		sectionShovelWriter = new DataOutputStream[numSections];
		sectionFetchIndexWriter = new DataOutputStream[numSections];
		sectionVertexValueShovelWriter = new DataOutputStream[numSections];

		inSectionEdgeCounters = new int[numSections];
		outSectionEdgeCounters = new int[numSections];

		for (int i = 0; i < numSections; i++) {
			sectionAdjWriter[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Filename.getSectionFilename(graphFilename, i))));
			sectionVDataWriter[i] = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream(Filename.getSectionVertexDataFilename(graphFilename, i))));
			// sectioEDataWriter[i] = new DataOutputStream(new
			// BufferedOutputStream(new
			// FileOutputStream(Filename.getSectionEdgeDataFilename(graphFilename,
			// i))));
			sectionShovelWriter[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Filename.getSectionShovelFilename(graphFilename, i))));
			sectionFetchIndexWriter[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Filename.getSectionFetchIndexFilename(
					graphFilename, i))));
			sectionVertexValueShovelWriter[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Filename.getSectionVertexShovelFilename(
					graphFilename, i))));
		}

		if (edgeValueTypeBytesToValueConverter != null) {
			edgeValueTemplate = new byte[edgeValueTypeBytesToValueConverter.sizeOf()];
		} else {
			edgeValueTemplate = new byte[0];
		}

		if (verterxValueTypeBytesToValueConverter != null) {
			vertexValueTemplate = new byte[verterxValueTypeBytesToValueConverter.sizeOf()];
		} else {
			vertexValueTemplate = new byte[0];
		}

	}

	private static final String vertexToEdgeSeparate = ":";
	private static final String idToValueSeparate = ",";
	private static final String edgeToEdgeSeparate = "->";

	// private static final long recordSep = 0xFFFFFFFFFFFFFFFFL;

	public void readData() throws IOException {
		BufferedReader bReader = new BufferedReader(new FileReader((new File(graphFilename))));

		String ln = null;
		long lnNum = 0;

		if (format == graphFormat.EDGELIST) {
			while ((ln = bReader.readLine()) != null) {
				lnNum++;
				if (lnNum % 1000000 == 0)
					System.out.println("Reading line: " + lnNum);

				String[] tokenStrings = ln.split("\\s");
				// System.out.println(Arrays.toString(tokenStrings));
				if (tokenStrings.length == 2) {
					this.addEdge(Integer.parseInt(tokenStrings[0]), Integer.parseInt(tokenStrings[1]), null);
				} else if (tokenStrings.length == 3) {
					this.addEdge(Integer.parseInt(tokenStrings[0]), Integer.parseInt(tokenStrings[1]), tokenStrings[2]);
				}
			}

		} else if (format == graphFormat.ADJACENCY) {
			while ((ln = bReader.readLine()) != null) {
				// id,value : id,value->id,value->id,value
				lnNum++;
				if (lnNum % 1000000 == 0)
					System.out.println("Reading line:" + lnNum);
				extractLine(ln);
			}

		}

		for (int i = 0; i < numSections; i++) {
			sectionShovelWriter[i].flush();
			sectionVertexValueShovelWriter[i].flush();
		}

		bReader.close();
		process();

	}

	public void process() throws IOException {

		int sizeof = (edgeValueTypeBytesToValueConverter != null ? edgeValueTypeBytesToValueConverter.sizeOf() : 0); // 边上的值的大小
		int sizeofValue = (verterxValueTypeBytesToValueConverter != null ? verterxValueTypeBytesToValueConverter.sizeOf() : 4);// 顶点值的size大小
		int msgSize = (msgConverter != null ? msgConverter.sizeOf() : 16); // id
																														// id
																														// value
		int cachelineSize = GraphConfig.cachelineSize;

		// byte[] offsetTypeTemplate = new byte[8];
		int[] copyOfLengthsOfWorkerMsgsPool = new int[GraphConfig.numWorkers];

		for (int i = 0; i < numSections; i++) {
			File shovelFile = new File(Filename.getSectionShovelFilename(graphFilename, i));
			File vertexShovleFile = new File(Filename.getSectionVertexShovelFilename(graphFilename, i));

			long[] edges = new long[(int) shovelFile.length() / (8 + sizeof)];
			byte[] edgeValues = new byte[edges.length * sizeof];

			long len = vertexShovleFile.length();
			long[] vertices = null;
			byte[] vertexValues = null;
			if (len != 0) {
				vertices = new long[(int) len / (8 + sizeofValue)];
				vertexValues = new byte[vertices.length * sizeofValue];

				BufferedDataInputStream vin = new BufferedDataInputStream(new FileInputStream(vertexShovleFile));

				for (int k = 0; k < vertices.length; k++) {
					long vid = vin.readLong();
					vertices[k] = vid;
					vin.readFully(vertexValueTemplate);
					System.arraycopy(vertexValueTemplate, 0, vertexValues, k * sizeofValue, sizeofValue);
				}
				vin.close();
				quickSort(vertices, vertexValues, sizeofValue, 0, vertices.length - 1);
			}

			vertexShovleFile.delete();

			// 处理边
			BufferedDataInputStream in = new BufferedDataInputStream(new FileInputStream(shovelFile));
			for (int k = 0; k < edges.length; k++) {
				long l = in.readLong();
				edges[k] = l;
				in.readFully(edgeValueTemplate);
				System.arraycopy(edgeValueTemplate, 0, edgeValues, i * sizeof, sizeof);
			}
			numEdges += edges.length;
			in.close();
			shovelFile.delete();
			quickSort(edges, edgeValues, sizeof, 0, edges.length - 1);

			// 本来应该使用long，这里使用int节省空间
			int currentInSectionOffset = 0;
			int valueOffset = 0;
			int fetchIndex = 0;
			int curvid = 0;
			int isstart = 0;
			int currentPos = 0;
			int valuePos = 0;

			int inSectionEdgeCounter = inSectionEdgeCounters[i];
			// int outSectionEdgeCounter = outSectionEdgeCounters[i];

			long[] inedgeIndexer = new long[inSectionEdgeCounter];
			byte[] inedgeValue = new byte[inSectionEdgeCounter * (sizeof + offsetWidth)];
			currentInSectionOffset = 0;
			int ic = 0;
			for (int k = 0; k < edges.length; k++) {
				int d_id = getSecond(edges[k]);
				if (forward(d_id) == i) {
					inedgeIndexer[ic] = pack(getSecond(edges[k]), getFirst(edges[k]));
					System.arraycopy(edgeValues, k * sizeof, inedgeValue, ic * (sizeof + offsetWidth), sizeof);
					System.arraycopy(intToByteArray(ic * cachelineSize), 0, inedgeValue, (ic * (sizeof + offsetWidth) + sizeof), offsetWidth);
					ic++;
				}
			}
			quickSort(inedgeIndexer, inedgeValue, sizeof + offsetWidth, 0, inSectionEdgeCounter - 1);

			/*
			 * for(int k=0;k<inSectionEdgeCounter;k++){
			 * sectioEDataWriter[i].writeLong(inedgeIndexer[k]); }
			 */

			// 从边构建邻接表
			for (int s = 0; s < edges.length; s++) {
				int from = getFirst(edges[s]);

				if (from != curvid) {
					// System.out.println("Processing vertex : " + curvid);
					int tmp = currentPos;
					int count = s - isstart;

					int outDegree = count;

					while (curvid == getFirst(inedgeIndexer[currentPos])) {
						count++;
						currentPos++;
					}

					int length = count * (4 + offsetWidth + sizeof) + 16 + offsetWidth;
					byte[] record = new byte[length];
					int curstart = 0;

					// 写入record的头信息 : 长度 顶点id 顶点value的offset
					System.arraycopy(intToByteArray(length), 0, record, curstart, 4);
					curstart += 4;
					// sectionAdjWriter[i].writeInt(length);
					System.arraycopy(intToByteArray(curvid), 0, record, curstart, 4);
					curstart += 4;
					// sectionAdjWriter[i].writeInt(curvid);
					System.arraycopy(longToByteArray(valueOffset), 0, record, curstart, offsetWidth);
					curstart += offsetWidth;
					// sectionAdjWriter[i].writeLong(valueOffset);

					assert (getFirst(vertices[valuePos]) == curvid);

					if (vertexValues != null) {
						System.arraycopy(vertexValues, valuePos * sizeofValue, cachelineTemplate, 0, sizeofValue);
						sectionVDataWriter[i].write(cachelineTemplate);
						valuePos++;
					} else {
						Arrays.fill(cachelineTemplate, (byte) 0);
						sectionVDataWriter[i].write(cachelineTemplate);
					}

					// 写入在本分区内的入边的信息 id weight offset

					System.arraycopy(intToByteArray(currentPos - tmp), 0, record, curstart, 4);
					curstart += 4;

					while (tmp < currentPos) {
						System.arraycopy(intToByteArray(getSecond(inedgeIndexer[tmp])), 0, record, curstart, 4);
						curstart += 4;

						// sectionAdjWriter[i].writeInt(getSecond(inedgeIndexer[currentPos]));
						// sectionAdjWriter[i].write(edgeValueTemplate);
						// sectionAdjWriter[i].writeLong(reverseOffset(byteArrayToLong(offsetTypeTemplate)));

						// System.arraycopy(inedgeValue, tmp * (sizeof +
						// offsetWidth), record, curstart, sizeof);
						// curstart += sizeof;
						// System.arraycopy(inedgeValue, (tmp * (sizeof +
						// offsetWidth) + sizeof), record, curstart,
						// offsetWidth);
						// curstart += offsetWidth;

						System.arraycopy(inedgeValue, tmp * (sizeof + offsetWidth), record, curstart, sizeof + offsetWidth);
						curstart += (sizeof + offsetWidth);
						tmp++;
					}
					// 写入record的出边信息 id 权重 边offset

					Set<Integer> workerIds = new HashSet<Integer>(count);

					System.arraycopy(intToByteArray(outDegree), 0, record, curstart, 4);
					curstart += 4;

					for (int p = isstart; p < s; p++) {

						// 写入邻接边id
						System.arraycopy(intToByteArray(getSecond(edges[p])), 0, record, curstart, 4);
						curstart += 4;
						// sectionAdjWriter[i].writeInt(Integer.reverseBytes(getSecond(edges[p])));
						// 写入邻接边的权值
						System.arraycopy(edgeValues, p * sizeof, record, curstart, sizeof);
						curstart += sizeof;
						// sectionAdjWriter[i].write(edgeValueTemplate);
						// 写入邻接边的offset
						if (forward(getSecond(edges[p])) == i) {
							System.arraycopy(intToByteArray(currentInSectionOffset), 0, record, curstart, offsetWidth);
							curstart += offsetWidth;
							// sectionAdjWriter[i].writeLong(currentInSectionOffset);
							currentInSectionOffset += cachelineSize;
						} else {
							int workerId = location(getSecond(edges[p]));
							workerIds.add(workerId);
							System.arraycopy(intToByteArray(lengthsOfWorkerMsgsPool[workerId]), 0, record, curstart, offsetWidth);
							curstart += offsetWidth;
							// sectionAdjWriter[i].writeLong(currentOutSectionOffset);
							lengthsOfWorkerMsgsPool[workerId] += msgSize;
						}
					}

					for (Integer ints : workerIds) {
						int offset2 = lengthsOfWorkerMsgsPool[ints];
						int offset1 = copyOfLengthsOfWorkerMsgsPool[ints];
						int totalMessageLen = offset2 - offset1;
						if (totalMessageLen != 0) {
							if (totalMessageLen % cachelineSize != 0)
								lengthsOfWorkerMsgsPool[ints] = offset2 + (64 - totalMessageLen % 64);
							copyOfLengthsOfWorkerMsgsPool[ints] = lengthsOfWorkerMsgsPool[ints];
						}
					}
					
					workerIds = null; // help to gc

					sectionAdjWriter[i].write(record);

					sectionFetchIndexWriter[i].writeInt(fetchIndex);

					fetchIndex += length;
					valueOffset += cachelineSize;

					curvid = from;
					isstart = s;
				}

			}

			sections[i] = new Section(i, graphFilename, valueOffset, currentInSectionOffset);
		}

		for (int i = 0; i < numSections; i++) {
			sectionAdjWriter[i].close();
			sectionVDataWriter[i].close();
			sectionFetchIndexWriter[i].close();
		}

	}

	// 解析邻接表行，转换为合适的格式 vid-offset id-offset id-offset id-offset
	// vid,val : id,val->id,val->id,val
	private void extractLine(String line) {
		String vertexPart = null;
		String edgePart = null;
		StringTokenizer st = new StringTokenizer(line, vertexToEdgeSeparate);
		StringTokenizer est = null;
		int tokens = st.countTokens();

		vertexPart = st.nextToken(); // id:value
		if (tokens == 2)
			edgePart = st.nextToken(); // id,value->id,value->id,value

		if (edgePart != null) {
			est = new StringTokenizer(edgePart, edgeToEdgeSeparate);
		}

		int vid = getFirst(vidToValue(vertexPart));

		if (est != null) {
			while (est.hasMoreTokens()) {
				String p = est.nextToken();
				eidToValue(p, vid);
			}
		}
	}

	private long eidToValue(String part, int from) {

		StringTokenizer st = new StringTokenizer(part, idToValueSeparate);
		int to = -1;
		String token = null;

		if (st.countTokens() == 2) {
			to = Integer.parseInt(st.nextToken());
			token = st.nextToken();
			if (edgeProcessor != null) {
				try {
					this.addEdge(from, to, token);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} else if (st.countTokens() == 1) {
			to = Integer.parseInt(st.nextToken());
			try {
				this.addEdge(from, to, null);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return pack(to, 0);
	}

	private long vidToValue(String part) {
		StringTokenizer st = new StringTokenizer(part, idToValueSeparate);
		int tokens = st.countTokens();
		int from = -1;
		String token = null;
		from = Integer.parseInt(st.nextToken());

		if (tokens == 2)
			token = st.nextToken();

		if (vertexProcessor != null) {
			VertexValueType value = vertexProcessor.receiveVertexValue(from, token);
			try {
				addVertexValue(from, value);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return pack(from, 0);

	}

	private int forward(int id) {
		return id / sectionSize;
	}

	private int location(int id) {
		return id % sectionSize % GraphConfig.numWorkers;
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

	public void addEdge(int from, int to, String token) throws IOException {
		int sectionid = forward(from);
		if (forward(to) == sectionid) {
			inSectionEdgeCounters[sectionid]++;
		} else {
			outSectionEdgeCounters[sectionid]++;
		}

		if (from == to) {
			if (vertexProcessor != null) {
				VertexValueType value = vertexProcessor.receiveVertexValue(from, token);
				try {
					addVertexValue(from, value);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		addToShovel(from, to, (edgeProcessor != null ? edgeProcessor.receiveEdge(from, to, token) : null));
	}

	private void addToShovel(int from, int to, EdgeValueType value) throws IOException {
		int section = forward(from);
		sectionShovelWriter[section].writeLong(pack(from, to));
		if (edgeValueTypeBytesToValueConverter != null) {
			edgeValueTypeBytesToValueConverter.setValue(edgeValueTemplate, value);
			sectionShovelWriter[section].write(edgeValueTemplate);
		}
	}

	public void addVertexValue(int from, VertexValueType value) throws IOException {
		int section = forward(from);
		sectionVertexValueShovelWriter[section].writeLong(pack(from, 0));
		verterxValueTypeBytesToValueConverter.setValue(vertexValueTemplate, value);
		sectionVertexValueShovelWriter[section].write(vertexValueTemplate);
	}

	private static Random random = new Random();

	private static int partition(long[] arr, byte[] values, int sizeof, int left, int right) {
		int i = left, j = right;
		long tmp;
		long pivot = arr[left + random.nextInt(right - left + 1)];
		byte[] valueTemplate = new byte[sizeof];

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
				if (values != null) {
					System.arraycopy(values, j * sizeof, valueTemplate, 0, sizeof);
					System.arraycopy(values, i * sizeof, values, j * sizeof, sizeof);
					System.arraycopy(valueTemplate, 0, values, i * sizeof, sizeof);
				}

				i++;
				j--;
			}

		}

		return i;
	}

	static void quickSort(long arr[], byte[] values, int sizeof, int left, int right) {
		if (left < right) {
			int index = partition(arr, values, sizeof, left, right);
			if (left < index - 1) {
				quickSort(arr, values, sizeof, left, index - 1);
			}
			if (index < right) {
				quickSort(arr, values, sizeof, index, right);
			}
		}
	}

	public static int byteArrayToInt(byte[] array) {
		return ((array[3] & 0xff) << 24) + ((array[2] & 0xff) << 16) + ((array[1] & 0xff) << 8) + (array[0] & 0xff);
	}

	public static byte[] intToByteArray(int val) {
		byte[] array = new byte[4];
		array[0] = (byte) ((val) & 0xff);
		array[1] = (byte) ((val >>> 8) & 0xff);
		array[2] = (byte) ((val >>> 16) & 0xff);
		array[3] = (byte) ((val >>> 24) & 0xff);
		return array;
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

	public static void main(String[] args) throws IOException {

		System.out.println(getFirst(11342L));
		System.out.println(getSecond(11342L));

		/*
		 * Graph graph = new Graph<Float, Float>("/home/doro/CG/google",
		 * "edgelist", null, new FloatConverter(), new VertexProcessor<Float>()
		 * {
		 * 
		 * @Override public Float receiveVertexValue(int _id, String token) {
		 * return (token == null ? 0.0f : Float.parseFloat(token)); }
		 * 
		 * }, new EdgeProcessor<Float>() {
		 * 
		 * @Override public Float receiveEdge(int from, int to, String token) {
		 * return (token == null ? 0.0f : Float.parseFloat(token)); } } null);
		 * 
		 * graph.XXXX();
		 */
	}

	public String getGraphFilename() {
		return graphFilename;
	}
}
