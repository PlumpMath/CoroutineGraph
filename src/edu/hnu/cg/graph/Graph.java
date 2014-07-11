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
import java.util.Random;
import java.util.StringTokenizer;

import edu.hnu.cg.graph.datablocks.BytesToValueConverter;
import edu.hnu.cg.graph.datablocks.FloatConverter;
import edu.hnu.cg.graph.preprocessing.EdgeProcessor;
import edu.hnu.cg.graph.preprocessing.VertexProcessor;
import edu.hnu.cg.graph.userdefine.GraphConfig;
import edu.hnu.cg.util.BufferedDataInputStream;

public class Graph<VertexValueType extends Number, EdgeValueType extends Number> {

	public enum graphFormat {
		EDGELIST, ADJACENCY
	};

	private String graphFilename;
	private graphFormat format;
	private long numEdges = 0;
	private DataOutputStream eStream;
	private DataOutputStream vStream;
	private VertexProcessor<VertexValueType> vertexProcessor;
	private EdgeProcessor<EdgeValueType> edgeProcessor;
	private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;
	private BytesToValueConverter<VertexValueType> verterxValueTypeBytesToValueConverter;

	private byte[] vertexValueTemplate;
	private byte[] edgeValueTemplate;

	private static int sectionSize;
	private static int numVertices;
	private static int numSections;

	static {
		sectionSize = GraphConfig.sectionSize;
		numVertices = GraphConfig.numVertices;

		if (numVertices % sectionSize == 0)
			numSections = numVertices / sectionSize;
		else
			numSections = numVertices / sectionSize + 1;
	}

	private DataOutputStream[] sectionAdjWriter; // section的邻接表文件输出流
	private DataOutputStream[] sectionVDataWriter;// 存储本section内顶点的value
	private DataOutputStream[] sectioEDataWriter;// 存储本section内与边相关的消息的文件
	private DataOutputStream[] sectionShovelWriter;
	private DataOutputStream[] sectionFetchIndexWriter;
	private BufferedDataInputStream[] sectionShovelReader;
	private BufferedDataInputStream[] sectionVDataReader;
	private BufferedDataInputStream[] sectionAdjReader;
	
	private int[] inSectionEdgeCounters;
	private int[] outSectionEdgeCounters;

	public Graph(String filename, String _format) throws FileNotFoundException {
		graphFilename = filename;
		if (_format.equals("edgelist"))
			format = graphFormat.EDGELIST;
		else if (_format.equals("adjacency"))
			format = graphFormat.ADJACENCY;

		sectionAdjWriter = new DataOutputStream[numSections];
		sectionVDataWriter = new DataOutputStream[numSections];
		sectioEDataWriter = new DataOutputStream[numSections];
		sectionShovelWriter = new DataOutputStream[numSections];
		sectionFetchIndexWriter = new DataOutputStream[numSections];
		
		inSectionEdgeCounters = new int[numSections];
		outSectionEdgeCounters = new int[numSections];

		for (int i = 0; i < numSections; i++) {
			sectionAdjWriter[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Filename.getSectionFilename(graphFilename, i))));
			sectionVDataWriter[i] = new DataOutputStream(
					new BufferedOutputStream(new FileOutputStream(Filename.getSectionVertexDataFilename(graphFilename, i))));
			sectioEDataWriter[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Filename.getSectionEdgeDataFilename(graphFilename, i))));
			sectionShovelWriter[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Filename.getSectionShovelFilename(graphFilename, i))));
			sectionFetchIndexWriter[i] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Filename.getSectionFetchIndexFilename(
					graphFilename, i))));
		}

	}

	public Graph(String filename, String _format, BytesToValueConverter<EdgeValueType> _edgeValueTypeBytesToValueConverter,
			BytesToValueConverter<VertexValueType> _verterxValueTypeBytesToValueConverter, VertexProcessor<VertexValueType> _vertexProcessor,
			EdgeProcessor<EdgeValueType> _edgeProcessor) throws IOException {

		graphFilename = filename;
		if (_format.equals("edgelist"))
			format = graphFormat.EDGELIST;
		else if (_format.equals("adjacency"))
			format = graphFormat.ADJACENCY;
		vertexProcessor = _vertexProcessor;
		edgeProcessor = _edgeProcessor;
		edgeValueTypeBytesToValueConverter = _edgeValueTypeBytesToValueConverter;
		verterxValueTypeBytesToValueConverter = _verterxValueTypeBytesToValueConverter;
		eStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(Filename.getCompactEdgeFilename(filename))));
		// vStream = new DataOutputStream(new BufferedOutputStream(new
		// FileOutputStream(Filename.getVertexDataFilename(filename))));
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
	private static final long recordSep = 0xFFFFFFFFFFFFFFFFL;

	public void XXXX() throws IOException {
		BufferedReader bReader = new BufferedReader(new FileReader((new File(graphFilename))));

		String ln = null;
		long lnNum = 0;

		if (format == graphFormat.EDGELIST) {
			while ((ln = bReader.readLine()) != null) {
				lnNum++;
				if (lnNum % 1000000 == 0)
					System.out.println("Reading line: " + lnNum);

				String[] tokenStrings = ln.split("\\s");
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
				long[] res = extractLine(ln);
				/*
				 * int sectionid = forward(getFirst(res[0])); for (long l : res)
				 * sectionAdjWriter[sectionid].writeLong(l);
				 * sectionAdjWriter[sectionid].writeLong(recordSep);
				 */
			}

		}

		bReader.close();
		process();

	}

	public void process() throws IOException {

		int sizeof = (edgeValueTypeBytesToValueConverter != null ? edgeValueTypeBytesToValueConverter.sizeOf() : 0);
		int sizeofValue = (edgeValueTypeBytesToValueConverter != null ? edgeValueTypeBytesToValueConverter.sizeOf() : 4);

		for (int i = 0; i < numSections; i++) {
			File edgeFile = new File(Filename.getCompactEdgeFilename(graphFilename));

			long[] edges = new long[(int) edgeFile.length() / (8 + sizeof)];
			byte[] edgeValues = new byte[edges.length * sizeof];

			BufferedDataInputStream in = new BufferedDataInputStream(new FileInputStream(edgeFile));
			for (int k = 0; k < edges.length; k++) {
				long l = in.readLong();
				edges[k] = l;
				in.readFully(edgeValueTemplate);
				System.arraycopy(edgeValueTemplate, 0, edgeValues, i * sizeof, sizeof);
			}
			numEdges += edges.length;
			in.close();
			edgeFile.delete();

			quickSort(edges, edgeValues, sizeof, 0, edges.length - 1);

			int currentInSectionOffset = 0;
			int currentOutSectionOffset = 0;
			int valueOffset = 0;
			int curvid = 0;
			int isstart = 0;
			int inSectionEdgeCounter = 0;
			int outSectionEdgeCounter = 0;
			int fetchIndex = 0;

			long[] inedgeIndexer = new long[inSectionEdgeCounters[i]];
			currentInSectionOffset = 0;
			for (int k = 0; k < edges.length; k++) {
				int d_id = getSecond(edges[k]);
				if (forward(d_id) == i) {
					inedgeIndexer[inSectionEdgeCounter++] = pack(getSecond(edges[k]),getFirst(edges[k]));
				}
			}
			byte[] insectionOffsets = new byte[inSectionEdgeCounter * 4];
			for (int k = 0; k < inSectionEdgeCounter; k++) {
				//写入offset
				System.arraycopy(intToByteArray(k), 0, insectionOffsets, k * 4, 4);
				//写入边上的权值
				//see you tomorrow
				
			}

			quickSort(inedgeIndexer, insectionOffsets, 4, 0, inSectionEdgeCounter - 1);

			/*
			 * for(int k=0;k<inSectionEdgeCounter;k++){
			 * sectioEDataWriter[i].writeLong(inedgeIndexer[k]); }
			 */

			// 从边构建邻接表
			for (int s = 0, m = 0; s < edges.length; s++) {
				int from = getFirst(edges[s]);

				if (from != curvid) {
					int count = s - isstart;

					int length = count * (8 + sizeof) + 12;
					// 写入record的头信息 : 长度 顶点id 顶点value的offset
					sectionAdjWriter[i].writeInt(length);
					sectionAdjWriter[i].writeInt(curvid);
					sectionAdjWriter[i].write(valueOffset);

					// 写入record的出边信息 id 权重 边offset
					for (int p = isstart; p < s; p++) {

						// 写入邻接边id
						sectionAdjWriter[i].writeInt(Integer.reverseBytes(getSecond(edges[p])));
						// 写入邻接边的权值
						System.arraycopy(edgeValues, i * sizeof, edgeValueTemplate, 0, sizeof);
						sectionAdjWriter[i].write(edgeValueTemplate);
						// 写入邻接边的offset
						if (forward(getSecond(edges[p])) == i) {
							sectionAdjWriter[i].writeInt(currentInSectionOffset++);
							inSectionEdgeCounter++;
						} else {
							sectionAdjWriter[i].writeInt(currentOutSectionOffset++);
							outSectionEdgeCounter++;
						}
					}

					sectionFetchIndexWriter[i].writeInt(fetchIndex);

					fetchIndex += length;
					valueOffset += sizeofValue;

					curvid = from;
				}

			}

		}

	}

	// 解析邻接表行，转换为合适的格式 vid-offset id-offset id-offset id-offset
	private long[] extractLine(String line) {
		long[] vline = null;
		int len = 0;
		String vertexPart = null;
		String edgePart = null;
		StringTokenizer st = new StringTokenizer(line, vertexToEdgeSeparate);
		StringTokenizer est = null;
		int tokens = st.countTokens();

		vertexPart = st.nextToken(); // id:value
		if (tokens == 2)
			edgePart = st.nextToken(); // id:value->id:value->id:value

		if (vertexPart != null)
			len++;
		if (edgePart != null) {
			est = new StringTokenizer(edgePart, edgeToEdgeSeparate);
			len += est.countTokens();
		}

		vline = new long[len];
		vline[0] = vidToValue(vertexPart);

		int i = 1;
		if (est != null) {
			while (est.hasMoreTokens()) {
				String p = est.nextToken();
				vline[i++] = eidToValue(p, getFirst(vline[0]));
			}
		}
		return vline;
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
		
		int sectionid = forward(from);
		
		if(forward(to) == sectionid){
			inSectionEdgeCounters[sectionid]++;
		}else{
			outSectionEdgeCounters[sectionid]++;
		}
		
		return pack(to, 0);
	}

	private long vidToValue(String part) {
		StringTokenizer st = new StringTokenizer(part, idToValueSeparate);
		int from = -1;
		String token = null;
		if (st.countTokens() == 2) {
			from = Integer.parseInt(st.nextToken());
			token = st.nextToken();
			if (vertexProcessor != null) {
				try {
					this.addEdge(from, from, token);
				} catch (NumberFormatException | IOException e) {
					e.printStackTrace();
				}
			}

		} else if (st.countTokens() == 1) {
			from = Integer.parseInt(st.nextToken());
		}

		return pack(from, 0);

	}

	static int reverseOffset(int offset) {
		return (~offset) + 1;
	}

	static long pack(int a, int b) {
		return ((long) a << 32) + b;
	}

	static int getFirst(long e) {
		return (int) e >> 32;
	}

	public static int forward(int id) {
		return id / sectionSize;
	}

	static int getSecond(long e) {
		return (int) (e & 0x00000000ffffffffL);
	}

	public void addEdge(int from, int to, String token) throws IOException {
		if (from == to) {
			if (vertexProcessor != null && token != null) {
				VertexValueType value = vertexProcessor.receiveVertexValue(from, token);
				if (value != null) {
					addVertexValue(from, value);
				}
			}
			return;
		}
		addToShovel(from, to, (edgeProcessor != null ? edgeProcessor.receiveEdge(from, to, token) : null));
	}

	private void addToShovel(int from, int to, EdgeValueType value) throws IOException {
		int section = forward(from);
		sectionShovelWriter[section].writeLong(pack(from, to));
		if (edgeValueTypeBytesToValueConverter != null) {
			edgeValueTypeBytesToValueConverter.setValue(edgeValueTemplate, value);
		}
		sectionShovelWriter[section].write(edgeValueTemplate);
	}

	public void addVertexValue(int from, VertexValueType value) throws IOException {
		int section = forward(from);
		sectionVDataWriter[section].writeInt(from);
		verterxValueTypeBytesToValueConverter.setValue(vertexValueTemplate, value);
		sectionVDataWriter[section].write(vertexValueTemplate);
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

	/**
	 * int到byte[]
	 * 
	 * @param i
	 * @return
	 */
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

	public static void main(String[] args) throws IOException {

		/*
		 * Graph graph = new Graph<Float, Float>("/home/doro/CG/google",
		 * "edgelist", new FloatConverter(), new FloatConverter(), new
		 * VertexProcessor<Float>() {
		 * 
		 * @Override public Float receiveVertexValue(int _id, String token) {
		 * return (token == null ? 0.0f : Float.parseFloat(token)); }
		 * 
		 * }, new EdgeProcessor<Float>() {
		 * 
		 * @Override public Float receiveEdge(int from, int to, String token) {
		 * return (token == null ? 0.0f : Float.parseFloat(token)); } });
		 */

		System.out.println(reverseOffset(reverseOffset(100)));
	}
}
