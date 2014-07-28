package edu.hnu.cg.graph;

//import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.MappedByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class Section {

	private int id;
	private int superstep;

	private String sectionFilename;
	private String vertexDataFilename;
	private String edgeDataFilename;
	private String fetchIndexFilename;

	private long valueSize;
	private long edataSize;
	

	private RandomAccessFile sectionFile;
	private MappedByteBuffer vertexInformationBuffer;
	private FileChannel vertexInfoFileChannel;

	private RandomAccessFile vertexDataFile;
	private MappedByteBuffer vertexDataBuffer;
	private FileChannel vertexDataFileChannel;

	private RandomAccessFile edgeDataFile;
	private MappedByteBuffer edgeDataBuffer;
	private FileChannel edgeDataFileChannel;

	private RandomAccessFile fetchIndexFile;
	private MappedByteBuffer indexBuffer;
	private FileChannel indexChannel;

	private volatile boolean loaded = false;
	private volatile boolean unloaded = false;

	public Section(int _id, String graphFiename, long vsize, long esize) throws IOException {
		id = _id;
		superstep = 0;
		valueSize = vsize;
		edataSize = esize;
		

		sectionFilename = Filename.getSectionFilename(graphFiename, id);
		vertexDataFilename = Filename.getSectionVertexDataFilename(graphFiename, id);
		edgeDataFilename = Filename.getSectionEdgeDataFilename(graphFiename, id, superstep);
		fetchIndexFilename = Filename.getSectionFetchIndexFilename(graphFiename, id);

		sectionFile = new RandomAccessFile(sectionFilename, "r");
		vertexDataFile = new RandomAccessFile(vertexDataFilename, "rw");
		edgeDataFile = new RandomAccessFile(edgeDataFilename, "rw");
		fetchIndexFile = new RandomAccessFile(fetchIndexFilename, "r");

	}

	public void setSuperstep(int step) {
		superstep = step;
	}

	// if this method is called , null pointer needed to be checked
	public MappedByteBuffer getVertexInformationBuffer() {
		return vertexInformationBuffer;
	}

	public MappedByteBuffer getVertexDataBuffer() {
		return vertexDataBuffer;
	}

	public MappedByteBuffer getEdgeDataBuffer() {
		return edgeDataBuffer;
	}
	public MappedByteBuffer getIndexBuffer() {
		return indexBuffer;
	}

	public void load() throws IOException {
		// 载入section信息文件
		if (sectionFile != null) {
			vertexInfoFileChannel = sectionFile.getChannel();
			vertexInformationBuffer = vertexInfoFileChannel.map(MapMode.READ_ONLY, 0, sectionFile.length());
			vertexInformationBuffer.position(0);
		} else {
			return;
		}

		if (fetchIndexFile != null) {
			indexChannel = fetchIndexFile.getChannel();
			indexBuffer = indexChannel.map(MapMode.READ_ONLY, 0, fetchIndexFile.length());
			indexBuffer.position(0);
		} else {
			return;
		}

		// 载入顶点value数据文件
		if (vertexDataFile != null) {
			if (vertexDataFile.length() == 0) {
				vertexDataFileChannel = vertexDataFile.getChannel();
				vertexDataBuffer = vertexDataFileChannel.map(MapMode.READ_ONLY, 0, valueSize);
				vertexDataBuffer.position(0);
			} else {
				vertexDataFileChannel = vertexDataFile.getChannel();
				vertexDataBuffer = vertexDataFileChannel.map(MapMode.READ_ONLY, 0, vertexDataFile.length());
				vertexDataBuffer.position(0);
			}

		} else {
			return;
		}

		if (edgeDataFile != null) {
			if (edgeDataFile.length() == 0) {
				edgeDataFileChannel = edgeDataFile.getChannel();
				edgeDataBuffer = edgeDataFileChannel.map(MapMode.READ_ONLY, 0, edataSize);
				edgeDataBuffer.position(0);
			} else {
				edgeDataFileChannel = edgeDataFile.getChannel();
				edgeDataBuffer = edgeDataFileChannel.map(MapMode.READ_ONLY, 0, edgeDataFile.length());
				edgeDataBuffer.position(0);
			}
		} else {
			return;
		}

		loaded = true;

	}

	public boolean isLoaded() {
		return loaded;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void clean(final MappedByteBuffer buffer) {
		AccessController.doPrivileged(new PrivilegedAction() {
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

	public void unload() throws IOException {

		// 关闭之前需要将内容更新到磁盘
		vertexDataBuffer.force();
		edgeDataBuffer.force();

		sectionFile.close();
		vertexDataFile.close();
		edgeDataFile.close();
		fetchIndexFile.close();

		vertexInfoFileChannel.close();
		vertexDataFileChannel.close();
		edgeDataFileChannel.close();
		indexChannel.close();

		clean(vertexInformationBuffer);
		clean(vertexDataBuffer);
		clean(edgeDataBuffer);
		clean(indexBuffer);

		unloaded = true;
		loaded = false;

	}

	public boolean isUnloaded() {
		return unloaded;
	}

	public static void main(String[] args) throws IOException {
		Section section = new Section(0, "/home/doro/CG/google", 473524992, 73094740);
		section.load();
		for(int k=0;k<100;k++){
			section.indexBuffer.get(k*10);
			System.out.println(section.indexBuffer.position());
		}
			

		int i = 0;
		while (section.vertexInformationBuffer.remaining() >0) {
			int len = section.vertexInformationBuffer.getInt();
			System.out.print("Recorde " + i + " : " + len + ",");
			int vid = section.vertexInformationBuffer.getInt();
			System.out.print(vid + ",");
			long valueOffset = section.vertexInformationBuffer.getInt();
			System.out.print(valueOffset);
			int k = 0;
			while (k < (len - 12) / 8) {
				int d_id = section.vertexInformationBuffer.getInt();
				System.out.print("->" + d_id);
				long offset = section.vertexInformationBuffer.getInt();
				System.out.print("," + offset);
				k++;
			}
			System.out.println();
			i++;
			
		}

		section.unload();
	}

}
