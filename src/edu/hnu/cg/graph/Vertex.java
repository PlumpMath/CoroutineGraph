package edu.hnu.cg.graph;

import java.awt.GraphicsConfigTemplate;

import sun.misc.Unsafe;
import edu.hnu.cg.graph.datablocks.BytesToValueConverter;
import edu.hnu.cg.graph.datablocks.DataBlockManager;
import edu.hnu.cg.graph.datablocks.Pointer;
import edu.hnu.cg.graph.datablocks.VertexDegree;
import edu.hnu.cg.graph.userdefine.GraphConfig;
import edu.hnu.cg.util.Tools;

@SuppressWarnings("restriction")
public class Vertex<VertexValue, EdgeValue> {

	public static final Unsafe unsafe = Tools.getUnsafe();
	@SuppressWarnings("unused")
	private static final long vertex_id_offset;
	@SuppressWarnings("unused")
	private static final long value_offset;
	@SuppressWarnings("unused")
	private static final long superstep_offset;
	private static final long out_degree_offset;
	private static final long in_degree_offset;

	static {
		try {
			vertex_id_offset = unsafe.objectFieldOffset(Vertex.class.getDeclaredField("vertex_id_offset"));
			value_offset = unsafe.objectFieldOffset(Vertex.class.getDeclaredField("value_offset"));
			superstep_offset = unsafe.objectFieldOffset(Vertex.class.getDeclaredField("superstep_offset"));
			out_degree_offset = unsafe.objectFieldOffset(Vertex.class.getDeclaredField("out_degree_offset"));
			in_degree_offset = unsafe.objectFieldOffset(Vertex.class.getDeclaredField("in_degree_offset"));
		} catch (Exception e) {
			throw new Error(e);
		}
	}

	public static DataBlockManager blockManager;
	public static BytesToValueConverter vertexValueConverter;
	public static BytesToValueConverter edgeValueConverter;
	public static boolean disableInedges = false;
	public static boolean disableOutedges = false;

	private int id;
	private Pointer vertexPtr;
	private volatile int out_degree;
	private int[] out_edges;
	private volatile int in_degree;
	private int[] in_edges;

	public Vertex(int _id, VertexDegree degree) {
		id = _id;

		if (degree != null) {
			in_degree = degree.inDegree;
			out_degree = degree.outDegree;
			if (!disableInedges) {
				in_edges = new int[degree.inDegree * (edgeValueConverter != null ? 3 : 1)];
			}
			if (!disableOutedges) {
				out_edges = new int[degree.outDegree * (edgeValueConverter != null ? 3 : 1)];
			}
		}
	}
	
	public Vertex(String adj){
		String[] vertexToEdgeStrings = adj.split(GraphConfig.vertexToEdgesSep);
		if(vertexToEdgeStrings.length == 2){
			int _id = -1;
			
			try{
				_id = Integer.parseInt(vertexToEdgeStrings[0]);
			}catch(NumberFormatException e){
				e.printStackTrace();
			}
			
			if(_id < 0){
				throw new Illegal
			}
		}
	}

	public void setId(int _id) {
		id = _id;
	}

	public void setVertexPtr(Pointer _vertexPtr) {
		vertexPtr = _vertexPtr;
	}

	public void setValue(VertexValue val) {
		blockManager.writeValue(vertexPtr, vertexValueConverter, val);
	}

	public int getId() {
		return id;
	}

	public int getOutDegree() {
		return out_degree;
	}

	public int getInDegree() {
		return in_degree;
	}

	public int getNumEdges() {
		return in_degree + out_degree;
	}

	@SuppressWarnings("unchecked")
	public VertexValue getValue() {
		return blockManager.dereference(vertexPtr, (BytesToValueConverter<VertexValue>) vertexValueConverter);
	}

	@SuppressWarnings("unchecked")
	public EdgeValue getOutEdgeValueByIndex(int i) {
		if (!disableOutedges) {
			if (out_edges != null) {
				int idx = i * 3;
				return blockManager.dereference(new Pointer(out_edges[idx], out_edges[idx + 1]), (BytesToValueConverter<EdgeValue>) edgeValueConverter);
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public EdgeValue getOutEdgeValueById(int _id) {
		if (!disableOutedges) {
			if (out_edges != null) {
				for (int i = 0; i < out_edges.length; i += 3) {
					if (out_edges[i + 2] == _id)
						return blockManager.dereference(new Pointer(out_edges[i], out_edges[i + 1]), (BytesToValueConverter<EdgeValue>) edgeValueConverter);
				}
			}
		}

		return null;

	}

	public EdgeValue getInEdgeValueByIndex(int i) {
		if (!disableInedges) {
			if (in_edges != null) {
				int idx = i * 3;
				return blockManager.dereference(new Pointer(in_edges[idx], in_edges[idx + 1]), (BytesToValueConverter<EdgeValue>) edgeValueConverter);
			}
		}
		return null;
	}

	public EdgeValue getInEdgeValueById(int _id) {

		if (!disableInedges) {
			if (in_edges != null) {
				for (int i = 0; i < in_edges.length; i += 3) {
					if (in_edges[i + 2] == _id)
						return blockManager.dereference(new Pointer(in_edges[i], in_edges[i + 1]), (BytesToValueConverter<EdgeValue>) edgeValueConverter);
				}
			}
		}

		return null;
	}

	public void addInedge(int chunkId, int offset, int _id) {
		int tmpInedges;
		for (;;) {
			int current = in_degree;
			tmpInedges = current + 1;
			if (unsafe.compareAndSwapInt(this, in_degree_offset, current, tmpInedges))
				break;
		}

		tmpInedges--;
		if (edgeValueConverter != null) {
			int idx = tmpInedges * 3;
			in_edges[idx] = chunkId;
			in_edges[idx + 1] = offset;
			in_edges[idx + 2] = _id;
		} else {
			if (in_edges != null) {
				in_edges[tmpInedges] = _id;
			}
		}
	}

	public void addOutEdge(int chunkId, int offset, int _id) {
		int tmpOutedges;
		for (;;) {
			int current = out_degree;
			tmpOutedges = current + 1;
			if (unsafe.compareAndSwapInt(this, out_degree_offset, current, tmpOutedges)) {
				break;
			}
		}
		tmpOutedges--;
		if (edgeValueConverter != null) {
			int idx = tmpOutedges * 3;
			out_edges[idx] = chunkId;
			out_edges[idx + 1] = offset;
			out_edges[idx + 2] = _id;

		} else {
			if (out_edges != null) {
				out_edges[tmpOutedges] = _id;
			}
		}
	}

}
