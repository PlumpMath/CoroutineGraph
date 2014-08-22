package edu.hnu.cg.graph;

public class Filename {

	public static String getSectionFilename(String filname, int sectionid) {
		return filname + "." + sectionid + ".section";
	}

	// 获得某个section 所对应的顶点值的文件
	// 该文件内的值按照顶点从小到达的顺序存储value
	public static String getSectionVertexDataFilename(String filename, int sectionid) {
		return filename + "." + sectionid + ".vdata";
	}

	public static String getSectionEdgeDataFilename(String filename, int sectionid, int superstep) {
		return filename + "." + sectionid + "-" + superstep + ".edata";
	}

	public static String getSectionShovelFilename(String filename, int sectionid) {
		return filename + "." + sectionid + ".shovel";
	}

	public static String getSectionVertexShovelFilename(String filename, int sectionid) {
		return filename + "." + sectionid + ".vertex" + ".shovel";
	}

	public static String getSectionFetchIndexFilename(String filename, int sectionid) {
		return filename + "." + sectionid + ".index";
	}

	public static String getCompactEdgeFilename(String filename) {
		return filename + ".e.tiny";
	}

	// 获得某个section所对应的边上的值或者消息
	// 值的存储按照该section所有入边的from 从小到大的顺序存储， 如果from相同 则按照 to 从小到大的顺序存储
	// 计算出每个边的value在文件中的偏移值
	public static String getSectionEdgeDataFilename(String sectionFilename, int superstep,
			String sectionid) {
		return sectionFilename + "." + superstep + "." + sectionid + ".edata";
	}

	public static String getOutSectionEdgeDataFilename(String filename, int superstep) {
		return filename + "." + superstep + ".outdata";
	}
}
