package edu.hnu.cg.graph.preprocessing;

public interface VertexProcessor<ValueType> {
	ValueType receiveVertexValue(int _id , String token);

}
