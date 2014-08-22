package edu.hnu.cg.framework;

import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.Section;
import edu.hnu.cg.graph.datablocks.BytesToValueConverter;
import edu.hnu.cg.graph.datablocks.DataBlockManager;
import edu.hnu.cg.graph.datablocks.MsgBytesTovalueConverter;
import edu.hnu.cg.graph.userdefine.GraphConfig;

public class Manager<VertexValueType, EdgeValueType, MsgValueType> {
	
	private Graph graph;
	private Worker[] workers;
	private Section[] sections;
	
	private int superstep = 0;
	private int currentSection ;
	
	DataBlockManager dataBlockManager;
	
	@SuppressWarnings("rawtypes")
	private Handler handler;
	private MsgBytesTovalueConverter<MsgValueType> msgConverter;
	private BytesToValueConverter<VertexValueType> vertexValueTypeBytesToValueConverter;
	private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;
	
	public Section getCurrentSection(){
		return sections[currentSection];
	}
	
	public Manager(Graph graph){
		this.graph = graph;
        initWorkers();

    }
	
	
    //初始化workers
	public void initWorkers(){
        int numWorkers = GraphConfig.numWorkers;
        workers = new Worker[numWorkers];

        for(int i=0;i<numWorkers;i++){
            //constructor should implements
            workers[i] = new Worker<>(i, this, handler, msgConverter, vertexValueTypeBytesToValueConverter, edgeValueTypeBytesToValueConverter, dataBlockManager);
            //msg buffer
            if(Graph.lengthsOfWorkerMsgsPool[i]!=0){
	            dataBlockManager.allocateBlock(Graph.lengthsOfWorkerMsgsPool[i]);
            }
        }

	}

	
	public Worker getWorker(int i){
		return workers[i];
	}

}
