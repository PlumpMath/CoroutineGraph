package edu.hnu.cg.framework;

import java.io.IOException;

import kilim.Pausable;
import kilim.Task;
import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.Section;
import edu.hnu.cg.graph.datablocks.BytesToValueConverter;
import edu.hnu.cg.graph.datablocks.DataBlockManager;
import edu.hnu.cg.graph.datablocks.MsgBytesTovalueConverter;
import edu.hnu.cg.graph.userdefine.GraphConfig;

public class Manager<VertexValueType extends Number, EdgeValueType extends Number, MsgValueType> extends Task {

	private Graph<VertexValueType, EdgeValueType, MsgValueType> graph;
	private Worker<VertexValueType, EdgeValueType, MsgValueType>[] workers;
	private Section[] sections;

	private int superstep = 0;
	private int currentSection;

	DataBlockManager dataBlockManager;

	@SuppressWarnings("rawtypes")
	private Handler handler;
	private MsgBytesTovalueConverter<MsgValueType> msgConverter;
	private BytesToValueConverter<VertexValueType> vertexValueTypeBytesToValueConverter;
	private BytesToValueConverter<EdgeValueType> edgeValueTypeBytesToValueConverter;

	public Section getCurrentSection() {
		return sections[currentSection];
	}

	public Manager(Graph<VertexValueType, EdgeValueType, MsgValueType> graph) {
		this.graph = graph;
		int numSections = Graph.numSections;
		sections = graph.sections;
		initWorkers();
	}

	// 初始化workers
	@SuppressWarnings("unchecked")
	public void initWorkers() {
		int numWorkers = GraphConfig.numWorkers;
		workers = new Worker[numWorkers];

		for (int i = 0; i < numWorkers; i++) {
			// constructor should implements
			int buffLen = Graph.lengthsOfWorkerMsgsPool[i];
			workers[i] = new Worker<>(i, this, handler, msgConverter, vertexValueTypeBytesToValueConverter, edgeValueTypeBytesToValueConverter,
					dataBlockManager, buffLen, superstep);
			// msg buffer
			if (Graph.lengthsOfWorkerMsgsPool[i] != 0) {
				dataBlockManager.allocateBlock(buffLen);
			}
		}

	}

	public Worker<VertexValueType, EdgeValueType, MsgValueType> getWorker(int i) {
		return workers[i];
	}

	private boolean running = true;

	@Override
	public void execute() throws Pausable {
		
		startWorkers();
		
		int counter = 0;
		
		while (running) {
			//载入分区
			load(counter);
			//发送消息,告知worker开始工作
			
			
			//等待分区完成
			waitPartitionFinishedWithAtomic();
			
			//卸载分区载入下一块分区

		}

	}

	private void waitPartitionFinishedWithAtomic(){
		
		int limit = currentPartition.size();
		
		while(counterForPartition.get()!= limit){
//			System.out.println(counterForPartition.get());
			
		}
		counterForPartition.set(0);
		
	}

	private void load(int counter) {
		try {
			sections[counter].load(superstep);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void startWorkers() {
		
		for(Worker<VertexValueType,EdgeValueType,MsgValueType> t : workers){
			t.start();
		}
	}

}
