package edu.hnu.cg.framework;

import edu.hnu.cg.graph.Graph;
import edu.hnu.cg.graph.Section;

public class Manager {
	
	private Graph graph;
	private Worker[] workers;
	private Section[] sections;
	private int superstep = 0;
	private int currentSection ;
	
	public Section getCurrentSection(){
		return sections[currentSection];
	}
	
	
	public Manager(Graph graph){
		this.graph = graph;
	}
	
	public void initWorkers(){
	}

}
