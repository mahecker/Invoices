package com.fhm.fep2.InvoiceProcessing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataSupply extends RecursiveTask<List<DataSupplyTask>> {
  
  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogManager.getLogger(DataSupply.class);
  private long startTimestamp;
  private long latestTimestamp;
  
  private String[] ids;
  private int threshold;
  
  public DataSupply(String[] ids, int threshold) {
	logger.entry((Object[]) ids, threshold);
	this.ids = ids;
	this.threshold = threshold;
	logger.exit();
  }
  
  @Override
  protected List<DataSupplyTask> compute() {
	// Initialization:
	logger.entry();
	startTimestamp = System.currentTimeMillis();
	latestTimestamp = startTimestamp;
	
	List<DataSupplyTask> tasks = new ArrayList<DataSupplyTask>();
	int count = this.ids.length;
	logger.debug("Initialization:\t" + getDuration() + " ms.");

	// Computation:
	if (count <= this.threshold) {
	  DataSupplyTask task = new DataSupplyTask(this.ids);
	  task.fork();
	  tasks.add(task);
	} else {
	  while (count > this.threshold) {
		count = this.ids.length - this.threshold;
		String[] partial = new String[this.threshold];
		String[] rest = new String[count];

		System.arraycopy(this.ids, 0, partial, 0, this.threshold);
		System.arraycopy(this.ids, this.threshold, rest, 0, count);
		this.ids = rest;

		DataSupplyTask task = new DataSupplyTask(partial);
		task.fork();
		tasks.add(task);
	  }
	}
	logger.debug("Computation:\t" + getDuration() + " ms.");
	
	logger.debug("Finalization:\t" + getDuration() + " ms.");
	logger.info("Total:\t\t" + (System.currentTimeMillis() - startTimestamp) + " ms.");
	return logger.exit(tasks);
  }
  
  private long getDuration() {
	logger.entry();
	long duration;
	long currentTimestamp = System.currentTimeMillis();

	duration = currentTimestamp - latestTimestamp;
	latestTimestamp = currentTimestamp;

	return logger.exit(duration);
  }
}
