package com.fhm.fep2.InvoiceProcessing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;

public class Billing extends RecursiveTask<List<BillingAction>> {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogManager.getLogger(Billing.class);
  private long startTimestamp;
  private long latestTimestamp;

  private Document[] docs;
  private int threshold;

  public Billing(Document[] docs, int threshold) {
	logger.entry((Object[]) docs, threshold);
	this.docs = docs;
	this.threshold = threshold;
	logger.exit();
  }

  @Override
  protected List<BillingAction> compute() {
	// Initialization:
	logger.entry();
	startTimestamp = System.currentTimeMillis();
	latestTimestamp = startTimestamp;

	List<BillingAction> tasks = new ArrayList<BillingAction>();
	int count = this.docs.length;

	logger.debug("Initialization:\t" + getDuration() + " ms.");

	// Computation:
	if (count <= this.threshold) {
	  BillingAction task = new BillingAction(this.docs);
	  task.fork();
	  tasks.add(task);
	} else {
	  while (count > this.threshold) {
		count = this.docs.length - this.threshold;
		Document[] partial = new Document[this.threshold];
		Document[] rest = new Document[count];

		System.arraycopy(this.docs, 0, partial, 0, this.threshold);
		System.arraycopy(this.docs, this.threshold, rest, 0, count);
		this.docs = rest;

		BillingAction task = new BillingAction(partial);
		task.fork();
		tasks.add(task);
	  }
	}
	logger.debug("Computation:\t\t" + getDuration() + " ms.");

	logger.debug("Finalization:\t\t" + getDuration() + " ms.");
	logger.info("Total:\t\t\t" + (System.currentTimeMillis() - startTimestamp) + " ms.");
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
