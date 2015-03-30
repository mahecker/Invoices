package com.fhm.fep2.InvoiceProcessing;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

public class InvoiceProcessing {

  // ******************************************** //
  // ************** Konfiguration *************** //
  // ******************************************** //
  // Zu simulierende Anzahl an Rechnungen
  public static final int INVOICE_COUNT = 100000;
  // Zu simulierende Anzahl an Rechnungspositionen je Rechnung
  public static final int POSITIONS_COUNT = 5;
  // Anzahl zu verwendender Rechenkerne
  public static final int CORES = 8;
  // Grenze zu verarbeitender Rechnungen je DataSupplyTask
  public static final int THRESHOLD_DATASUPPLY = 50000;
  // Grenze zu verarbeitender Rechnungen je BillingTask
  public static final int THRESHOLD_BILLING = getOptThreshold();
  // Konfiguration für MySQL-Anbindung
  public static final String USER = "mh";
  public static final String PWD = "mh";
  public static final String JDBC = "com.mysql.jdbc.Driver";
  public static final String URL = "jdbc:mysql://localhost:3306/test";
  // Rechnung in Datei speichern (./invoices)
  public static final boolean SAVE_INVOICE = false;
  // Prefix für Benutzerdefinierte Logs
  private static final Logger logger = getCustomConfiguredLogger("TEST");
  // ******************************************** //
  // ******************************************** //

  private static final long START_TIMESTAMP = System.currentTimeMillis();
  private static long latestTimestamp = START_TIMESTAMP;

  public static void main(String[] args) {
	// Initialization:
	logger.entry((Object[]) args);

	logger.info(" ####################################");
	logger.info("# INVOICE_COUNT: " + INVOICE_COUNT);
	logger.info("# POSITIONS_PER_INVOICE: " + POSITIONS_COUNT);
	logger.info("# THRESHOLD_DATASUPPLY: " + THRESHOLD_DATASUPPLY);
	logger.info("# THRESHOLD_BILLING: " + THRESHOLD_BILLING);
	logger.info("# CORES: " + CORES);
	logger.info("# SAVE_INVOICE: " + SAVE_INVOICE);
	logger.info(" ####################################\n");

	ForkJoinPool pool = new ForkJoinPool(CORES);
	List<BillingAction> billingTasks = new ArrayList<BillingAction>();
	logger.debug("Initialization:\t" + getDuration() + " ms.\n");

	// Computation:
	try {
	  RandomID randomID = new RandomID();
	  DataSupply dataSupply = new DataSupply(pool.invoke(randomID), THRESHOLD_DATASUPPLY);
	  List<DataSupplyTask> dataSupplyTasks = pool.invoke(dataSupply);
	  for (DataSupplyTask dataSupplyTask : dataSupplyTasks) {
		Billing billing = new Billing(dataSupplyTask.join(), THRESHOLD_BILLING);
		billingTasks.addAll(pool.invoke(billing));
	  }
	} finally {
	  if (!pool.isShutdown()) {
		for (BillingAction task : billingTasks) {
		  task.join();
		}
		logger.debug("Computation:\t" + getDuration() + " ms.");
		// Finalization:
		logger.info("Steal-Count:\t" + pool.getStealCount());
		pool.shutdown();
	  }
	}

	logger.debug("Finalization:\t" + getDuration() + " ms.");
	logger.info("Total:\t\t" + (System.currentTimeMillis() - START_TIMESTAMP) + " ms.\n");
	logger.exit();
  }
  
  private static int getOptThreshold() {
	int threshold;
	
	threshold = (int) Math.ceil((double) INVOICE_COUNT / (CORES * 2));
	
	return threshold;
  }

  private static Logger getCustomConfiguredLogger(String prefix) {
	Logger logger = null;

	String logFileName = prefix;
	logFileName += "_" + Integer.toString(INVOICE_COUNT);
	logFileName += "_" + Integer.toString(CORES);
	logFileName += "_" + Integer.toString(THRESHOLD_DATASUPPLY);
	logFileName += "_" + Integer.toString(THRESHOLD_BILLING);
	logFileName += "_" + Integer.toString(POSITIONS_COUNT);
	logFileName += "_" + Boolean.toString(SAVE_INVOICE);
	
	try {
	  File logFile = new File("./logs/" + logFileName + ".log");
	  if (logFile.exists()) {
		logFile.delete();
	  }
	} catch (Exception e) {
	  e.printStackTrace();
	}

	System.setProperty("logFilename", logFileName);
	LoggerContext ctx = (LoggerContext) LogManager.getContext(true);
	ctx.reconfigure();
	logger = LogManager.getLogger(InvoiceProcessing.class);

	return logger;
  }

  private static long getDuration() {
	logger.entry();
	long duration;
	long currentTimestamp = System.currentTimeMillis();

	duration = currentTimestamp - latestTimestamp;
	latestTimestamp = currentTimestamp;

	return logger.exit(duration);
  }
}
