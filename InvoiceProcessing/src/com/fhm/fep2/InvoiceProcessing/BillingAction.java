package com.fhm.fep2.InvoiceProcessing;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.RecursiveAction;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.fop.apps.FOPException;
import org.apache.fop.apps.Fop;
import org.apache.fop.apps.FopFactory;
import org.apache.fop.apps.MimeConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;

public class BillingAction extends RecursiveAction {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogManager.getLogger(BillingAction.class);
  private long startTimestamp;
  private long latestTimestamp;

  private Document[] docs;
  private static final Templates cachedXSLT = getCachedXSLT();

  public BillingAction(Document[] docs) {
	logger.entry((Object[]) docs);
	this.docs = docs;
	logger.exit();
  }

  @Override
  protected void compute() {
	// Initialization:
	logger.entry();
	startTimestamp = System.currentTimeMillis();
	latestTimestamp = startTimestamp;

	OutputStream out = new NullOutputStream();
	FopFactory fopFactory = FopFactory.newInstance();
	logger.debug("Initialization:\t" + getDuration() + " ms.");

	// Computation:
	try {
	  for (Document doc : this.docs) {
		if (InvoiceProcessing.SAVE_INVOICE) {
		  out = new BufferedOutputStream(new FileOutputStream(new File("./invoices/sampleLive.pdf")));
		}
		Fop fop = fopFactory.newFop(MimeConstants.MIME_PDF, out);
		Transformer transformer = BillingAction.cachedXSLT.newTransformer();
		Source xml = new DOMSource(doc);
		Result result = new SAXResult(fop.getDefaultHandler());
		transformer.transform(xml, result);
	  }
	  logger.debug("Computation:\t" + getDuration() + " ms.");
	} catch (FOPException | TransformerException | FileNotFoundException e) {
	  logger.catching(e);
	} finally {
	  // Finalization:
	  try {
		out.close();
	  } catch (IOException e) {
		logger.catching(e);
	  }
	}

	logger.debug("Finalization:\t" + getDuration() + " ms.");
	logger.info("Total:\t\t" + (System.currentTimeMillis() - startTimestamp) + " ms.");
	logger.exit();
  }

  private static Templates getCachedXSLT() {
	logger.entry();
	Templates cachedXSLT = null;

	try {
	  Source XSLT = new StreamSource("./templates/InvoiceTemplate.xsl");
	  TransformerFactory factory = TransformerFactory.newInstance();
	  cachedXSLT = factory.newTemplates(XSLT);
	} catch (TransformerConfigurationException e) {
	  logger.catching(e);
	}

	return logger.exit(cachedXSLT);
  }

  private long getDuration() {
	logger.entry();
	long duration;
	long currentTimestamp = System.currentTimeMillis();

	duration = currentTimestamp - this.latestTimestamp;
	this.latestTimestamp = currentTimestamp;

	return logger.exit(duration);
  }
}
