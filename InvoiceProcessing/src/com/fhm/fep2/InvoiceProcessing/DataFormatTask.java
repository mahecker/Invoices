package com.fhm.fep2.InvoiceProcessing;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.concurrent.RecursiveTask;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class DataFormatTask extends RecursiveTask<Document[]> {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogManager.getLogger(DataFormatTask.class);
  private long startTimestamp;
  private long latestTimestamp;

  private ResultSet rs;
  private int count;

  public DataFormatTask(ResultSet rs, int count) {
	logger.entry(rs);
	this.rs = rs;
	this.count = count;
	logger.exit();
  }

  @Override
  protected Document[] compute() {
	// Initialization:
	logger.entry();
	startTimestamp = System.currentTimeMillis();
	latestTimestamp = startTimestamp;

	Document[] docs = new Document[this.count];

	try {
	  Document doc = null;
	  Element customer = null;
	  Element positions = null;
	  Element position = null;

	  boolean nextInvoice = true;
	  boolean hasNextInvoicePosition;
	  int currentInvoiceID = 0;
	  int nextInvoiceID = 0;
	  double sumListPrice = 0;
	  DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
	  DocumentBuilder builder = factory.newDocumentBuilder();
	  ResultSetMetaData rsmd = this.rs.getMetaData();
	  logger.debug("Initialization:\t" + getDuration() + " ms.");

	  // Computation:
	  hasNextInvoicePosition = rs.next();
	  while (hasNextInvoicePosition) {
		if (nextInvoice) {
		  // Neue Rechnung
		  doc = builder.newDocument();
		  this.count--;

		  // Kundeninformation einfügen
		  customer = doc.createElement("Customer");
		  for (int i = 1; i <= 11; i++) {
			// Spaltennamen in Position einfügen
			Element node = doc.createElement(rsmd.getColumnName(i));
			// Zelleninhalt in Position einfügen
			node.appendChild(doc.createTextNode(this.rs.getObject(i).toString()));
			customer.appendChild(node);
		  }
		  doc.appendChild(customer);

		  // Gruppenknoten für Rechnungspositionen einfügen
		  positions = doc.createElement("Positions");
		  customer.appendChild(positions);

		  // Aktuelle Rechnungsnummer merken
		  currentInvoiceID = this.rs.getInt("cs.cs_bill_cdemo_sk");
		}

		// Einzelne Rechnungsposition in Gruppenknoten einfügen
		position = doc.createElement("Position");
		for (int i = 12; i <= rsmd.getColumnCount(); i++) {
		  // Spaltennamen in Position einfügen
		  Element node = doc.createElement(rsmd.getColumnName(i));
		  // Zelleninhalt in Position einfügen
		  node.appendChild(doc.createTextNode(this.rs.getObject(i).toString()));
		  position.appendChild(node);
		}
		positions.appendChild(position);

		// Nettopreis der Rechnungsposition zu Gesamtpreis aggregieren
		sumListPrice += this.rs.getDouble("cs.cs_ext_list_price");

		hasNextInvoicePosition = this.rs.next();
		if (hasNextInvoicePosition) {
		  // Rechnungsnummer der nächsten Position
		  nextInvoiceID = this.rs.getInt("cs.cs_bill_cdemo_sk");
		}

		// Rechnung abschließen (Wenn neue Rechnungsnummer oder keine weitere
		// Rechnung vorhanden)
		if ((hasNextInvoicePosition && (currentInvoiceID != nextInvoiceID)) || !(hasNextInvoicePosition)) {
		  Element netTotal = doc.createElement("cs_net_sum");
		  Element salesTax = doc.createElement("cs_tax");
		  Element grossTotal = doc.createElement("cs_total_sum");

		  netTotal.appendChild(doc.createTextNode(new DecimalFormat("#.00").format(sumListPrice)));
		  salesTax.appendChild(doc.createTextNode(new DecimalFormat("#.00").format(sumListPrice * 0.19)));
		  grossTotal.appendChild(doc.createTextNode(new DecimalFormat("#.00").format(sumListPrice * 1.19)));

		  positions.appendChild(netTotal);
		  positions.appendChild(salesTax);
		  positions.appendChild(grossTotal);

		  customer.appendChild(positions);
		  docs[this.count] = doc;
		  sumListPrice = 0;

		  if (hasNextInvoicePosition) {
			nextInvoice = true;
		  }
		} else {
		  nextInvoice = false;
		}
	  }
	  logger.debug("Computation:\t" + getDuration() + " ms.");
	} catch (SQLException | ParserConfigurationException e) {
	  logger.catching(e);
	}

	logger.debug("Finalization:\t" + getDuration() + " ms.");
	logger.info("Total:\t\t" + (System.currentTimeMillis() - startTimestamp) + " ms.");
	return logger.exit(docs);
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
