package com.fhm.fep2.InvoiceProcessing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.RecursiveTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;

public class DataSupplyTask extends RecursiveTask<Document[]> {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogManager.getLogger(DataSupplyTask.class);
  private long startTimestamp;
  private long latestTimestamp;

  private String[] ids;

  public DataSupplyTask(String[] ids) {
	logger.entry((Object[]) ids);
	this.ids = ids;
	logger.exit();
  }

  @Override
  protected Document[] compute() {
	// Initialization:
	logger.entry();
	startTimestamp = System.currentTimeMillis();
	latestTimestamp = startTimestamp;

	Document[] docs = null;
	Connection conn = null;
	ResultSet rs = null;

	try {
	  Class.forName(InvoiceProcessing.JDBC);
	  conn = DriverManager.getConnection(InvoiceProcessing.URL, InvoiceProcessing.USER, InvoiceProcessing.PWD);
	  String sql = getQuery();
	  PreparedStatement pstmt = conn.prepareStatement(sql);
	  pstmt.setInt(1, InvoiceProcessing.POSITIONS_COUNT);
	  logger.debug("Initialization:\t" + getDuration() + " ms.");

	  // Computation:
	  rs = pstmt.executeQuery();
	  int count = getRowCount(rs);
	  logger.debug("Inv.-Positions:\t" + count + " pcs.");
	  if (count > 0) {
		DataFormatTask dataFormat = new DataFormatTask(rs, this.ids.length);
		dataFormat.fork();
		docs = dataFormat.join();
	  }
	  logger.debug("Computation:\t" + getDuration() + " ms.");
	} catch (SQLException | ClassNotFoundException e) {
	  logger.catching(e);
	} finally {
	  // Finalization:
	  try {
		if (!rs.isClosed()) {
		  rs.close();
		}
		if (!conn.isClosed()) {
		  conn.close();
		}
	  } catch (Exception e) {
		logger.catching(e);
	  }
	}

	logger.debug("Finalization:\t" + getDuration() + " ms.");
	logger.info("Total:\t\t" + (System.currentTimeMillis() - startTimestamp) + " ms.");
	return logger.exit(docs);
  }

  private String getQuery() {
	logger.entry();
	String sql;

	sql = "SELECT c.c_customer_sk, c.c_first_name, c.c_last_name, ";
	sql += "ca.ca_street_name, ca.ca_street_type, ca.ca_street_number, ca.ca_zip, ca.ca_county, ca.ca_state, ca.ca_country, ";
	sql += "cs.cs_bill_cdemo_sk, ";
	sql += "i.i_item_sk, i.i_category, i.i_class, i.i_brand, i.i_product_name, i.i_units, ";
	sql += "cs.cs_quantity, cs.cs_list_price, cs.cs_ext_list_price ";
	sql += "FROM (";
	sql += "SELECT cs_bill_cdemo_sk, cs_quantity, cs_list_price, cs_ext_list_price, cs_bill_customer_sk, cs_bill_addr_sk, cs_item_sk, ";
	sql += "@rn := IF(@prev = cs_bill_cdemo_sk, @rn + 1, 1) AS rn, @prev := cs_bill_cdemo_sk ";
	sql += "FROM catalog_sales ";
	sql += "JOIN (SELECT @prev := NULL, @rn := 0) AS vars ";
	sql += "WHERE cs_bill_cdemo_sk <> 0 AND cs_bill_cdemo_sk IS NOT NULL AND cs_bill_cdemo_sk REGEXP ('^[0-9]') AND ";
	sql += "cs_bill_customer_sk <> 0 AND cs_bill_customer_sk IS NOT NULL AND cs_bill_customer_sk REGEXP ('^[0-9]') AND ";
	sql += "cs_bill_addr_sk <> 0 AND cs_bill_addr_sk IS NOT NULL AND cs_bill_addr_sk REGEXP ('^[0-9]') AND ";
	sql += "cs_item_sk <> 0 AND cs_item_sk IS NOT NULL AND cs_item_sk REGEXP ('^[0-9]') ";
	sql += "ORDER BY cs_bill_cdemo_sk ";
	sql += ") AS cs JOIN customer AS c ON c.c_customer_sk = cs.cs_bill_customer_sk ";
	sql += "JOIN customer_address AS ca ON ca.ca_address_sk = cs.cs_bill_addr_sk ";
	sql += "JOIN item AS i ON i.i_item_sk = cs.cs_item_sk ";
	sql += "WHERE cs.cs_bill_cdemo_sk IN (" + getIdsFromStringArray() + ") AND ";
	sql += "cs.rn <= ? ";
	sql += "ORDER BY cs.cs_bill_cdemo_sk, c.c_customer_sk, cs.rn;";

	return logger.exit(sql);
  }

  // Notwendig, da MySQL kein "setArray" in prepared Statements unterstützt
  private String getIdsFromStringArray() {
	logger.entry();
	String ids = new String();

	for (int i = 0; i < this.ids.length; i++) {
	  if (i == 0) {
		ids = this.ids[i];
	  } else {
		ids += "," + this.ids[i];
	  }
	}

	return logger.exit(ids);
  }

  private int getRowCount(ResultSet rs) throws SQLException {
	logger.entry(rs);
	int count;

	rs.last();
	count = rs.getRow();
	rs.beforeFirst();

	return logger.exit(count);
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
