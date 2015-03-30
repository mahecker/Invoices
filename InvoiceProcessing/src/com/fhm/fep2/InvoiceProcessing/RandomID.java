package com.fhm.fep2.InvoiceProcessing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.RecursiveTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RandomID extends RecursiveTask<String[]> {

  private static final long serialVersionUID = 1L;
  private static final Logger logger = LogManager.getLogger(RandomID.class);
  private long startTimestamp;
  private long latestTimestamp;

  @Override
  protected String[] compute() {
	// Initialization:
	logger.entry();
	startTimestamp = System.currentTimeMillis();
	latestTimestamp = startTimestamp;

	String[] ids = null;
	Connection conn = null;
	ResultSet rs = null;

	try {
	  Class.forName(InvoiceProcessing.JDBC);
	  conn = DriverManager.getConnection(InvoiceProcessing.URL, InvoiceProcessing.USER, InvoiceProcessing.PWD);
	  String sql = getQuery();
	  PreparedStatement pstmt = conn.prepareStatement(sql);
	  pstmt.setInt(1, InvoiceProcessing.POSITIONS_COUNT);
	  pstmt.setInt(2, InvoiceProcessing.INVOICE_COUNT);
	  logger.debug("Initialization:\t" + getDuration() + " ms.");

	  // Computation:
	  rs = pstmt.executeQuery();
	  int count = getRowCount(rs);

	  if (count > 0) {
		ids = new String[count];
		while (rs.next()) {
		  ids[(rs.getRow() - 1)] = rs.getString("cs.cs_bill_cdemo_sk");
		}
	  }
	  logger.debug("Computation:\t\t" + getDuration() + " ms.");
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

	logger.debug("Finalization:\t\t" + getDuration() + " ms.");
	logger.info("Total:\t\t" + (System.currentTimeMillis() - startTimestamp) + " ms.");
	return logger.exit(ids);
  }

  private String getQuery() {
	logger.entry();
	String sql;

	sql = "SELECT cs.cs_bill_cdemo_sk, COUNT(cs.cs_item_sk) AS count ";
	sql += "FROM (SELECT cs_bill_cdemo_sk, cs_item_sk FROM catalog_sales ";
	sql += "WHERE cs_bill_cdemo_sk <> 0 AND cs_bill_cdemo_sk IS NOT NULL AND cs_bill_cdemo_sk REGEXP ('^[0-9]') AND ";
	sql += "cs_bill_customer_sk <> 0 AND cs_bill_customer_sk IS NOT NULL AND cs_bill_customer_sk REGEXP ('^[0-9]') AND ";
	sql += "cs_bill_addr_sk <> 0 AND cs_bill_addr_sk IS NOT NULL AND cs_bill_addr_sk REGEXP ('^[0-9]') AND ";
	sql += "cs_item_sk <> 0 AND cs_item_sk IS NOT NULL AND cs_item_sk REGEXP ('^[0-9]') ";
	sql += "ORDER BY RAND()) AS cs ";
	sql += "GROUP BY cs.cs_bill_cdemo_sk ";
	sql += "HAVING count >= ? ";
	sql += "ORDER BY count LIMIT ?;";

	return logger.exit(sql);
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

	duration = currentTimestamp - latestTimestamp;
	latestTimestamp = currentTimestamp;

	return logger.exit(duration);
  }
}
