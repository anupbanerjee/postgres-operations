package com.anup.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresBulkDataGenerator {

	private String postgresIpaddress;

	private String postgresPort;

	private String postgresUserName;

	private String postgresPassword;

	private static final Logger logger = LoggerFactory.getLogger(PostgresBulkDataGenerator.class);

	/**
	 * @return the postgresIpaddress
	 */
	public String getPostgresIpaddress() {
		return postgresIpaddress;
	}

	/**
	 * @param postgresIpaddress
	 *            the postgresIpaddress to set
	 */
	@Option(name = "-h", required = true, aliases = "--host", usage = "Specify the Postgres Host Names - example 98.185.200.164")
	public void setPostgresIpaddress(String postgresIpaddress) {
		this.postgresIpaddress = postgresIpaddress;
	}

	/**
	 * @return the postgresPort
	 */
	public String getPostgresPort() {
		return postgresPort;
	}

	/**
	 * @param postgresPort
	 *            the postgresPort to set
	 */
	@Option(name = "-p", required = true, aliases = "--port", usage = "Specify the Postgres postgres port - default 5432")
	public void setPostgresPort(String postgresPort) {
		this.postgresPort = postgresPort;
	}

	/**
	 * @return the postgresUserName
	 */
	public String getPostgresUserName() {
		return postgresUserName;
	}

	/**
	 * @param postgresUserName
	 *            the postgresUserName to set
	 */
	@Option(name = "-u", required = true, aliases = "--username", usage = "Specify the Postgres user name")
	public void setPostgresUserName(String postgresUserName) {
		this.postgresUserName = postgresUserName;
	}

	/**
	 * @return the postgresPassword
	 */
	public String getPostgresPassword() {
		return postgresPassword;
	}

	/**
	 * @param postgresPassword
	 *            the postgresPassword to set
	 */
	@Option(name = "-pw", required = true, aliases = "--password", usage = "Specify the Postgres password")
	public void setPostgresPassword(String postgresPassword) {
		this.postgresPassword = postgresPassword;
	}

	/**
	 * Runs main and loads data for 100 CMTS for 255 IP Address 100 Times
	 * 
	 * @param args
	 * @throws Exception
	 * 
	 * ------------------------------------------------
	 * 
	 * create table ipdrdata (
	 * cmts_id int,
	 * ipaddress varchar,
	 * timeinmillis bigint,
	 * upstream bigint,
	 * downstream bigint,
	 * primary key(cmts_id, ipaddress, timeinmillis)
     * )
	 ------------------------------------------------
	 * Program Argument
	 * -h 127.0.01 -p 5432 -u postgres -pw postgres
	 * ----------------------------------------------
	 *  
	 */
	private void doMain(String[] args) throws Exception {
		
		CmdLineParser parser = new CmdLineParser(this);
		parser.parseArgument(args);
		logger.info(":: Initializing Bulk Load !!!");

		Class.forName("org.postgresql.Driver");
		String url = "jdbc:postgresql://"+getPostgresIpaddress()+":"+getPostgresPort()+"/cmts_ipdr_data";
		Connection conn = DriverManager.getConnection(url,getPostgresUserName() ,getPostgresPassword());

		PreparedStatement pstmt = conn.prepareStatement(
				"insert into ipdrdata(cmts_id,ipaddress,timeinmillis, upstream,downstream) values(?,?,?,?,?)");

		for (int p = 1; p <= 100; p++) {
			for (int i = 1; i <= 100; i++) {
				pstmt.setInt(1, i);
				conn.setAutoCommit(false);
				for (int j = 1; j <= 255; j++) {
					pstmt.setString(2, "10.22." + i + "." + j);
					pstmt.setLong(3, System.currentTimeMillis());
					pstmt.setLong(4, j * 25);
					pstmt.setLong(5, j * 50);
					pstmt.addBatch();
				}
				int counts[] = pstmt.executeBatch();
				conn.commit();
				logger.debug("Round : {} for CMTS : {} Number of Records : {} ", p, i, counts.length);
			}
			logger.debug("Completed Round : {}", p);
		}
		pstmt.close();
		conn.close();
	}

	public static void main(String args[]) throws Exception {
		
		try {
			new PostgresBulkDataGenerator().doMain(args);
		} catch (Throwable t) {
			t.printStackTrace();
		} finally {
			System.exit(0);
		}
	}
}