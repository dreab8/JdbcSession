package org.hibernate.test.resource2.jdbc.common;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import org.hibernate.resource2.jdbc.spi.JdbcConnectionAccess;

/**
 * @author Steve Ebersole
 */
public class JdbcConnectionAccessTestingImpl implements JdbcConnectionAccess {
	/**
	 * Singleton access
	 */
	public static final JdbcConnectionAccessTestingImpl INSTANCE = new JdbcConnectionAccessTestingImpl();

	private final Driver driver;
	private final String url;
	private final Properties connectionProperties;

	public JdbcConnectionAccessTestingImpl() {
		try {
			driver = (Driver) Class.forName( "org.h2.Driver" ).newInstance();

			url = "jdbc:h2:mem:db1";

			connectionProperties = new Properties();
			connectionProperties.setProperty( "username", "sa" );
			connectionProperties.setProperty( "password", "" );
		}
		catch (Exception e) {
			throw new RuntimeException( "Unable to configure JdbcConnectionAccessTestingImpl", e );
		}
	}

	@Override
	public Connection obtainConnection() throws SQLException {
		return driver.connect( url, connectionProperties );
	}

	@Override
	public void releaseConnection(Connection connection) throws SQLException {
		connection.close();
	}

	@Override
	public boolean supportsAggressiveRelease() {
		return false;
	}
}
