package org.hibernate.resource.jdbc.spi;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides centralized access to JDBC connections, hiding the complexity of accounting for any context specific
 * info (multi-tenancy e.g.).
 *<p/>
 * todo : NOTE that this is a moving of org.hibernate.engine.jdbc.spi.JdbcConnectionAccess
 *
 * @author Steve Ebersole
 */
public interface JdbcConnectionAccess extends Serializable {
	/**
	 * Obtain a JDBC connection
	 *
	 * @return The obtained connection
	 *
	 * @throws java.sql.SQLException Indicates a problem getting the connection
	 */
	public Connection obtainConnection() throws SQLException;

	/**
	 * Release a previously obtained connection
	 *
	 * @param connection The connection to release
	 *
	 * @throws java.sql.SQLException Indicates a problem releasing the connection
	 */
	public void releaseConnection(Connection connection) throws SQLException;
}
