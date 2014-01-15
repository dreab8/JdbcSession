/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2014, Red Hat Inc. or third-party contributors as
 * indicated by the @author tags or express copyright attribution
 * statements applied by the authors.  All third-party contributions are
 * distributed under license by Red Hat Inc.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
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
