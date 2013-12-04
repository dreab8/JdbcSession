/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2013, Red Hat Inc. or third-party contributors as
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
package org.hibernate.resource2.jdbc.spi;

import java.sql.Connection;

import org.hibernate.ConnectionReleaseMode;

/**
 * @author Steve Ebersole
 */
public interface LogicalConnection {

	public ConnectionReleaseMode interpret(ConnectionReleaseMode mode);

	public PhysicalJdbcTransaction getPhysicalJdbcTransaction();

	/**
	 * Is this logical connection instance "physically" connected.  Meaning
	 * do we currently internally have a cached connection.
	 *
	 * @return True if physically connected; false otherwise.
	 */
	public boolean isPhysicallyConnected();

	/**
	 * Retrieves the connection currently "logically" managed by this LogicalConnectionImpl.
	 * <p/>
	 * Note, that we may need to obtain a connection to return here if a
	 * connection has either not yet been obtained (non-UserSuppliedConnectionProvider)
	 * or has previously been aggressively released.
	 *
	 * @return The current Connection.
	 */
	public Connection getConnection();

	/**
	 * Callback to release Connections, generally as part of ConnectionReleaseMode
	 */
	public void releaseConnection();

	/**
	 * Release the underlying connection and clean up any other resources associated
	 * with this logical connection.
	 * <p/>
	 * This leaves the logical connection in a "no longer usable" state.
	 *
	 * @return The application-supplied connection, or {@code null} if Hibernate was managing connection.
	 */
	public Connection close();

	/**
	 * Manually disconnect the underlying JDBC Connection.  The assumption here
	 * is that the manager will be reconnected at a later point in time.
	 *
	 * @return The connection maintained here at time of disconnect.  Null if
	 * there was no connection cached internally.
	 */
	public Connection manualDisconnect();

	/**
	 * Manually reconnect the underlying JDBC Connection.  Should be called at some point after manualDisconnect().
	 *
	 * @param suppliedConnection For user supplied connection strategy the user needs to hand us the connection
	 * with which to reconnect.  It is an error to pass a connection in the other strategies.
	 */
	public void manualReconnect(Connection suppliedConnection);
}
