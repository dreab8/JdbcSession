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
package org.hibernate.resource2.jdbc.internal;

import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.ConnectionReleaseMode;
import org.hibernate.JDBCException;
import org.hibernate.ResourceClosedException;
import org.hibernate.resource2.jdbc.spi.JdbcConnectionAccess;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.resource2.jdbc.spi.JdbcObserver;
import org.hibernate.resource2.jdbc.spi.LogicalConnection;

/**
 * Represents a LogicalConnection where we manage obtaining and releasing the Connection as needed.
 *
 * @author Steve Ebersole
 */
public class LogicalConnectionManagedImpl extends AbstractLogicalConnectionImpl {
	private static final CoreMessageLogger log = CoreLogging.messageLogger( LogicalConnectionManagedImpl.class );

	private final JdbcConnectionAccess connectionAccess;
	private final JdbcObserver observer;
	private final SqlExceptionHelper sqlExceptionHelper;

	private Connection physicalConnection;
	private boolean closed;

	public LogicalConnectionManagedImpl(
			JdbcConnectionAccess connectionAccess,
			JdbcObserver observer,
			SqlExceptionHelper sqlExceptionHelper) {
		this.connectionAccess = connectionAccess;
		this.observer = observer;
		this.sqlExceptionHelper = sqlExceptionHelper;
	}

	@Override
	public ConnectionReleaseMode interpret(ConnectionReleaseMode mode) {
		if ( mode == ConnectionReleaseMode.AFTER_STATEMENT
				&& !connectionAccess.supportsAggressiveRelease() ) {
			log.debug( "Connection provider reports to not support aggressive release; overriding" );
			return ConnectionReleaseMode.AFTER_TRANSACTION;
		}
		else {
			return mode;
		}
	}

	@Override
	protected boolean isOpen() {
		return !closed;
	}

	@Override
	public boolean isPhysicallyConnected() {
		return physicalConnection != null;
	}

	@Override
	protected Connection getConnectionForTransactionManagement() {
		return getConnection();
	}

	boolean initiallyAutoCommit;

	@Override
	public void begin() {
		initiallyAutoCommit = determineInitialAutoCommitMode( getConnectionForTransactionManagement() );
		super.begin();
	}

	@Override
	protected void afterCompletion() {
		resetConnection( initiallyAutoCommit );
		initiallyAutoCommit = false;
	}

	@Override
	public Connection getConnection() {
		if ( physicalConnection == null ) {
			obtainConnection();
		}
		return physicalConnection;
	}

	/**
	 * Physically opens a JDBC Connection.
	 *
	 * @throws org.hibernate.JDBCException Indicates problem opening a connection
	 */
	private void obtainConnection() throws JDBCException {
		if ( closed ) {
			throw new ResourceClosedException( "Logical connection is closed" );
		}

		log.debug( "Obtaining JDBC connection" );
		observer.jdbcConnectionAcquisitionStart();
		try {
			physicalConnection = connectionAccess.obtainConnection();
		}
		catch (SQLException e) {
			throw sqlExceptionHelper.convert( e, "Unable to obtain JDBC Connection" );
		}
		finally {
			observer.jdbcConnectionAcquisitionEnd();
		}
	}

	@Override
	public Connection close() {
		if ( closed ) {
			// no op
			return null;
		}

		log.trace( "Closing logical connection" );
		try {
			releaseConnection();
		}
		finally {
			// no matter what
			closed = true;
			log.trace( "Logical connection closed" );
		}
		return null;
	}

	/**
	 * Physically closes the JDBC Connection.
	 *
	 * @throws JDBCException Indicates problem closing a connection
	 */
	public void releaseConnection() {
		if ( closed ) {
			throw new ResourceClosedException( "Logical connection is closed" );
		}

		log.debug( "Releasing JDBC connection" );
		if ( physicalConnection == null ) {
			return;
		}

		try {
			observer.jdbcConnectionReleaseStart();
			if ( !physicalConnection.isClosed() ) {
				sqlExceptionHelper.logAndClearWarnings( physicalConnection );
			}
			connectionAccess.releaseConnection( physicalConnection );
		}
		catch (SQLException e) {
			throw sqlExceptionHelper.convert( e, "Unable to release JDBC Connection" );
		}
		finally {
			physicalConnection = null;
			observer.jdbcConnectionReleaseEnd();
			log.debug( "Released JDBC connection" );
		}
	}

	@Override
	public Connection manualDisconnect() {
		if ( closed ) {
			throw new ResourceClosedException( "Logical connection is closed" );
		}

		throw new IllegalStateException( "Cannot manually disconnect unless Connection was originally supplied by user" );
	}

	@Override
	public void manualReconnect(Connection suppliedConnection) {
		if ( closed ) {
			throw new ResourceClosedException( "Logical connection is closed" );
		}

		throw new IllegalStateException( "Cannot manually reconnect unless Connection was originally supplied by user" );
	}
}
