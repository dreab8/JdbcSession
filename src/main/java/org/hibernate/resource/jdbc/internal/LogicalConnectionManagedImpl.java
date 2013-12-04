package org.hibernate.resource.jdbc.internal;

import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.ResourceClosedException;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.resource.jdbc.spi.JdbcConnectionAccess;
import org.hibernate.resource.jdbc.spi.JdbcObserver;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;

import org.jboss.logging.Logger;

/**
 * Represents a LogicalConnection where we manage obtaining and releasing the Connection as needed.
 *
 * @author Steve Ebersole
 */
public class LogicalConnectionManagedImpl extends AbstractLogicalConnectionImplementor {
	private static final Logger log = Logger.getLogger( LogicalConnectionManagedImpl.class );

	private final JdbcConnectionAccess jdbcConnectionAccess;
	private final JdbcObserver observer;
	private final SqlExceptionHelper sqlExceptionHelper;

	private Connection physicalConnection;
	private boolean closed;

	public LogicalConnectionManagedImpl(
			JdbcConnectionAccess jdbcConnectionAccess,
			JdbcSessionContext jdbcSessionContext) {
		this.jdbcConnectionAccess = jdbcConnectionAccess;
		this.observer = jdbcSessionContext.getObserver();
		this.sqlExceptionHelper = jdbcSessionContext.getSqlExceptionHelper();
	}

	@Override
	public boolean isOpen() {
		return !closed;
	}

	@Override
	public boolean isPhysicallyConnected() {
		return physicalConnection != null;
	}

	@Override
	protected Connection getConnectionForTransactionManagement() {
		return getPhysicalConnection();
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
	public Connection getPhysicalConnection() {
		errorIfClosed();
		return acquireConnectionIfNeeded();
	}

	private Connection acquireConnectionIfNeeded() {
		errorIfClosed();

		if ( physicalConnection == null ) {
			// todo : is this the right place for these observer calls?
			observer.jdbcConnectionAcquisitionStart();
			try {
				physicalConnection = jdbcConnectionAccess.obtainConnection();
			}
			catch (SQLException e) {
				throw sqlExceptionHelper.convert( e, "Unable to acquire JDBC Connection" );
			}
			finally {
				observer.jdbcConnectionAcquisitionEnd();
			}
		}
		return physicalConnection;
	}

	@Override
	public void releaseConnection() {
		errorIfClosed();

		if ( physicalConnection == null ) {
			return;
		}

		// todo : is this the right place for these observer calls?
		observer.jdbcConnectionReleaseStart();
		try {
			if ( !physicalConnection.isClosed() ) {
				sqlExceptionHelper.logAndClearWarnings( physicalConnection );
			}
			jdbcConnectionAccess.releaseConnection( physicalConnection );
		}
		catch (SQLException e) {
			throw sqlExceptionHelper.convert( e, "Unable to release JDBC Connection" );
		}
		finally {
			observer.jdbcConnectionReleaseEnd();
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

	@Override
	public LogicalConnectionImplementor makeShareableCopy() {
		errorIfClosed();
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}


	@Override
	public Connection close() {
		if ( closed ) {
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
}
