package org.hibernate.resource.jdbc.internal;

import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.ResourceClosedException;
import org.hibernate.TransactionException;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.resource.jdbc.spi.PhysicalJdbcTransaction;

import org.jboss.logging.Logger;

/**
 * @author Steve Ebersole
 */
public abstract class AbstractLogicalConnectionImplementor implements LogicalConnectionImplementor, PhysicalJdbcTransaction {
	private static final Logger log = Logger.getLogger( AbstractLogicalConnectionImplementor.class );

	@Override
	public PhysicalJdbcTransaction getPhysicalJdbcTransaction() {
		errorIfClosed();
		return this;
	}

	protected void errorIfClosed() {
		if ( !isOpen() ) {
			throw new ResourceClosedException( this.toString() + " is closed" );
		}
	}

	protected abstract Connection getConnectionForTransactionManagement();

	@Override
	public void begin() {
		try {
			log.trace( "Preparing to 'begin' transaction via JDBC Connection.setAutoCommit(false)" );
			getConnectionForTransactionManagement().setAutoCommit( false );
			log.trace( "Transaction 'begun' via JDBC Connection.setAutoCommit(false)" );
		}
		catch( SQLException e ) {
			throw new TransactionException( "JDBC begin transaction failed: ", e );
		}
	}

	@Override
	public void commit() {
		try {
			log.trace( "Beginning physical transaction commit via JDBC Connection.commit()" );
			getConnectionForTransactionManagement().commit();
			log.trace( "Completed physical transaction commit via JDBC Connection.commit()" );
		}
		catch( SQLException e ) {
			throw new TransactionException( "Unable to commit against JDBC Connection", e );
		}

		afterCompletion();
	}

	protected void afterCompletion() {
		// by default, nothing to do
	}

	protected void resetConnection(boolean initiallyAutoCommit) {
		try {
			if ( initiallyAutoCommit ) {
				log.trace( "re-enabling auto-commit on JDBC Connection after completion of JDBC-based transaction" );
				getConnectionForTransactionManagement().setAutoCommit( true );
			}
		}
		catch ( Exception e ) {
			log.debug(
					"Could not re-enable auto-commit on JDBC Connection after completion of JDBC-based transaction : " + e
			);
		}
	}

	@Override
	public void rollback() {
		try {
			log.trace( "Beginning physical transaction rollback via JDBC Connection.rollback()" );
			getConnectionForTransactionManagement().rollback();
			log.trace( "Completed physical transaction rollback via JDBC Connection.rollback()" );
		}
		catch( SQLException e ) {
			throw new TransactionException( "Unable to rollback against JDBC Connection", e );
		}

		afterCompletion();
	}

	protected static boolean determineInitialAutoCommitMode(Connection providedConnection) {
		try {
			return providedConnection.getAutoCommit();
		}
		catch (SQLException e) {
			log.debug( "Unable to ascertain initial auto-commit state of provided connection; assuming auto-commit" );
			return true;
		}
	}
}
