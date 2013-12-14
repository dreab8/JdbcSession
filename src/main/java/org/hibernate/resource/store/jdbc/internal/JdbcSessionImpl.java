package org.hibernate.resource.store.jdbc.internal;

import org.hibernate.resource.store.Operation;
import org.hibernate.resource.store.jdbc.LogicalConnection;
import org.hibernate.resource.store.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.store.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource.store.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.resource.transaction.ResourceLocalTransaction;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.spi.ResourceLocalTransactionCoordinatorOwner;

import org.jboss.logging.Logger;

/**
 * @author Steve Ebersole
 */
public class JdbcSessionImpl implements JdbcSessionImplementor, ResourceLocalTransactionCoordinatorOwner {
	private static final Logger log = Logger.getLogger( JdbcSessionImpl.class );

	private final JdbcSessionContext context;
	private final LogicalConnectionImplementor logicalConnection;
	private final TransactionCoordinator transactionCoordinator;

	private boolean closed;

	public JdbcSessionImpl(
			JdbcSessionContext context,
			LogicalConnectionImplementor logicalConnection,
			TransactionCoordinatorBuilder transactionCoordinatorBuilder) {
		this.context = context;
		this.logicalConnection = logicalConnection;
		this.transactionCoordinator = transactionCoordinatorBuilder.buildTransactionCoordinator( this );
	}

	@Override
	public LogicalConnection getLogicalConnection() {
		return logicalConnection;
	}

	@Override
	public TransactionCoordinator getTransactionCoordinator() {
		return transactionCoordinator;
	}

	@Override
	public boolean isOpen() {
		return !closed;
	}

	@Override
	public void close() {
		if ( closed ) {
			return;
		}

		try {
			logicalConnection.close();
		}
		finally {
			closed = true;
		}
	}

	@Override
	public boolean isReadyToSerialize() {
		// todo : new LogicalConnectionImplementor.isReadyToSerialize method?
		return !logicalConnection.isPhysicallyConnected()
				&& !logicalConnection.getResourceRegistry().hasRegisteredResources();
	}

	@Override
	public <T> T accept(Operation<T> operation) {
		// todo : implement
		return null;
	}


	// ResourceLocalTransactionCoordinatorOwner impl ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public ResourceLocalTransaction getResourceLocalTransaction() {
		return logicalConnection.getPhysicalJdbcTransaction();
	}

	@Override
	public boolean isActive() {
		return isOpen();
	}

	@Override
	public void beforeTransactionCompletion() {
		// todo : implement
		// for now, just log...
		log.trace( "JdbcSessionImpl#beforeTransactionCompletion" );
	}

	@Override
	public void afterTransactionCompletion(boolean successful) {
		// todo : implement
		// for now, just log...
		log.tracef( "JdbcSessionImpl#afterTransactionCompletion(%s)", successful );
	}
}
