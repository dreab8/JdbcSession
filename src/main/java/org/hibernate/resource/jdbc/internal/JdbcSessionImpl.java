package org.hibernate.resource.jdbc.internal;

import org.hibernate.resource.jdbc.LogicalConnection;
import org.hibernate.resource.jdbc.Operation;
import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.resource.jdbc.spi.PhysicalJdbcTransaction;
import org.hibernate.resource.jdbc.spi.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.TransactionCoordinator;

import org.jboss.logging.Logger;

/**
 * @author Steve Ebersole
 */
public class JdbcSessionImpl implements JdbcSessionImplementor {
	private static final Logger log = Logger.getLogger( JdbcSessionImpl.class );

	private final JdbcSessionContext context;
	private final LogicalConnectionImplementor logicalConnection;
	private final TransactionCoordinator transactionCoordinator;
	private final ResourceRegistryStandardImpl resourceRegistry;

	private boolean closed;

	public JdbcSessionImpl(
			JdbcSessionContext context,
			LogicalConnectionImplementor logicalConnection,
			TransactionCoordinatorBuilder transactionCoordinatorBuilder) {
		this.context = context;
		this.logicalConnection = logicalConnection;
		this.transactionCoordinator = transactionCoordinatorBuilder.buildTransactionCoordinator( this );
		this.resourceRegistry = new ResourceRegistryStandardImpl();
	}

	@Override
	public JdbcSessionContext getJdbcSessionContext() {
		return context;
	}

	@Override
	public PhysicalJdbcTransaction getPhysicalJdbcTransaction() {
		return logicalConnection.getPhysicalJdbcTransaction();
	}

	@Override
	public LogicalConnection getLogicalConnection() {
		return logicalConnection;
	}

	@Override
	public ResourceRegistry getResourceRegistry() {
		return resourceRegistry;
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
			resourceRegistry.releaseResources();
			logicalConnection.close();
		}
		finally {
			closed = true;
		}
	}

	@Override
	public boolean isReadyToSerialize() {
		return !logicalConnection.isPhysicallyConnected() && !resourceRegistry.hasRegisteredResources();
	}

	@Override
	public <T> T accept(Operation<T> operation) {
		return null;  //To change body of implemented methods use File | Settings | File Templates.
	}
}
