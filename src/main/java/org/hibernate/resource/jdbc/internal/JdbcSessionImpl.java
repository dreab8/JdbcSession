package org.hibernate.resource.jdbc.internal;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jboss.logging.Logger;

import org.hibernate.HibernateException;
import org.hibernate.resource.jdbc.LogicalConnection;
import org.hibernate.resource.jdbc.Operation;
import org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.backend.store.spi.DataStoreTransaction;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorOwner;

/**
 * @author Steve Ebersole
 */
public class JdbcSessionImpl
		implements JdbcSessionImplementor,
				   TransactionCoordinatorOwner {
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
	@SuppressWarnings("unchecked")
	public <R> R accept(Operation<R> operation) {
		try {
			return operation.perform( this );
		}
		catch (SQLException e) {
			throw context.getSqlExceptionHelper().convert( e, "Unable to release JDBC Connection" );
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new HibernateException( "Unexpected error performing JdbcOperation", e );
		}
	}

	@Override
	public <R> R accept(PreparedStatementQueryOperationSpec<R> operation) {
		try {
			final PreparedStatement statement = operation.getQueryStatementBuilder().buildQueryStatement(
					logicalConnection.getPhysicalConnection(),
					operation.getSql(),
					operation.getResultSetType(),
					operation.getResultSetConcurrency()
			);

			try {
				bindParameters( operation.getParameterBindings(), statement );

				configureStatement( operation, statement );

				final ResultSet resultSet = operation.getStatementExecutor().execute( statement, this );

				try {
					register( resultSet, statement );
					return operation.getResultSetProcessor().extractResults( resultSet, this );
				}
				finally {
					if ( !operation.holdOpenResources() ) {
						release( resultSet, statement );
					}
				}
			}
			finally {
				if ( !operation.holdOpenResources() ) {
					release( statement );
				}
			}
		}
		catch (SQLException e) {
			throw context.getSqlExceptionHelper().convert( e, "" );
		}
		finally {
			afterStatement( !operation.holdOpenResources() );
		}
	}

	private void bindParameters(ParameterBindings parameterBindings, PreparedStatement statement) {
		int col = parameterBindings.bindLimitOffsetParametersAtStartOfQuery( statement );

		col += parameterBindings.bindParameters( statement, col );

		parameterBindings.bindLimitOffsetParametersAtEndOfQuery( statement, col );

		parameterBindings.setMaxRow( statement );
	}

	private void register(ResultSet resultSet, Statement statement) {
		logicalConnection.getResourceRegistry().register( resultSet, statement );
	}

	private void release(ResultSet resultSet, Statement statement) {
		logicalConnection.getResourceRegistry().release( resultSet, statement );
	}

	private void release(Statement statement) {
		logicalConnection.getResourceRegistry().release( statement );
	}

	private <R> void configureStatement(PreparedStatementQueryOperationSpec<R> operation, Statement statement)
			throws SQLException {
		statement.setQueryTimeout( operation.getQueryTimeout() );
	}

	private void afterStatement(boolean holdOpernResources) {
		// todo : implement
	}

	// ResourceLocalTransactionAccess impl ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public DataStoreTransaction getResourceLocalTransaction() {
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
