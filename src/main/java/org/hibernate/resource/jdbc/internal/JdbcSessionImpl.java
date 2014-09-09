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
import org.hibernate.resource.jdbc.QueryOperationSpec;
import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.ScrollableQueryOperationSpec;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.backend.store.spi.DataStoreTransaction;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorOwner;

import static org.hibernate.resource.jdbc.ScrollableQueryOperationSpec.Result;

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
				&& !getResourceRegistry().hasRegisteredResources();
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
	public Result accept(ScrollableQueryOperationSpec operation) {
		try {
			final PreparedStatement statement = prepareStatement( operation );

			final ResultSet resultSet = operation.getStatementExecutor().execute( statement, this );

			register( resultSet, statement );

			return new Result() {
				@Override
				public void close() {
					getResourceRegistry().release( resultSet, statement );
				}

				@Override
				public ResultSet getResultSet() {
					return resultSet;
				}
			};
		}
		catch (SQLException e) {
			throw context.getSqlExceptionHelper().convert( e, "" );
		}
	}

	@Override
	public <R> R accept(PreparedStatementQueryOperationSpec<R> operation) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = prepareStatement( operation );

			resultSet = operation.getStatementExecutor().execute( statement, this );

			return operation.getResultSetProcessor().extractResults( resultSet, this );
		}
		catch (SQLException e) {
			throw context.getSqlExceptionHelper().convert( e, "" );
		}
		finally {
			if ( resultSet != null ) {
				close( resultSet );
			}
			getResourceRegistry().release( statement );
		}
	}

	private PreparedStatement prepareStatement(QueryOperationSpec operation) throws SQLException {
		final PreparedStatement statement = operation.getQueryStatementBuilder().buildQueryStatement(
				logicalConnection.getPhysicalConnection(),
				operation.getSql(),
				operation.getResultSetType(),
				operation.getResultSetConcurrency()
		);

		operation.getParameterBindings().bindParameters( statement );

		configureStatement( operation, statement );

		return statement;
	}

	private void register(ResultSet resultSet, Statement statement) {
		logicalConnection.getResourceRegistry().register( resultSet, statement );
	}

	private void configureStatement(QueryOperationSpec operation, Statement statement)
			throws SQLException {
		statement.setQueryTimeout( operation.getQueryTimeout() );
	}

	private ResourceRegistry getResourceRegistry() {
		return getLogicalConnection().getResourceRegistry();
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

	private void close(ResultSet resultSet) {
		ResourceRegistryStandardImpl.close( resultSet );
	}

	protected void close(Statement statement) {
		ResourceRegistryStandardImpl.close( statement );
	}
}
