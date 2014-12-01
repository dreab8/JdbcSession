package org.hibernate.resource.jdbc.internal;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jboss.logging.Logger;

import org.hibernate.HibernateException;
import org.hibernate.resource.jdbc.BatchableOperationSpec;
import org.hibernate.resource.jdbc.LogicalConnection;
import org.hibernate.resource.jdbc.Operation;
import org.hibernate.resource.jdbc.PreparedStatementWithGeneratedKeyInsertOperationSpec;
import org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec;
import org.hibernate.resource.jdbc.QueryOperationSpec;
import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.ScrollableQueryOperationSpec;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchFactory;
import org.hibernate.resource.jdbc.spi.BatchObserver;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.backend.store.spi.DataStoreTransaction;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorOwner;

import static org.hibernate.resource.jdbc.BatchableOperationSpec.BatchableOperationStep;
import static org.hibernate.resource.jdbc.PreparedStatementWithGeneratedKeyInsertOperationSpec.GenerateKeyResultSet;
import static org.hibernate.resource.jdbc.ScrollableQueryOperationSpec.Result;

/**
 * @author Steve Ebersolepublic JdbcSessionContext getSessionContext()nsp
 */
public class JdbcSessionImpl
		implements JdbcSessionImplementor,
				   TransactionCoordinatorOwner {
	private static final Logger log = Logger.getLogger( JdbcSessionImpl.class );

	private final JdbcSessionContext context;
	private final LogicalConnectionImplementor logicalConnection;
	private final TransactionCoordinator transactionCoordinator;
	private BatchFactory batchFactory;

	private boolean closed;

	public JdbcSessionImpl(
			JdbcSessionContext context,
			LogicalConnectionImplementor logicalConnection,
			TransactionCoordinatorBuilder transactionCoordinatorBuilder,
			BatchFactory batchFactory) {
		this.context = context;
		this.logicalConnection = logicalConnection;
		this.transactionCoordinator = transactionCoordinatorBuilder.buildTransactionCoordinator( this );
		this.batchFactory = batchFactory;
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
	public JdbcSessionContext getSessionContext() {
		return this.context;
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
	public void accept(BatchableOperationSpec operation, BatchableOperationSpec.Context operationContext) {
		Batch currentBatch = getResourceRegistry().getCurrentBatch();
		if ( currentBatch != null ) {
			if ( !currentBatch.getKey().equals( operation.getBatchKey() ) ) {
				currentBatch.execute();
				getResourceRegistry().releaseCurrentBatch();
				currentBatch = buildBatch( operation );
			}
		}
		else {
			currentBatch = buildBatch( operation );
		}
		try {
			for ( BatchableOperationStep step : operation.getSteps() ) {
				step.apply( currentBatch, logicalConnection.getPhysicalConnection(), operationContext );
			}
		}
		catch (SQLException e) {
			currentBatch.release();
			throw context.getSqlExceptionHelper().convert( e, "" );
		}
	}

	@Override
	public GenerateKeyResultSet accept(PreparedStatementWithGeneratedKeyInsertOperationSpec operation) {

		final PreparedStatement statement = operation.getStatementBuilder()
				.buildStatement(
						logicalConnection.getPhysicalConnection(),
						context,
						operation.getSql()
				);
		try {
			operation.getParameterBindings().bindParameters( statement );
			getResourceRegistry().register( statement, false );
			statement.executeUpdate();
		}
		catch (SQLException e) {
			getResourceRegistry().release( statement );
			throw context.getSqlExceptionHelper().convert( e, "" );
		}
		try {
			final ResultSet generatedKeys = statement.getGeneratedKeys();

			register( generatedKeys, statement );

			return new GenerateKeyResultSet() {
				@Override
				public void close() {
					getResourceRegistry().release( generatedKeys, statement );
				}

				@Override
				public ResultSet getGeneratedKeys() {
					return generatedKeys;
				}
			};

		}
		catch (SQLException e) {
			throw context.getSqlExceptionHelper().convert( e, "" );
		}
	}

	@Override
	public Result accept(ScrollableQueryOperationSpec operation) {
		final PreparedStatement statement = prepareStatement( operation );
		try {
			final ResultSet resultSet = operation.getStatementExecutor().execute( statement );

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
			getResourceRegistry().release( statement );
			throw context.getSqlExceptionHelper().convert( e, "" );
		}
	}

	@Override
	public <R> R accept(PreparedStatementQueryOperationSpec<R> operation) {
		final PreparedStatement statement = prepareStatement( operation );
		ResultSet resultSet = null;
		try {
			resultSet = operation.getStatementExecutor().execute( statement );
			return operation.getResultSetProcessor().extractResults( resultSet );
		}
		catch (SQLException e) {
			throw context.getSqlExceptionHelper().convert( e, "" );
		}
		finally {
			ResourceRegistryStandardImpl.close( resultSet );
			getResourceRegistry().release( resultSet, statement );
		}
	}

	@Override
	public void executeBatch() {
		final Batch currentBatch = getResourceRegistry().getCurrentBatch();
		if ( currentBatch != null ) {
			currentBatch.execute();
		}
	}

	@Override
	public void abortBatch() {
		final ResourceRegistry registry = getResourceRegistry();
		final Batch currentBatch = registry.getCurrentBatch();
		if ( currentBatch != null ) {
			currentBatch.release();
		}
		registry.releaseCurrentBatch();
	}

	private ResourceRegistry getResourceRegistry() {
		return getLogicalConnection().getResourceRegistry();
	}

	private Batch buildBatch(BatchableOperationSpec operation) {
		final Batch batch = batchFactory.buildBatch(
				operation.getBatchKey(),
				context.getSqlExceptionHelper(),
				operation.foregoBatching()
		);

		if ( operation.getObservers() != null ) {
			for ( BatchObserver observer : operation.getObservers() ) {
				batch.addObserver( observer );
			}
		}

		getResourceRegistry().register( batch );
		return batch;
	}

	private PreparedStatement prepareStatement(QueryOperationSpec operation) {

		final PreparedStatement statement = operation.getQueryStatementBuilder().buildQueryStatement(
				logicalConnection.getPhysicalConnection(),
				getSessionContext(),
				operation.getSql(),
				operation.getResultSetType(),
				operation.getResultSetConcurrency()
		);

		getResourceRegistry().register( statement, operation.isCancellable() );

		try {
			operation.getParameterBindings().bindParameters( statement );
			configureStatement( operation, statement );
		}
		catch (SQLException e) {
			getResourceRegistry().release( statement );
			throw context.getSqlExceptionHelper().convert( e, "" );
		}
		return statement;
	}

	private void register(ResultSet resultSet, Statement statement) {
		logicalConnection.getResourceRegistry().register( resultSet, statement );
	}

	private void configureStatement(QueryOperationSpec operation, Statement statement)
			throws SQLException {
		final int queryTimeOut = operation.getQueryTimeout();
		if ( queryTimeOut > 0 ) {
			statement.setQueryTimeout( operation.getQueryTimeout() );
		}
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
