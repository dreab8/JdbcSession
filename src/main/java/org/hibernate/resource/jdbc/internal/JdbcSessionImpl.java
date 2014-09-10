package org.hibernate.resource.jdbc.internal;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.jboss.logging.Logger;

import org.hibernate.HibernateException;
import org.hibernate.resource.jdbc.BatchableOperationSpec;
import org.hibernate.resource.jdbc.LogicalConnection;
import org.hibernate.resource.jdbc.Operation;
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
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.StatementBuilder;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilder;
import org.hibernate.resource.transaction.backend.store.spi.DataStoreTransaction;
import org.hibernate.resource.transaction.spi.TransactionCoordinatorOwner;

import static org.hibernate.resource.jdbc.BatchableOperationSpec.InsertOpertationStep;
import static org.hibernate.resource.jdbc.BatchableOperationSpec.OperationStep;
import static org.hibernate.resource.jdbc.BatchableOperationSpec.UpdateOrInsertOperationStep;
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
	public void accept(BatchableOperationSpec operation) {
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
			addStepsToBatch( currentBatch, operation.getSteps() );
		}
		catch (SQLException e) {
			throw context.getSqlExceptionHelper().convert( e, "" );
		}
	}

	@Override
	public Result accept(ScrollableQueryOperationSpec operation) {
		try {
			logicalConnection.getResourceRegistry();

			final PreparedStatement statement = prepareStatement( operation );

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
			throw context.getSqlExceptionHelper().convert( e, "" );
		}
	}

	@Override
	public <R> R accept(PreparedStatementQueryOperationSpec<R> operation) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = prepareStatement( operation );

			resultSet = operation.getStatementExecutor().execute( statement );

			return operation.getResultSetProcessor().extractResults( resultSet );
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
		addObservers( batch, operation.getObservers() );
		getResourceRegistry().register( batch );
		return batch;
	}

	private void addObservers(Batch batch, List<BatchObserver> observers) {
		if ( observers != null ) {
			for ( BatchObserver observer : observers ) {
				batch.addObserver( observer );
			}
		}
	}

	private void addStepsToBatch(Batch currentBatch, List<OperationStep> steps)
			throws SQLException {
		for ( OperationStep step : steps ) {
			if ( step instanceof UpdateOrInsertOperationStep ) {

				final UpdateOrInsertOperationStep updateStep = (UpdateOrInsertOperationStep) step;

				PreparedStatement statement = prepareStatement(
						updateStep.getQueryStatementBuilder(),
						updateStep.getUpdateParameterBindings(),
						updateStep.getUpdateSql()
				);

				int rowCount = statement.executeUpdate();

				close( statement );

				if ( rowCount == 0 ) {
					statement = currentBatch.getStatement( updateStep.getInsertSql() );
					if ( statement == null ) {
						statement = prepareStatement(
								updateStep.getQueryStatementBuilder(),
								updateStep.getInsertParameterBindings(),
								updateStep.getInsertSql()
						);
					}
					currentBatch.addBatch( updateStep.getInsertSql(), statement );
				}
			}
			else if ( step instanceof InsertOpertationStep ) {
				InsertOpertationStep insertOpertationStep = (InsertOpertationStep) step;
				PreparedStatement statement = prepareStatement(
						insertOpertationStep.getQueryStatementBuilder(),
						insertOpertationStep.getParameterBindings(),
						insertOpertationStep.getSql()
				);

				statement.executeUpdate();

				ResultSet generatedKeys = statement.getGeneratedKeys();
				insertOpertationStep.storeGeneratedId( generatedKeys );
				close( generatedKeys );
				close( statement );
			}
			else {
				addStepToBatch( currentBatch, (BatchableOperationSpec.GenericOperationStep) step );
			}
		}
	}

	private void addStepToBatch(Batch currentBatch, BatchableOperationSpec.GenericOperationStep step)
			throws SQLException {
		final String sql = step.getSql();
		PreparedStatement statement = currentBatch.getStatement( sql );
		if ( statement == null ) {
			statement = prepareStatement( step.getQueryStatementBuilder(), step.getParameterBindings(), sql );
		}
		currentBatch.addBatch( sql, statement );

	}

	private PreparedStatement prepareStatement(
			StatementBuilder<? extends PreparedStatement> statementBuilder,
			ParameterBindings parameterBindings,
			String sql) throws SQLException {
		final PreparedStatement statement = statementBuilder.buildPreparedStatement(
				logicalConnection.getPhysicalConnection(),
				sql
		);
		parameterBindings.bindParameters( statement );
		return statement;
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
