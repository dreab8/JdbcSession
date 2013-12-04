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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.hibernate.AssertionFailure;
import org.hibernate.ConnectionReleaseMode;
import org.hibernate.HibernateException;
import org.hibernate.JDBCException;
import org.hibernate.ResourceClosedException;
import org.hibernate.ScrollMode;
import org.hibernate.TransactionException;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.resource2.jdbc.spi.TransactionCoordinatorBuilder;
import org.hibernate.resource2.transaction.TransactionCoordinator;
import org.hibernate.resource2.jdbc.CallableExecutionOutputs;
import org.hibernate.resource2.jdbc.ExecutionOutput;
import org.hibernate.resource2.jdbc.Operation;
import org.hibernate.resource2.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource2.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource2.jdbc.spi.LogicalConnection;
import org.hibernate.resource2.jdbc.spi.PhysicalJdbcTransaction;
import org.hibernate.resource2.jdbc.spi.StatementInspector;

/**
 * Models the context of a JDBC Connection and transaction.  The name comes from the database view where what is
 * represented by the JDBC Connection (and associated transaction) is called a "session".
 *
 * @author Steve Ebersole
 */
public class JdbcSessionImpl implements JdbcSessionImplementor {
	private static final CoreMessageLogger log = CoreLogging.messageLogger( JdbcSessionImpl.class );

	private boolean closed;

	private final JdbcSessionContext context;
	private final LogicalConnection logicalConnection;
	private final TransactionCoordinator transactionCoordinator;

	private final Map<Statement,Set<ResultSet>> xref = new HashMap<Statement,Set<ResultSet>>();
	private final Set<ResultSet> unassociatedResultSets = new HashSet<ResultSet>();

	private Statement lastQuery;

	private final ConnectionReleaseMode connectionReleaseMode;
	private boolean connectionReleaseDisabled;

	public JdbcSessionImpl(
			JdbcSessionContext context,
			LogicalConnection logicalConnection,
			TransactionCoordinatorBuilder transactionCoordinatorBuilder) {
		this.context = context;
		this.logicalConnection = logicalConnection;
		this.connectionReleaseMode = logicalConnection.interpret( context.getConnectionReleaseMode() );
		this.transactionCoordinator = transactionCoordinatorBuilder.buildTransactionCoordinator( this );
	}

	public JdbcSessionContext getContext() {
		return context;
	}

	public StatementInspector getStatementInspector() {
		return getContext().getStatementInspector();
	}

	public JDBCException convert(SQLException e, String message) {
		return context.getSqlExceptionHelper().convert( e, message );
	}

	public JDBCException convert(SQLException e, String message, String sql) {
		return context.getSqlExceptionHelper().convert( e, message, sql );
	}

	@Override
	public boolean isPhysicallyConnected() {
		return logicalConnection.isPhysicallyConnected();
	}

	@Override
	public boolean isReadyToSerialize() {
		return !logicalConnection.isPhysicallyConnected() && !hasRegisteredResources();
	}

	private Connection connection() {
		return logicalConnection.getConnection();
	}

	@Override
	public <T> T accept(org.hibernate.resource2.jdbc.Work<T> work) {
		if ( closed ) {
			throw new ResourceClosedException( "JdbcSession is already closed" );
		}

		try {
			return work.perform( logicalConnection.getConnection() );
		}
		catch (SQLException e) {
			throw context.getSqlExceptionHelper().convert(
					e,
					"Error calling Work#perform [" + work.getClass().getName() + "]"
			);
		}
	}

	@Override
	public <T> T accept(Operation<T> operation) {
		if ( closed ) {
			throw new ResourceClosedException( "JdbcSession is already closed" );
		}

		// todo : would be great to segment the resources registered from this operation and auto-release them when done.
		// 		For now best we can do:
		final boolean hadResourcesBeforeOperation = hasRegisteredResources();
		try {
			return operation.perform( this );
		}
		catch (SQLException e) {
			throw context.getSqlExceptionHelper().convert(
					e,
					"Error calling Operation#perform [" + operation.getClass().getName() + "]"
			);
		}
		finally {
			if ( !hadResourcesBeforeOperation ) {
				releaseRegisteredResources();
			}
		}
	}

//	@Override
//	public void startingTransactionCompletion() {
//		// todo : tbh, not sure we need this one
//	}
//
//	@Override
//	public void transactionCompleted(boolean wasSuccessful) {
//		transactionTimeOutInstant = -1;
//
//		if ( connectionReleaseMode == ConnectionReleaseMode.AFTER_STATEMENT
//				|| connectionReleaseMode == ConnectionReleaseMode.AFTER_TRANSACTION ) {
//			if ( hasRegisteredResources() ) {
//				log.forcingContainerResourceCleanup();
//				releaseRegisteredResources();
//			}
//			log.debug( "Aggressively releasing JDBC connection after-transaction" );
//			logicalConnection.releaseConnection();
//		}
//	}

	@Override
	public TransactionCoordinator getTransactionCoordinator() {
		return transactionCoordinator;
	}

	@Override
	public boolean isOpen() {
		return !closed;
	}

	@Override
	public Connection close() {
		if ( closed ) {
			return null;
		}

		try {
			releaseRegisteredResources();
			return logicalConnection.close();
		}
		finally {
			closed = true;
		}
	}

	@Override
	public void enableConnectionRelease() {
		log.trace( "(re)enabling connection releasing" );
		connectionReleaseDisabled = false;
		afterStatementConnectionRelease();
	}

	@Override
	public void disableConnectionRelease() {
		log.trace( "disabling connection releasing" );
		connectionReleaseDisabled = true;
	}

	@Override
	public PhysicalJdbcTransaction getPhysicalJdbcTransaction() {
		return logicalConnection.getPhysicalJdbcTransaction();
	}

	private void afterStatementConnectionRelease() {
		if ( connectionReleaseMode == ConnectionReleaseMode.AFTER_STATEMENT ) {
			if ( connectionReleaseDisabled ) {
				log.trace( "Skipping after-statement connection release due to manual disabling" );
				return;
			}
			if ( hasRegisteredResources() ) {
				log.trace( "Skipping after-statement connection release due to registered resources" );
			}

			log.debug( "Aggressively releasing JDBC connection after-statement" );
			logicalConnection.releaseConnection();
		}
	}


	// ResourceRegistry ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public void register(Statement statement) {
		log.tracef( "Registering statement [%s]", statement );
		if ( xref.containsKey( statement ) ) {
			throw new HibernateException( "JDBC Statement already registered" );
		}
		xref.put( statement, null );
	}

	@Override
	public void release(Statement statement) {
		log.tracev( "Releasing statement [{0}]", statement );
		final Set<ResultSet> resultSets = xref.get( statement );
		if ( resultSets != null ) {
			for ( ResultSet resultSet : resultSets ) {
				close( resultSet );
			}
			resultSets.clear();
		}
		xref.remove( statement );
		close( statement );

		afterStatementConnectionRelease();
	}

	@SuppressWarnings({ "unchecked" })
	protected void close(ResultSet resultSet) {
		log.tracef( "Closing result set [%s]", resultSet );

		try {
			resultSet.close();
		}
		catch (SQLException e) {
			log.debugf( "Unable to release JDBC result set [%s]", e.getMessage() );
		}
		catch ( Exception e ) {
			// try to handle general errors more elegantly
			log.debugf( "Unable to release JDBC result set [%s]", e.getMessage() );
		}
	}

	@SuppressWarnings({ "unchecked" })
	protected void close(Statement statement) {
		log.tracef( "Closing prepared statement [%s]", statement );

		try {
			// if we are unable to "clean" the prepared statement,
			// we do not close it
			try {
				if ( statement.getMaxRows() != 0 ) {
					statement.setMaxRows( 0 );
				}
				if ( statement.getQueryTimeout() != 0 ) {
					statement.setQueryTimeout( 0 );
				}
			}
			catch( SQLException sqle ) {
				// there was a problem "cleaning" the prepared statement
				if ( log.isDebugEnabled() ) {
					log.debugf( "Exception clearing maxRows/queryTimeout [%s]", sqle.getMessage() );
				}
				// EARLY EXIT!!!
				return;
			}
			statement.close();
			if ( lastQuery == statement ) {
				lastQuery = null;
			}
		}
		catch( SQLException e ) {
			log.debugf( "Unable to release JDBC statement [%s]", e.getMessage() );
		}
		catch ( Exception e ) {
			// try to handle general errors more elegantly
			log.debugf( "Unable to release JDBC statement [%s]", e.getMessage() );
		}
	}

	@Override
	public void register(ResultSet resultSet, Statement statement) {
		log.tracef( "Registering result set [%s]", resultSet );

		if ( statement == null ) {
			try {
				statement = resultSet.getStatement();
			}
			catch ( SQLException e ) {
				throw convert( e, "unable to access Statement from ResultSet" );
			}
		}
		if ( statement != null ) {
			// Keep this at DEBUG level, rather than warn.  Numerous connection pool implementations can return a
			// proxy/wrapper around the JDBC Statement, causing excessive logging here.  See HHH-8210.
			if ( log.isDebugEnabled() && !xref.containsKey( statement ) ) {
				log.debug( "ResultSet statement was not registered (on register)" );
			}
			Set<ResultSet> resultSets = xref.get( statement );
			if ( resultSets == null ) {
				resultSets = new HashSet<ResultSet>();
				xref.put( statement, resultSets );
			}
			resultSets.add( resultSet );
		}
		else {
			unassociatedResultSets.add( resultSet );
		}
	}

	@Override
	public void release(ResultSet resultSet, Statement statement) {
		log.tracef( "Releasing result set [%s]", resultSet );

		if ( statement == null ) {
			try {
				statement = resultSet.getStatement();
			}
			catch ( SQLException e ) {
				throw convert( e, "unable to access Statement from ResultSet" );
			}
		}
		if ( statement != null ) {
			// Keep this at DEBUG level, rather than warn.  Numerous connection pool implementations can return a
			// proxy/wrapper around the JDBC Statement, causing excessive logging here.  See HHH-8210.
			if ( log.isDebugEnabled() && !xref.containsKey( statement ) ) {
				log.unregisteredStatement();
				log.debug( "ResultSet statement was not registered (on release)" );
			}
			final Set<ResultSet> resultSets = xref.get( statement );
			if ( resultSets != null ) {
				resultSets.remove( resultSet );
				if ( resultSets.isEmpty() ) {
					xref.remove( statement );
				}
			}
		}
		else {
			final boolean removed = unassociatedResultSets.remove( resultSet );
			if ( !removed ) {
				log.unregisteredResultSetWithoutStatement();
			}
		}
		close( resultSet );
	}

	public void registerLastQuery(Statement statement) {
		log.tracef( "Registering last query statement [%s]", statement );
		lastQuery = statement;
	}

	@Override
	public void cancelLastQuery() {
		try {
			if (lastQuery != null) {
				lastQuery.cancel();
			}
		}
		catch (SQLException e) {
			throw convert( e, "Cannot cancel query" );
		}
		finally {
			lastQuery = null;
		}
	}

	@Override
	public boolean hasRegisteredResources() {
		return ! xref.isEmpty() || ! unassociatedResultSets.isEmpty();
	}

	public void releaseRegisteredResources() {
		log.tracef( "Releasing JDBC resources [%s]", this );
		for ( Map.Entry<Statement,Set<ResultSet>> entry : xref.entrySet() ) {
			if ( entry.getValue() != null ) {
				closeAll( entry.getValue() );
			}
			close( entry.getKey() );
		}
		xref.clear();

		closeAll( unassociatedResultSets );
	}

	protected void closeAll(Set<ResultSet> resultSets) {
		for ( ResultSet resultSet : resultSets ) {
			close( resultSet );
		}
		resultSets.clear();
	}


	// StatementPreparer ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public Statement createStatement() {
		try {
			final Statement statement = connection().createStatement();
			register( statement );
			return statement;
		}
		catch ( SQLException e ) {
			throw convert( e, "could not create statement" );
		}
	}

	@Override
	public PreparedStatement prepareStatement(String sql) {
		return buildPreparedStatementPreparationTemplate( sql, false ).prepareStatement();
	}

	private StatementPreparationTemplate buildPreparedStatementPreparationTemplate(String sql, final boolean isCallable) {
		return new StatementPreparationTemplate( this, sql ) {
			@Override
			protected PreparedStatement doPrepare() throws SQLException {
				return isCallable
						? connection().prepareCall( getSql() )
						: connection().prepareStatement( getSql() );
			}
		};
	}

	@Override
	public PreparedStatement prepareStatement(String sql, boolean isCallable) {
		executeBatch();
		return buildPreparedStatementPreparationTemplate( sql, isCallable ).prepareStatement();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, final int autoGeneratedKeys) {
		if ( autoGeneratedKeys == PreparedStatement.RETURN_GENERATED_KEYS ) {
			checkAutoGeneratedKeysSupportEnabled();
		}
		executeBatch();
		return new StatementPreparationTemplate( this, sql ) {
			@SuppressWarnings("MagicConstant")
			public PreparedStatement doPrepare() throws SQLException {
				return connection().prepareStatement( getSql(), autoGeneratedKeys );
			}
		}.prepareStatement();
	}

	private void checkAutoGeneratedKeysSupportEnabled() {
		if ( ! context.isGetGeneratedKeysEnabled() ) {
			throw new AssertionFailure( "getGeneratedKeys() support is not enabled" );
		}
	}

	@Override
	public PreparedStatement prepareStatement(String sql, final String[] columnNames) {
		checkAutoGeneratedKeysSupportEnabled();
		executeBatch();
		return new StatementPreparationTemplate( this, sql ) {
			public PreparedStatement doPrepare() throws SQLException {
				return connection().prepareStatement( getSql(), columnNames );
			}
		}.prepareStatement();
	}

	@Override
	public PreparedStatement prepareQueryStatement(String sql, final boolean isCallable, final ScrollMode scrollMode) {
		final PreparedStatement ps;

		if ( scrollMode != null && !scrollMode.equals( ScrollMode.FORWARD_ONLY ) ) {
			if ( ! context.isScrollableResultSetsEnabled() ) {
				throw new AssertionFailure("scrollable result sets are not enabled");
			}

			ps = new QueryStatementPreparationTemplate( this, sql ) {
				@SuppressWarnings("MagicConstant")
				public PreparedStatement doPrepare() throws SQLException {
					return isCallable
							? connection().prepareCall( getSql(), scrollMode.toResultSetType(), ResultSet.CONCUR_READ_ONLY )
							: connection().prepareStatement( getSql(), scrollMode.toResultSetType(), ResultSet.CONCUR_READ_ONLY );
				}
			}.prepareStatement();
		}
		else {
			ps = new QueryStatementPreparationTemplate( this, sql ) {
				public PreparedStatement doPrepare() throws SQLException {
					return isCallable
							? connection().prepareCall( getSql() )
							: connection().prepareStatement( getSql() );
				}
			}.prepareStatement();
		}

		registerLastQuery( ps );
		return ps;
	}


	// StatementExecutor ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	@Override
	public void executeDdl(Statement statement, String sql) {
		context.getSqlStatementLogger().logStatement( sql );
		try {
			context.getObserver().jdbcExecuteStatementStart();
			statement.execute( sql );
		}
		catch (SQLException e) {
			throw convert( e, "Could not execute DDL statement" );
		}
		finally {
			context.getObserver().jdbcExecuteStatementEnd();
			afterStatementConnectionRelease();
		}
	}

	@Override
	public int executeUpdate(Statement statement, String sql) {
		context.getSqlStatementLogger().logStatement( sql );
		try {
			context.getObserver().jdbcExecuteStatementStart();
			return statement.executeUpdate( sql );
		}
		catch (SQLException e) {
			throw convert( e, "Could not execute DDL statement" );
		}
		finally {
			context.getObserver().jdbcExecuteStatementEnd();
			afterStatementConnectionRelease();
		}
	}

	@Override
	public ResultSet executeQuery(Statement statement, String sql) {
		context.getSqlStatementLogger().logStatement( sql );
		final ResultSet resultSet = doExecuteQuery( statement, sql );
		register( resultSet, statement );
		return resultSet;
	}

	private ResultSet doExecuteQuery(Statement statement, String sql) {
		try {
			context.getObserver().jdbcExecuteStatementStart();
			return statement.executeQuery( sql );
		}
		catch (SQLException e) {
			throw convert( e, "Could not execute PreparedStatement" );
		}
		finally {
			context.getObserver().jdbcExecuteStatementEnd();
		}
	}

	@Override
	public ExecutionOutput execute(PreparedStatement statement) {
		// todo : implement
		return null;
	}

	@Override
	public int executeUpdate(PreparedStatement statement) {
		try {
			context.getObserver().jdbcExecuteStatementStart();
			return statement.executeUpdate();
		}
		catch (SQLException e) {
			throw convert( e, "Could not execute PreparedStatement" );
		}
		finally {
			context.getObserver().jdbcExecuteStatementEnd();
			afterStatementConnectionRelease();
		}
	}

	@Override
	public ResultSet executeQuery(PreparedStatement statement) {
		final ResultSet resultSet = doExecuteQuery( statement );
		register( resultSet, statement );
		return resultSet;
	}

	private ResultSet doExecuteQuery(PreparedStatement statement) {
		try {
			context.getObserver().jdbcExecuteStatementStart();
			return statement.executeQuery();
		}
		catch (SQLException e) {
			throw convert( e, "Could not execute PreparedStatement" );
		}
		finally {
			context.getObserver().jdbcExecuteStatementEnd();
		}
	}

	@Override
	public CallableExecutionOutputs execute(CallableStatement callableStatement) {
		// todo : implement
		return null;
	}


	// batching ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	public void executeBatch() {
		// todo : implement
	}


	// StatementPreparationTemplate ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	private transient long transactionTimeOutInstant = -1;

	@Override
	public void setTransactionTimeOut(int seconds) {
		transactionTimeOutInstant = System.currentTimeMillis() + ( seconds * 1000 );
	}

	public int determineRemainingTransactionTimeOutPeriod() {
		if ( transactionTimeOutInstant < 0 ) {
			return -1;
		}
		final int secondsRemaining = (int) ((transactionTimeOutInstant - System.currentTimeMillis()) / 1000);
		if ( secondsRemaining <= 0 ) {
			throw new TransactionException( "transaction timeout expired" );
		}
		return secondsRemaining;
	}

}
