/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) {DATE}, Red Hat Inc. or third-party contributors as
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
package org.hibernate.test.resource.jdbc.operationspec;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.hibernate.resource.jdbc.BatchableOperationSpec;
import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec;
import org.hibernate.resource.jdbc.internal.StandardQueryPreparedStatementBuilderImpl;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchKey;
import org.hibernate.resource.jdbc.spi.BatchObserver;
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;
import org.hibernate.resource.jdbc.spi.ResultSetProcessor;
import org.hibernate.resource.jdbc.spi.StatementExecutor;

import org.junit.Assert;
import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;

import static org.hamcrest.core.Is.is;
import static org.hibernate.resource.jdbc.BatchableOperationSpec.BatchableOperationStep;

/**
 * @author Andrea Boriero
 */
public class BatchableInsertEntityWithSelectedIdIntegrationTest extends AbstractBatchableOperationSpecIntegrationTest {
	private static final int BATCH_SIZE = 10;
	private static final String INSERT_SQL = "INSERT INTO ENTITY_TABLE (PROPERTY) values (?)";
	private static final boolean FOREGO_BATCHING = false;


	@Test
	public void testInsertTableRowAndSelectinId() throws SQLException {
		final BatchableOperationStep insertStep = prepareBatchableInsertOperationStep( "123" );

		getJdbcSession().accept( prepareOperationSpec( insertStep ), buildContext() );

		getJdbcSession().executeBatch();
		commit();

		Long generatedId = (Long) insertStep.getGeneratedId();

		Assert.assertThat( generatedId, is( 1L ) );
	}

	@Override
	protected void createTables() throws SQLException {
		final String createTableSql = "create table ENTITY_TABLE (" +
				"        ID bigint generated by default as identity," +
				"        PROPERTY varchar(255)," +
				"        primary key (ID) )";
		execute( createTableSql );
	}

	@Override
	protected void dropTables() throws SQLException {
		execute( "DROP table ENTITY_TABLE IF EXISTS " );
	}

	@Override
	protected int getBatchSize() {
		return BATCH_SIZE;
	}

	private BatchableOperationSpec prepareOperationSpec(
			final BatchableOperationStep insertStep) {
		return new BatchableOperationSpec() {
			@Override
			public BatchKey getBatchKey() {
				return new BatchKeyImpl( "INSERT#ENTITY" );
			}

			@Override
			public boolean foregoBatching() {
				return FOREGO_BATCHING;
			}

			@Override
			public List<BatchObserver> getObservers() {
				return null;
			}

			@Override
			public List<BatchableOperationStep> getSteps() {
				return Arrays.asList( insertStep );
			}
		};
	}

	private BatchableOperationStep prepareBatchableInsertOperationStep(final String value) {
		return new BatchableOperationStep() {
			Long id;

			@Override
			public void apply(
					JdbcSession session, Batch batch,
					Connection connection,
					Context context)
					throws SQLException {
				final PreparedStatement statement = getStatement( batch, connection, INSERT_SQL );
				statement.setString( 1, value );
				batch.addBatch( INSERT_SQL, statement );
				id = (Long) getJdbcSession().accept( prepareSelectIdOperatioSpec() );
			}

			@Override
			public Serializable getGeneratedId() {
				return id;
			}
		};
	}

	private PreparedStatementQueryOperationSpec prepareSelectIdOperatioSpec() {
		return new PreparedStatementQueryOperationSpec() {
			@Override
			public ResultSetProcessor getResultSetProcessor() {
				return new SimpleResultSetProcessor();
			}

			@Override
			public QueryStatementBuilder<? extends PreparedStatement> getQueryStatementBuilder() {
				return new StandardQueryPreparedStatementBuilderImpl();
			}

			@Override
			public StatementExecutor getStatementExecutor() {
				return new SimpleStatementExecutor();
			}

			@Override
			public ParameterBindings getParameterBindings() {
				return new ParameterBindings() {
					@Override
					public void bindParameters(PreparedStatement statement) throws SQLException {

					}
				};
			}

			@Override
			public int getQueryTimeout() {
				return 0;
			}

			@Override
			public String getSql() {
				return "SELECT ID FROM ENTITY_TABLE";
			}

			@Override
			public ResultSetType getResultSetType() {
				return null;
			}

			@Override
			public ResultSetConcurrency getResultSetConcurrency() {
				return null;
			}

			@Override
			public int getOffset() {
				return 0;
			}

			@Override
			public int getLimit() {
				return 0;
			}

			@Override
			public boolean isCancellable() {
				return false;
			}
		};
	}

	private class SimpleStatementExecutor implements StatementExecutor {
		@Override
		public ResultSet execute(PreparedStatement statement) throws SQLException {
			return statement.executeQuery();
		}
	}

	private class SimpleResultSetProcessor implements ResultSetProcessor {
		@Override
		public Object extractResults(ResultSet resultSet) throws SQLException {
			resultSet.next();
			return resultSet.getLong( "ID" );
		}
	}
}
