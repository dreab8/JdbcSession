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
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.hibernate.JDBCException;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.resource.jdbc.BatchableOperationSpec;
import org.hibernate.resource.jdbc.PreparedStatementWithGeneratedKeyInsertOperationSpec;
import org.hibernate.resource.jdbc.internal.InsertWithReturnColumsStatementBuilder;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchKey;
import org.hibernate.resource.jdbc.spi.BatchObserver;
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.StatementBuilder;
import org.hibernate.tuple.GenerationTiming;

import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Andrea Boriero
 */
public class BatchableInsertEntitySpanOverMultipleTablesWithGeneratedIdIntegrationTest
		extends AbstractQueryOperationSpecIntegrationTest {
	private static final int BATCH_SIZE = 10;
	private static final String SUPERCLASS_INSERT_SQL = "INSERT INTO SUPERCLASS_TABLE (SUPERCLASS_PROPERTY) values (?)";
	private static final String SUBCLASS_INSERT_SQL = "INSERT INTO SUBCLASS_TABLE (ID, SUBCLASS_PROPERTY) values (?,?)";
	private static final boolean FOREGO_BATCHING = true;

	@Test
	public void testTheSubclassTableRowHasGeneratedIdOfTheSuperclassInsertedRow() throws SQLException {

		final BatchableOperationSpec.BatchableOperationStep insertIntoSuperclassTableStep1 = getInsertIntoSuperClassTableBatchableOperationStep(
				"123"
		);

		final BatchableOperationSpec.BatchableOperationStep insertIntoSubclassTableStep1 = getBatchableInsertIntoSubClasstableOperationStep(
				insertIntoSuperclassTableStep1,
				123
		);

		final BatchableOperationSpec.BatchableOperationStep insertIntoSuperclassTableStep2 = getInsertIntoSuperClassTableBatchableOperationStep(
				"456"
		);

		final BatchableOperationSpec.BatchableOperationStep insertIntoSubclassTableStep2 = getBatchableInsertIntoSubClasstableOperationStep(
				insertIntoSuperclassTableStep2,
				456
		);

		getJdbcSession().accept(
				getOperationSpec( insertIntoSuperclassTableStep1, insertIntoSubclassTableStep1 ), getContext()
		);
		getJdbcSession().accept(
				getOperationSpec( insertIntoSuperclassTableStep2, insertIntoSubclassTableStep2 ), getContext()
		);

		try {
			getJdbcSession().executeBatch();
			commit();

			Statement selectStatement = getLocalConnection().createStatement();
			ResultSet resultSet = selectStatement.executeQuery(
					"SELECT * FROM SUBCLASS_TABLE where id = " + insertIntoSuperclassTableStep1.getGeneratedId()
			);

			assertThat( resultSet.next(), is( true ) );
			assertThat( resultSet.getInt( "SUBCLASS_PROPERTY" ), is( 123 ) );
			assertThat( resultSet.getLong( "ID" ), is( insertIntoSuperclassTableStep1.getGeneratedId() ) );

			resultSet = selectStatement.executeQuery( "SELECT * FROM SUBCLASS_TABLE where id = " + insertIntoSuperclassTableStep2.getGeneratedId() );
			assertThat( resultSet.next(), is( true ) );
			assertThat( resultSet.getInt( "SUBCLASS_PROPERTY" ), is( 456 ) );
			assertThat( resultSet.getLong( "ID" ), is( insertIntoSuperclassTableStep2.getGeneratedId() ) );

		}
		catch (JDBCException e) {
			rollback();
			throw e;
		}
	}

	private BatchableOperationSpec.InsertContext getContext() {
		return new BatchableOperationSpec.InsertContext() {
			@Override
			public Serializable getId() {
				return null;
			}

			@Override
			public Object[] getFields() {
				return new Object[0];
			}

			@Override
			public Object getObject() {
				return null;
			}

			@Override
			public SessionImplementor getSessionImplementor() {
				return null;
			}

			@Override
			public GenerationTiming getMatchTiming() {
				return null;
			}
		};
	}

	private BatchableOperationSpec getOperationSpec(
			final BatchableOperationSpec.BatchableOperationStep insertIntoSuperclassTableStep,
			final BatchableOperationSpec.BatchableOperationStep insertIntoSubclassTableStep) {
		return new BatchableOperationSpec() {
			@Override
			public BatchKey getBatchKey() {
				return new BatchKeyImpl( "INSERT#SUBCLASS" );
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
				return Arrays.asList( insertIntoSuperclassTableStep, insertIntoSubclassTableStep );
			}
		};
	}

	private BatchableOperationSpec.BatchableOperationStep getBatchableInsertIntoSubClasstableOperationStep(
			final BatchableOperationSpec.BatchableOperationStep step, final int value) {
		return new BatchableOperationSpec.BatchableOperationStep() {
			@Override
			public void apply(Batch batch, Connection connection, BatchableOperationSpec.Context context)
					throws SQLException {
				PreparedStatement statement = batch.getStatement( SUBCLASS_INSERT_SQL );
				if ( statement == null ) {
					statement = connection.prepareStatement( SUBCLASS_INSERT_SQL );
				}
				statement.setLong( 1, (Long) step.getGeneratedId() );
				statement.setInt( 2, value );
				batch.addBatch( SUBCLASS_INSERT_SQL, statement );
			}

			@Override
			public Serializable getGeneratedId() throws SQLException {
				return null;
			}

		};
	}

	private BatchableOperationSpec.BatchableOperationStep getInsertIntoSuperClassTableBatchableOperationStep(final String value) {
		return new BatchableOperationSpec.BatchableOperationStep() {
			private Long generatedId;

			@Override
			public void apply(final Batch batch, Connection connection, BatchableOperationSpec.Context context)
					throws SQLException {

				PreparedStatementWithGeneratedKeyInsertOperationSpec.GenerateKeyResultSet generateKeyResultSet = getJdbcSession()
						.accept(
								new PreparedStatementWithGeneratedKeyInsertOperationSpec() {
									@Override
									public StatementBuilder<? extends PreparedStatement> getStatementBuilder() {
										return new InsertWithReturnColumsStatementBuilder( getColumNames() );
									}

									@Override
									public ParameterBindings getParameterBindings() {
										return new ParameterBindings() {
											@Override
											public void bindParameters(PreparedStatement statement)
													throws SQLException {
												statement.setString( 1, value );
											}
										};
									}

									@Override
									public String getSql() {
										return SUPERCLASS_INSERT_SQL;
									}

									@Override
									public String[] getColumNames() {
										String[] columnNames = {"ID"};
										return columnNames;
									}
								}
						);

				ResultSet generatedKeys = generateKeyResultSet.getGeneratedKeys();
				try {
					generatedKeys.next();
					generatedId = generatedKeys.getLong( 1 );
				}
				finally {
					generateKeyResultSet.close();
				}
			}

			@Override
			public Serializable getGeneratedId() throws SQLException {
				return generatedId;
			}
		};
	}

	@Override
	protected void createTables() throws SQLException {
		final String createSuperClassTableSql = "create table SUPERCLASS_TABLE (" +
				"        ID bigint generated by default as identity," +
				"        SUPERCLASS_PROPERTY varchar(255)," +
				"        primary key (ID) )";
		final String createSubClassTableSql = "create table SUBCLASS_TABLE (" +
				"        ID bigint not null," +
				"        SUBCLASS_PROPERTY varchar(255)," +
				"        primary key (ID) )";
		final String addForeignkeyToSubclassTableSql = "alter table SUBCLASS_TABLE " +
				"        add constraint FK_ldlwoj0fv2uvxg7dxkvs7nn4g " +
				"        foreign key (ID) " +
				"        references SUPERCLASS_TABLE";

		execute( createSuperClassTableSql );
		execute( createSubClassTableSql );
		execute( addForeignkeyToSubclassTableSql );
	}

	@Override
	protected void dropTables() throws SQLException {
		execute( "DROP table SUPERCLASS_TABLE IF EXISTS " );
		execute( "DROP table SUBCLASS_TABLE IF EXISTS" );
	}

	@Override
	protected int getBatchSize() {
		return BATCH_SIZE;
	}
}
