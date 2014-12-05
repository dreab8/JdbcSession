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
import org.hibernate.resource.jdbc.BatchableOperationSpec;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchKey;
import org.hibernate.resource.jdbc.spi.BatchObserver;

import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hibernate.resource.jdbc.BatchableOperationSpec.BatchableOperationStep;

/**
 * @author Andrea Boriero
 */
public class BatchableUpdateEntityWithOptionalTablesIntegrationTest extends
																	AbstractBatchableOperationSpecIntegrationTest {
	private static final int BATCH_SIZE = 10;

	private static final String OPTIONAL_TABLE_INSERT_SQL = "INSERT INTO OPTIONAL_TABLE (ID, OPTIONAL_VALUE) values  (?,?)";
	public static final String UPDATE_BASE_TABLE_SQL = "UPDATE BASE_TABLE set BASE_PROPERTY = ? where ID = ? ";
	public static final String UPDATE_OPTIONAL_TABLE_SQL = "UPDATE OPTIONAL_TABLE set OPTIONAL_VALUE = ? where ID = ?";

	@Test
	public void testTheRowOfTheOptionalTableIsInsertedWhenNotPresent() throws Exception {
		final int id = 1;
		final String baseProperty = "old";
		final String newBaseProperty = "new";
		final String optionalValue = "0123";

		insertIntoBaseTable( id, baseProperty );

		final BatchableOperationStep updateSuperclassTableStep = new BatchableOperationStep() {
			@Override
			public void apply(
					Batch batch,
					Connection connection,
					BatchableOperationSpec.Context context)
					throws SQLException {
				final PreparedStatement statement = getStatement( batch, connection, UPDATE_BASE_TABLE_SQL );
				statement.setLong( 2, id );
				statement.setString( 1, newBaseProperty );
				batch.addBatch( UPDATE_BASE_TABLE_SQL, statement );
			}

			@Override
			public Serializable getGeneratedId() {
				return 0;
			}
		};

		final BatchableOperationStep insertIntoOptionalTableStep = new BatchableOperationStep() {
			@Override
			public void apply(
					Batch batch,
					Connection connection,
					BatchableOperationSpec.Context context)
					throws SQLException {
				PreparedStatement statement = getStatement( batch, connection, UPDATE_OPTIONAL_TABLE_SQL );

				statement.setString( 1, optionalValue );
				statement.setLong( 2, id );

				batch.addBatch( UPDATE_OPTIONAL_TABLE_SQL, statement );

				performInsertIfNoRowUpdated( batch, connection );
			}

			@Override
			public Serializable getGeneratedId() {
				return 0;
			}

			private void performInsertIfNoRowUpdated(Batch batch, Connection connection) throws SQLException {
				Integer rowCount = batch.getRowCount( UPDATE_OPTIONAL_TABLE_SQL );

				if ( rowCount == 0 ) {
					final PreparedStatement statement = getStatement( batch, connection, OPTIONAL_TABLE_INSERT_SQL );
					statement.setLong( 1, id );
					statement.setString( 2, optionalValue );
					batch.addBatch( OPTIONAL_TABLE_INSERT_SQL, statement );
				}
			}
		};

		getJdbcSession().accept(
				new BatchableOperationSpec() {
					@Override
					public BatchKey getBatchKey() {
						return new BatchKeyImpl( "UPDATE#CREDITCARD" );
					}

					@Override
					public boolean foregoBatching() {
						/*
						 Cannot be batched because it is necessary to check the result of the update operation
						 to decide id an insert is needed
						  */
						return false;
					}

					@Override
					public List<BatchObserver> getObservers() {
						return null;
					}

					@Override
					public List<BatchableOperationStep> getSteps() {
						return Arrays.asList( updateSuperclassTableStep, insertIntoOptionalTableStep );
					}
				}, new BatchableOperationSpec.UpdateContext() {
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
					public int[] getDirtyFields() {
						return new int[0];
					}

					@Override
					public boolean isDirtyCollection() {
						return false;
					}

					@Override
					public Object[] getOldFields() {
						return new Object[0];
					}

					@Override
					public Object getOldVersion() {
						return null;
					}

					@Override
					public Object getrowId() {
						return null;
					}
				}

		);

		try {
			getJdbcSession().executeBatch();
			commit();

			checkBaseTableIsUpdated( id, newBaseProperty );
			checkRowIsInsertedIntoOptionalTable( id, optionalValue );
		}
		catch (JDBCException e) {
			rollback();
			throw e;
		}
	}

	@Test
	public void testTheRowOfTheOptionalTableIsUpdatedWhenPresent() throws Exception {
		final int id = 1;
		final String baseProperty = "Fab";
		final String newBaseProeprty = "Fab_new";
		final String optionalValue = "0123";
		final String newOptionalValue = "123";

		insertIntoBaseTable( id, baseProperty );
		insertIntoOptionalTable( id, optionalValue );

		final BatchableOperationStep updateSuperclassTableStep = new BatchableOperationStep() {
			@Override
			public void apply(
					Batch batch,
					Connection connection,
					BatchableOperationSpec.Context context)
					throws SQLException {
				final PreparedStatement statement = getStatement( batch, connection, UPDATE_BASE_TABLE_SQL );
				statement.setLong( 2, id );
				statement.setString( 1, newBaseProeprty );
				batch.addBatch( UPDATE_BASE_TABLE_SQL, statement );
			}

			@Override
			public Serializable getGeneratedId() {
				return 0;
			}
		};

		final BatchableOperationStep insertIntoOptionalTableStep = new BatchableOperationStep() {
			@Override
			public void apply(
					Batch batch,
					Connection connection,
					BatchableOperationSpec.Context context)
					throws SQLException {
				final PreparedStatement statement = getStatement( batch, connection, UPDATE_OPTIONAL_TABLE_SQL );

				statement.setString( 1, newOptionalValue );
				statement.setLong( 2, id );

				batch.addBatch( UPDATE_OPTIONAL_TABLE_SQL, statement );

				performUpdateNoRowUpdated( batch, connection );
			}

			@Override
			public Serializable getGeneratedId() {
				return 0;
			}

			private void performUpdateNoRowUpdated(Batch batch, Connection connection) throws SQLException {
				Integer rowCount = batch.getRowCount( UPDATE_OPTIONAL_TABLE_SQL );

				if ( rowCount == 0 ) {
					final PreparedStatement statement = getStatement( batch, connection, OPTIONAL_TABLE_INSERT_SQL );
					statement.setLong( 1, id );
					statement.setString( 2, newOptionalValue );
					batch.addBatch( OPTIONAL_TABLE_INSERT_SQL, statement );
				}
			}
		};

		getJdbcSession().accept(
				new BatchableOperationSpec() {
					@Override
					public BatchKey getBatchKey() {
						return new BatchKeyImpl( "UPDATE#CREDITCARD" );
					}

					@Override
					public boolean foregoBatching() {
						/*
						 Cannot be batched because it is necessary to check the result of the update operation
						 to decide id an insert is needed
						  */
						return false;
					}

					@Override
					public List<BatchObserver> getObservers() {
						return null;
					}

					@Override
					public List<BatchableOperationStep> getSteps() {
						return Arrays.asList( updateSuperclassTableStep, insertIntoOptionalTableStep );
					}
				}, new BatchableOperationSpec.UpdateContext() {
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
					public int[] getDirtyFields() {
						return new int[0];
					}

					@Override
					public boolean isDirtyCollection() {
						return false;
					}

					@Override
					public Object[] getOldFields() {
						return new Object[0];
					}

					@Override
					public Object getOldVersion() {
						return null;
					}

					@Override
					public Object getrowId() {
						return null;
					}
				}
		);

		try {
			getJdbcSession().executeBatch();
			commit();

			checkBaseTableIsUpdated( id, newBaseProeprty );
			checkRowIsInsertedIntoOptionalTable( id, newOptionalValue );
		}
		catch (JDBCException e) {
			rollback();
			throw e;
		}
	}

	@Test
	public void testTheRowIdDeletedFromTheOptionalTableWhenTheNewOptionalValueIsNull() throws Exception {
		final int id = 1;
		final String baseProperty = "Fab";
		final String newBaseProeprty = "Fab_new";
		final String optionalValue = "123";

		insertIntoBaseTable( id, baseProperty );
		insertIntoOptionalTable( id, optionalValue );

		final BatchableOperationStep updateSuperclassTableStep = new BatchableOperationStep() {
			@Override
			public void apply(
					Batch batch,
					Connection connection,
					BatchableOperationSpec.Context context)
					throws SQLException {
				final PreparedStatement statement = getStatement( batch, connection, UPDATE_BASE_TABLE_SQL );
				statement.setLong( 2, id );
				statement.setString( 1, newBaseProeprty );
				batch.addBatch( UPDATE_BASE_TABLE_SQL, statement );
			}

			@Override
			public Serializable getGeneratedId() {
				return null;
			}
		};

		final BatchableOperationStep deleteRowFormOptionalTableStep = new BatchableOperationStep() {
			@Override
			public void apply(
					Batch batch,
					Connection connection,
					BatchableOperationSpec.Context context)
					throws SQLException {
				final PreparedStatement statement = getStatement(
						batch,
						connection,
						"DELETE FROM OPTIONAL_TABLE WHERE ID = ?"
				);

				statement.setLong( 1, id );
				batch.addBatch( "DELETE FROM OPTIONAL_TABLE WHERE ID = ?", statement );
			}

			@Override
			public Serializable getGeneratedId() {
				return null;
			}
		};

		getJdbcSession().accept(
				new BatchableOperationSpec() {
					@Override
					public BatchKey getBatchKey() {
						return new BatchKeyImpl( "UPDATE#CREDITCARD" );
					}

					@Override
					public boolean foregoBatching() {
						return true;
					}

					@Override
					public List<BatchObserver> getObservers() {
						return null;
					}

					@Override
					public List<BatchableOperationStep> getSteps() {
						return Arrays.asList( updateSuperclassTableStep, deleteRowFormOptionalTableStep );
					}
				}, new BatchableOperationSpec.UpdateContext() {
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
					public int[] getDirtyFields() {
						return new int[0];
					}

					@Override
					public boolean isDirtyCollection() {
						return false;
					}

					@Override
					public Object[] getOldFields() {
						return new Object[0];
					}

					@Override
					public Object getOldVersion() {
						return null;
					}

					@Override
					public Object getrowId() {
						return null;
					}
				}
		);

		try {
			getJdbcSession().executeBatch();
			commit();

			checkRowIsDeletedFromOptionalTable( id );

			checkBaseTableIsUpdated( id, newBaseProeprty );
		}
		catch (JDBCException e) {
			rollback();
			throw e;
		}
	}

	private void checkBaseTableIsUpdated(int id, String newBaseProeprty) throws SQLException {
		Statement selectStatement = getLocalConnection().createStatement();
		ResultSet resultSet = selectStatement.executeQuery(
				"SELECT * FROM BASE_TABLE b where b.ID = " + id
		);
		resultSet.next();
		assertThat( resultSet.getString( "BASE_PROPERTY" ), is( newBaseProeprty ) );
	}

	private void checkRowIsDeletedFromOptionalTable(int id) throws SQLException {
		Statement selectStatement = getLocalConnection().createStatement();
		ResultSet resultSet = selectStatement.executeQuery(
				"SELECT * FROM OPTIONAL_TABLE o where o.ID = " + id
		);

		assertThat( resultSet.next(), is( false ) );
	}

	private void checkRowIsInsertedIntoOptionalTable(int id, String optionalValue) throws SQLException {
		Statement selectStatement = getLocalConnection().createStatement();
		ResultSet resultSet = selectStatement.executeQuery(
				"SELECT * FROM OPTIONAL_TABLE o where o.ID = " + id
		);

		assertThat( resultSet.next(), is( true ) );
		assertThat( resultSet.getString( "OPTIONAL_VALUE" ), is( optionalValue ) );
	}

	private void insertIntoOptionalTable(int id, String oldOptionalValue) throws Exception {
		final String OPTIONAL_TABLE_INSERT_SQL = "INSERT INTO OPTIONAL_TABLE (ID, OPTIONAL_VALUE) values (?,?)";
		PreparedStatement statement = getLocalConnection().prepareStatement( OPTIONAL_TABLE_INSERT_SQL );
		statement.setLong( 1, id );
		statement.setString( 2, oldOptionalValue );
		statement.executeUpdate();
		commit();
	}


	private void insertIntoBaseTable(int id, String baseTableProperty) throws Exception {
		final String BASE_TABLE_INSERT_SQL = "INSERT INTO BASE_TABLE (ID, BASE_PROPERTY) values  (?,?)";

		PreparedStatement statement = getLocalConnection().prepareStatement( BASE_TABLE_INSERT_SQL );
		statement.setLong( 1, id );
		statement.setString( 2, baseTableProperty );
		statement.executeUpdate();
		commit();
	}

	@Override
	protected void dropTables() throws SQLException {
		execute( "DROP table BASE_TABLE IF EXISTS " );
		execute( "DROP table OPTIONAL_TABLE IF EXISTS" );
	}

	@Override
	protected int getBatchSize() {
		return BATCH_SIZE;
	}

	@Override
	protected void createTables() throws SQLException {
		String createCreditCardTableSql = "create table BASE_TABLE (" +
				"        ID bigint not null," +
				"        BASE_PROPERTY varchar(255)," +
				"        primary key (ID) )";
		String createBillingAddressTableSql = "create table OPTIONAL_TABLE (" +
				"        ID bigint not null," +
				"        OPTIONAL_VALUE varchar(255)," +
				"        primary key (ID) )";
		String addForeignLeyToCreditCardTableSql = "alter table OPTIONAL_TABLE " +
				"        add constraint FK_ldlwoj0fv2uvxg7dxkvs7nn4g " +
				"        foreign key (ID) " +
				"        references BASE_TABLE";

		execute( createCreditCardTableSql );
		execute( createBillingAddressTableSql );
		execute( addForeignLeyToCreditCardTableSql );
	}
}
