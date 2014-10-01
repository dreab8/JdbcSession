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
package org.hibernate.test.resource.jdbc;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Inheritance;
import javax.persistence.InheritanceType;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.hibernate.annotations.Generated;
import org.hibernate.resource.jdbc.BatchableOperationSpec;
import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.PreparedStatementInsertOperationSpec;
import org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec;
import org.hibernate.resource.jdbc.internal.BatchFactoryImpl;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchKey;
import org.hibernate.resource.jdbc.spi.BatchObserver;
import org.hibernate.resource.jdbc.spi.JdbcSessionFactory;
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;
import org.hibernate.resource.jdbc.spi.ResultSetProcessor;
import org.hibernate.resource.jdbc.spi.StatementBuilder;
import org.hibernate.resource.jdbc.spi.StatementExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.hibernate.test.resource.common.DatabaseConnectionInfo;
import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;
import org.hibernate.test.resource.jdbc.common.JdbcSessionOwnerTestingImpl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hibernate.annotations.GenerationTime.INSERT;
import static org.hibernate.resource.jdbc.BatchableOperationSpec.BatchableOperationStep;
import static org.hibernate.resource.jdbc.PreparedStatementInsertOperationSpec.GenerateKeyResultSet;

/**
 * @author Andrea Boriero
 */
public class BatchableOpeartionSpecStepTest {

	private static final int BATCH_SIZE = 3;
	private static final Long DEFAULT_SECURITY_CODE = 123L;
	private static final String VEHICLE_INSERT_SQL = "INSERT INTO Vehicle ( serialNumber) values  (?)";
	private static final String CAR_INSERT_SQL = "INSERT INTO Car (car_id, speed) values  (?,?)";

	private static final String BILLING_ADDRESS_INSERT_SQL = "INSERT INTO BillingAddress (ADDRESS_ID, owner) values  (?,?)";
	private static final String CREDIT_CARD_INSERT_SQL = "INSERT INTO CreditCard (ADDRESS_ID, number) values  (?,?)";
	public static final String UPDATE_BILLING_ADDRESS = "UPDATE BillingAddress set owner = ? where ADDRESS_ID = ? ";
	public static final String UPDATE_CREDIT_CARD = "UPDATE CreditCard set number = ? where ADDRESS_ID = ?";

	private JdbcSession jdbcSession;
	private Connection localConnection;

	@Before
	public void setUp() throws SQLException {
		localConnection = DatabaseConnectionInfo.INSTANCE.makeConnection();
		localConnection.setAutoCommit( false );
		createTables();
		jdbcSession = createJdbSession();
	}

	@After
	public void tearDown() throws SQLException {
		dropTables();
	}

	@Test
	public void testStepsForInsertEntityWithInheritanceJoinedStrategy() throws SQLException {

		final Serializable id = 1L;

		final BatchableOperationStep insertIntoSuperclassTable = new BatchableOperationStep() {
			@Override
			public void apply(Batch batch, Connection connection) throws SQLException {
				PreparedStatement statement = batch.getStatement( BILLING_ADDRESS_INSERT_SQL );
				if ( statement == null ) {
					statement = connection.prepareStatement( BILLING_ADDRESS_INSERT_SQL );
				}
				statement.setLong( 1, (Long) id );
				statement.setString( 2, "Fab" );
				batch.addBatch( BILLING_ADDRESS_INSERT_SQL, statement );
			}

			@Override
			public long getGeneratedId() throws SQLException {
				return 0;
			}

		};

		final BatchableOperationStep insertIntoSubclassTable = new BatchableOperationStep() {
			@Override
			public void apply(Batch batch, Connection connection) throws SQLException {
				PreparedStatement statement = batch.getStatement( CREDIT_CARD_INSERT_SQL );
				if ( statement == null ) {
					statement = connection.prepareStatement( CREDIT_CARD_INSERT_SQL );
				}
				statement.setLong( 1, (Long) id );
				statement.setString( 2, "0123" );
				batch.addBatch( CREDIT_CARD_INSERT_SQL, statement );
			}

			@Override
			public long getGeneratedId() throws SQLException {
				return 0;
			}

		};

		jdbcSession.accept(
				new BatchableOperationSpec() {

					private BatchKey key = new BatchKeyImpl( "INSERT#CREDITCARD" );
					private boolean foregoBatching = true;

					@Override
					public BatchKey getBatchKey() {
						return key;
					}

					@Override
					public boolean foregoBatching() {
						return foregoBatching;
					}

					@Override
					public List<BatchObserver> getObservers() {
						return null;
					}

					@Override
					public List<BatchableOperationStep> getSteps() {
						return Arrays.asList( insertIntoSuperclassTable, insertIntoSubclassTable );
					}
				}
		);

		jdbcSession.executeBatch();

		Statement selectStatement = localConnection.createStatement();
		ResultSet resultSet = selectStatement.executeQuery( "SELECT * FROM CreditCard" );

		assertThat( resultSet.next(), is( true ) );
		assertThat( resultSet.getLong( "ADDRESS_ID" ), is( 1l ) );

		localConnection.commit();
	}

	@Test
	public void testStepsForInsertEntityWithInheritanceJoinedStrategyAndGenerateValues() throws SQLException {
		final CreditCard creditCard = new CreditCard();
		creditCard.setNumber( "123" );
		creditCard.setOwner( "drea" );

		final Serializable entityId = 1L;
		final String sqlUpdateGeneratedValuesSelectString = "select creditcard_.securityCode as securityCode_ " +
				"from CreditCard creditcard_ inner join BillingAddress creditcard_1_ " +
				"#on creditcard_.ADDRESS_ID=creditcard_1_.ADDRESS_ID " +
				"where creditcard_.ADDRESS_ID=?";

		final BatchableOperationStep insertIntoSuperclassTable = new BatchableOperationStep() {
			@Override
			public void apply(Batch batch, Connection connection) throws SQLException {
				PreparedStatement statement = batch.getStatement( BILLING_ADDRESS_INSERT_SQL );
				if ( statement == null ) {
					statement = connection.prepareStatement( BILLING_ADDRESS_INSERT_SQL );
				}
				statement.setLong( 1, (Long) entityId );
				statement.setString( 2, "Fab" );
				batch.addBatch( BILLING_ADDRESS_INSERT_SQL, statement );
			}

			@Override
			public long getGeneratedId() throws SQLException {
				return 0;
			}

		};

		final BatchableOperationStep insertIntoSubclassTable = new BatchableOperationStep() {
			@Override
			public void apply(Batch batch, Connection connection) throws SQLException {
				PreparedStatement statement = batch.getStatement( CREDIT_CARD_INSERT_SQL );
				if ( statement == null ) {
					statement = connection.prepareStatement( CREDIT_CARD_INSERT_SQL );
				}
				statement.setLong( 1, (Long) entityId );
				statement.setString( 2, "0123" );
				batch.addBatch( CREDIT_CARD_INSERT_SQL, statement );
			}

			@Override
			public long getGeneratedId() throws SQLException {
				return 0;
			}

		};

		final BatchableOperationStep updateSubclassEntityWithGeneratedValue = new BatchableOperationStep() {
			@Override
			public void apply(Batch batch, Connection connection) throws SQLException {

				PreparedStatementQueryOperationSpec spec = new PreparedStatementQueryOperationSpec() {
					@Override
					public ResultSetProcessor getResultSetProcessor() {
						return new ResultSetProcessor() {
							@Override
							public Object extractResults(
									ResultSet resultSet) throws SQLException {
								resultSet.next();
								long securityCode = resultSet.getLong( "securityCode" );
								creditCard.setSecurityCode( securityCode );
								return creditCard;
							}
						};
					}

					@Override
					public QueryStatementBuilder<? extends PreparedStatement> getQueryStatementBuilder() {
						return new QueryStatementBuilder<PreparedStatement>() {
							@Override
							public PreparedStatement buildQueryStatement(
									Connection connection,
									String sql,
									ResultSetType resultSetType,
									ResultSetConcurrency resultSetConcurrency) throws SQLException {
								PreparedStatement statement = connection.prepareStatement(
										sql,
										resultSetType.getJdbcConstantValue(),
										resultSetConcurrency.getJdbcConstantValue()
								);
								return statement;
							}
						};
					}

					@Override
					public StatementExecutor getStatementExecutor() {
						return new StatementExecutor() {
							@Override
							public ResultSet execute(
									PreparedStatement statement) throws SQLException {
								return statement.executeQuery();
							}
						};
					}

					@Override
					public ParameterBindings getParameterBindings() {
						return new ParameterBindings() {
							@Override
							public void bindParameters(PreparedStatement statement) throws SQLException {
								statement.setLong( 1, (Long) entityId );
							}
						};
					}

					@Override
					public int getQueryTimeout() {
						return 0;
					}

					@Override
					public String getSql() {
						return sqlUpdateGeneratedValuesSelectString;
					}

					@Override
					public ResultSetType getResultSetType() {
						return ResultSetType.FORWARD_ONLY;
					}

					@Override
					public ResultSetConcurrency getResultSetConcurrency() {
						return ResultSetConcurrency.READ_ONLY;
					}

					@Override
					public int getOffset() {
						return 0;
					}

					@Override
					public int getLimit() {
						return 0;
					}
				};

				jdbcSession.accept( spec );
			}

			@Override
			public long getGeneratedId() throws SQLException {
				return 0;
			}
		};

		jdbcSession.accept(
				new BatchableOperationSpec() {

					private BatchKey key = new BatchKeyImpl( "INSERT#CREDITCARD" );
					private boolean foregoBatching = false;

					@Override
					public BatchKey getBatchKey() {
						return key;
					}

					@Override
					public boolean foregoBatching() {
						return foregoBatching;
					}

					@Override
					public List<BatchObserver> getObservers() {
						return null;
					}

					@Override
					public List<BatchableOperationStep> getSteps() {
						return Arrays.asList( insertIntoSuperclassTable, insertIntoSubclassTable, updateSubclassEntityWithGeneratedValue );
					}
				}
		);

		jdbcSession.executeBatch();
		localConnection.commit();

		assertThat( creditCard.getSecurityCode(), is( DEFAULT_SECURITY_CODE ) );
	}

	@Test
	public void testStepsForInsertEntityWithInheritanceJoinedStrategyAndGeneratedIDIdentity() throws SQLException {

		final BatchableOperationStep insertIntoSuperclassTable = new BatchableOperationStep() {
			private Long id;

			@Override
			public void apply(Batch batch, Connection connection) throws SQLException {

				GenerateKeyResultSet generateKeyResultSet = jdbcSession.accept(
						new PreparedStatementInsertOperationSpec() {
							@Override
							public StatementBuilder<? extends PreparedStatement> getStatementBuilder() {
								return new StatementBuilder<PreparedStatement>() {
									@Override
									public PreparedStatement buildQueryStatement(
											Connection connection,
											String sql) throws SQLException {
										return connection.prepareStatement( sql );
									}
								};
							}

							@Override
							public ParameterBindings getParameterBindings() {
								return new ParameterBindings() {
									@Override
									public void bindParameters(PreparedStatement statement)
											throws SQLException {
										statement.setString( 1, "123" );
									}
								};
							}

							@Override
							public String getSql() {
								return VEHICLE_INSERT_SQL;
							}
						}
				);

				ResultSet generatedKeys = generateKeyResultSet.getGeneratedKeys();
				generatedKeys.next();
				id = generatedKeys.getLong( 1 );
				generateKeyResultSet.close();
			}

			@Override
			public long getGeneratedId() throws SQLException {
				return id;
			}

		};

		final BatchableOperationStep insertIntoSubclassTable = new BatchableOperationStep() {
			@Override
			public void apply(Batch batch, Connection connection) throws SQLException {
				PreparedStatement statement = batch.getStatement( CAR_INSERT_SQL );
				if ( statement == null ) {
					statement = connection.prepareStatement( CAR_INSERT_SQL );
				}
				statement.setLong( 1, insertIntoSuperclassTable.getGeneratedId() );
				statement.setInt( 2, 123 );
				batch.addBatch( CAR_INSERT_SQL, statement );
			}

			@Override
			public long getGeneratedId() throws SQLException {
				return 0;
			}

		};

		jdbcSession.accept(
				new BatchableOperationSpec() {

					private BatchKey key = new BatchKeyImpl( "INSERT#CAR" );

					private boolean foregoBatching = false;

					@Override
					public BatchKey getBatchKey() {
						return key;
					}

					@Override
					public boolean foregoBatching() {
						return foregoBatching;
					}

					@Override
					public List<BatchObserver> getObservers() {
						return null;
					}

					@Override
					public List<BatchableOperationStep> getSteps() {
						return Arrays.asList( insertIntoSuperclassTable, insertIntoSubclassTable );
					}
				}
		);

		jdbcSession.executeBatch();

		Statement selectStatement = localConnection.createStatement();
		ResultSet resultSet = selectStatement.executeQuery( "SELECT * FROM Car" );

		assertThat( resultSet.next(), is( true ) );
		assertThat( resultSet.getInt( "speed" ), is( 123 ) );
		assertThat( resultSet.getLong( "car_id" ), is( insertIntoSuperclassTable.getGeneratedId() ) );

		localConnection.commit();
	}

	@Test
	public void testStepsForUpdateEntityWithInheritance() throws SQLException {

		PreparedStatement statement = localConnection.prepareStatement( BILLING_ADDRESS_INSERT_SQL );
		statement.setLong( 1, 1 );
		statement.setString( 2, "Fab" );
		statement.executeUpdate();
		localConnection.commit();

		final BatchableOperationStep updateSuperclassTable = new BatchableOperationStep() {
			@Override
			public void apply(Batch batch, Connection connection) throws SQLException {
				PreparedStatement statement = batch.getStatement( UPDATE_BILLING_ADDRESS );
				if ( statement == null ) {
					statement = connection.prepareStatement( UPDATE_BILLING_ADDRESS );
				}
				statement.setLong( 2, 1 );
				statement.setString( 1, "noone" );
				batch.addBatch( UPDATE_BILLING_ADDRESS, statement );
			}

			@Override
			public long getGeneratedId() throws SQLException {
				return 0;
			}

		};

		final BatchableOperationStep insertorUpdateIntoSubclassTable = new BatchableOperationStep() {
			@Override
			public void apply(Batch batch, Connection connection) throws SQLException {
				PreparedStatement statement = batch.getStatement( UPDATE_CREDIT_CARD );
				if ( statement == null ) {
					statement = connection.prepareStatement( UPDATE_CREDIT_CARD );
				}
				statement.setLong( 2, 1 );
				statement.setString( 1, "0123" );
				batch.addBatch( UPDATE_CREDIT_CARD, statement );

				Integer rowCount = batch.getRowCount( UPDATE_CREDIT_CARD );

				if ( rowCount == 0 ) {
					statement = batch.getStatement( CREDIT_CARD_INSERT_SQL );
					if ( statement == null ) {
						statement = connection.prepareStatement( CREDIT_CARD_INSERT_SQL );
					}
					statement.setLong( 1, 1 );
					statement.setString( 2, "0123" );
					batch.addBatch( CREDIT_CARD_INSERT_SQL, statement );
				}
			}

			@Override
			public long getGeneratedId() throws SQLException {
				return 0;
			}

		};

		jdbcSession.accept(
				new BatchableOperationSpec() {

					private BatchKey key = new BatchKeyImpl( "UPDATE#CREDITCARD" );
					private boolean foregoBatching = false;

					@Override
					public BatchKey getBatchKey() {
						return key;
					}

					@Override
					public boolean foregoBatching() {
						return foregoBatching;
					}

					@Override
					public List<BatchObserver> getObservers() {
						return null;
					}

					@Override
					public List<BatchableOperationStep> getSteps() {
						return Arrays.asList( updateSuperclassTable, insertorUpdateIntoSubclassTable );
					}
				}
		);

		jdbcSession.executeBatch();

		Statement selectStatement = localConnection.createStatement();
		ResultSet resultSet = selectStatement.executeQuery(
				"SELECT * FROM BillingAddress b left join CreditCard c on b.address_Id = c.Address_Id"
		);

		assertThat( resultSet.next(), is( true ) );
		assertThat( resultSet.getString( "number" ), is( "0123" ) );
		assertThat( resultSet.getString( "owner" ), is( "noone" ) );

		localConnection.commit();
	}

	private void dropTables() throws SQLException {
		execute( "DROP table CreditCard IF EXISTS " );
		execute( "DROP table BillingAddress IF EXISTS" );

		execute( "DROP table Car IF EXISTS" );
		execute( "DROP table Vehicle IF EXISTS" );
	}

	private void createTables() throws SQLException {
		String createCreditCardTableSql = "create table CreditCard (" +
				"        ADDRESS_ID bigint not null," +
				"        number varchar(255)," +
				"		 securityCode integer default " + DEFAULT_SECURITY_CODE + " ," +
				"        primary key (ADDRESS_ID) )";
		String createBillingAddressTableSql = "create table BillingAddress (" +
				"        ADDRESS_ID bigint not null," +
				"        owner varchar(255)," +
				"        primary key (ADDRESS_ID) )";
		String addForeignLeyToCreditCardTableSql = "alter table CreditCard " +
				"        add constraint FK_ldlwoj0fv2uvxg7dxkvs7nn4g " +
				"        foreign key (ADDRESS_ID) " +
				"        references BillingAddress";

		String createVehicleTableSql = "create table Vehicle (" +
				"        id bigint generated by default as identity," +
				"        serialNumber varchar(255)," +
				"        primary key (id) )";

		String createCarTableSql = "create table Car (" +
				"        car_id bigint not null," +
				"        speed integer," +
				"        primary key (car_id) )";

		String addForeignLeyToCarTableSql = "alter table Car " +
				"        add constraint FK_ldlwoj0fv2uvxg7dxkvs7nn5g " +
				"        foreign key (car_id) " +
				"        references Vehicle";

		execute( createCreditCardTableSql );
		execute( createBillingAddressTableSql );
		execute( addForeignLeyToCreditCardTableSql );

		execute( createVehicleTableSql );
		execute( createCarTableSql );
		execute( addForeignLeyToCarTableSql );
	}

	private void execute(String sql) throws SQLException {
		PreparedStatement createCreditCardTable = localConnection.prepareStatement( sql );
		createCreditCardTable.execute();
	}

	private JdbcSession createJdbSession() {
		JdbcSessionOwnerTestingImpl JDBC_SESSION_OWNER = new JdbcSessionOwnerTestingImpl();
		JDBC_SESSION_OWNER.setBatchFactory( new BatchFactoryImpl( BATCH_SIZE ) );
		return JdbcSessionFactory.INSTANCE.create( JDBC_SESSION_OWNER, new ResourceRegistryStandardImpl() );
	}

	@Entity
	@Inheritance(strategy = InheritanceType.JOINED)
	public abstract class BillingAddress {

		@Id
		@GeneratedValue(strategy = GenerationType.SEQUENCE)
		protected Long id;

		Long amount;

		protected String owner;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getOwner() {
			return owner;
		}

		public void setOwner(String owner) {
			this.owner = owner;
		}

		public Long getAmount() {
			return amount;
		}

		public void setAmount(Long amount) {
			this.amount = amount;
		}
	}

	@Entity
	public class CreditCard extends BillingAddress {
		private String number;

		@Generated(INSERT)
		@Column(insertable = false)
		Long securityCode;

		public String getNumber() {
			return number;
		}

		public void setNumber(String number) {
			this.number = number;
		}

		public Long getSecurityCode() {
			return securityCode;
		}

		public void setSecurityCode(Long securityCode) {
			this.securityCode = securityCode;
		}
	}


}
