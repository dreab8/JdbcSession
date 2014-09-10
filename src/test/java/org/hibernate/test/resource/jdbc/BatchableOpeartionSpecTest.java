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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.hibernate.resource.jdbc.BatchableOperationSpec;
import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.internal.BatchFactoryImpl;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;
import org.hibernate.resource.jdbc.spi.BatchKey;
import org.hibernate.resource.jdbc.spi.BatchObserver;
import org.hibernate.resource.jdbc.spi.JdbcSessionFactory;
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.StatementBuilder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.hibernate.test.resource.common.DatabaseConnectionInfo;
import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;
import org.hibernate.test.resource.jdbc.common.JdbcSessionOwnerTestingImpl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hibernate.resource.jdbc.BatchableOperationSpec.GenericOperationStep;
import static org.hibernate.resource.jdbc.BatchableOperationSpec.InsertOpertationStep;
import static org.hibernate.resource.jdbc.BatchableOperationSpec.OperationStep;
import static org.hibernate.resource.jdbc.BatchableOperationSpec.UpdateOrInsertOperationStep;

/**
 * @author Andrea Boriero
 */
public class BatchableOpeartionSpecTest {

	private static final int BATCH_SIZE = 3;
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
	public void testInheritanceJoinedStrategy() throws SQLException {
		final OperationStep step1 = new GenericOperationStep() {
			@Override
			public StatementBuilder<? extends PreparedStatement> getQueryStatementBuilder() {
				return new StatementBuilder<PreparedStatement>() {
					@Override
					public PreparedStatement buildPreparedStatement(Connection connection, String sql)
							throws SQLException {
						return connection.prepareStatement( sql );
					}
				};
			}

			@Override
			public ParameterBindings getParameterBindings() {
				return new ParameterBindings() {
					@Override
					public void bindParameters(PreparedStatement statement) throws SQLException {
						statement.setLong( 1, 1 );
						statement.setString( 2, "Fab" );
					}
				};
			}

			@Override
			public String getSql() {
				return BILLING_ADDRESS_INSERT_SQL;
			}
		};

		final OperationStep step2 = new GenericOperationStep() {
			@Override
			public StatementBuilder<? extends PreparedStatement> getQueryStatementBuilder() {
				return new StatementBuilder<PreparedStatement>() {
					@Override
					public PreparedStatement buildPreparedStatement(Connection connection, String sql)
							throws SQLException {
						return connection.prepareStatement( sql );
					}
				};
			}

			@Override
			public ParameterBindings getParameterBindings() {
				return new ParameterBindings() {
					@Override
					public void bindParameters(PreparedStatement statement) throws SQLException {
						statement.setLong( 1, 1 );
						statement.setString( 2, "0123" );
					}
				};
			}

			@Override
			public String getSql() {
				return CREDIT_CARD_INSERT_SQL;
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
					public List<OperationStep> getSteps() {
						return Arrays.asList( step1, step2 );
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
	public void testInheritanceJoinedStrategyWithgeneratedIDIdentity() throws SQLException {
		final InsertOpertationStep step1 = new InsertOpertationStep() {
			private Serializable id;

			@Override
			public StatementBuilder<? extends PreparedStatement> getQueryStatementBuilder() {
				return new StatementBuilder<PreparedStatement>() {
					@Override
					public PreparedStatement buildPreparedStatement(Connection connection, String sql)
							throws SQLException {
						return connection.prepareStatement( sql );
					}
				};
			}

			@Override
			public ParameterBindings getParameterBindings() {
				return new ParameterBindings() {
					@Override
					public void bindParameters(PreparedStatement statement) throws SQLException {
						statement.setString( 1, "123" );
					}
				};
			}

			@Override
			public String getSql() {
				return VEHICLE_INSERT_SQL;
			}

			@Override
			public void storeGeneratedId(ResultSet generatedKeys) throws SQLException {
				generatedKeys.next();
				id = generatedKeys.getLong( 1 );
			}

			@Override
			public Serializable getGeneratedId() {
				return id;
			}

		};

		final OperationStep step2 = new GenericOperationStep() {
			@Override
			public StatementBuilder<? extends PreparedStatement> getQueryStatementBuilder() {
				return new StatementBuilder<PreparedStatement>() {
					@Override
					public PreparedStatement buildPreparedStatement(Connection connection, String sql)
							throws SQLException {
						return connection.prepareStatement( sql );
					}
				};
			}

			@Override
			public ParameterBindings getParameterBindings() {
				return new ParameterBindings() {
					@Override
					public void bindParameters(PreparedStatement statement) throws SQLException {
						statement.setLong( 1, (Long) step1.getGeneratedId() );
						statement.setInt( 2, 123 );
					}
				};
			}

			@Override
			public String getSql() {
				return CAR_INSERT_SQL;
			}
		};

		jdbcSession.accept(
				new BatchableOperationSpec() {

					private BatchKey key = new BatchKeyImpl( "INSERT#CAR" );

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
					public List<OperationStep> getSteps() {
						return Arrays.asList( step1, step2 );
					}
				}
		);

		jdbcSession.executeBatch();

		Statement selectStatement = localConnection.createStatement();
		ResultSet resultSet = selectStatement.executeQuery( "SELECT * FROM Car" );

		assertThat( resultSet.next(), is( true ) );
		assertThat( resultSet.getInt( "speed" ), is( 123 ) );
		assertThat( resultSet.getLong( "car_id" ), is( step1.getGeneratedId() ) );

		localConnection.commit();
	}


	@Test
	public void testUpdateOrInsert() throws SQLException {

		PreparedStatement statement = localConnection.prepareStatement( BILLING_ADDRESS_INSERT_SQL );
		statement.setLong( 1, 1 );
		statement.setString( 2, "Fab" );
		statement.executeUpdate();
		localConnection.commit();

		final GenericOperationStep step1 = new GenericOperationStep() {
			@Override
			public StatementBuilder<? extends PreparedStatement> getQueryStatementBuilder() {
				return new StatementBuilder<PreparedStatement>() {
					@Override
					public PreparedStatement buildPreparedStatement(Connection connection, String sql)
							throws SQLException {
						return connection.prepareStatement( sql );
					}
				};
			}

			@Override
			public ParameterBindings getParameterBindings() {
				return new ParameterBindings() {
					@Override
					public void bindParameters(PreparedStatement statement) throws SQLException {
						statement.setLong( 2, 1 );
						statement.setString( 1, "noone" );
					}
				};
			}

			@Override
			public String getSql() {
				return UPDATE_BILLING_ADDRESS;
			}
		};

		final UpdateOrInsertOperationStep step2 = new UpdateOrInsertOperationStep() {
			@Override
			public StatementBuilder<? extends PreparedStatement> getQueryStatementBuilder() {
				return new StatementBuilder<PreparedStatement>() {
					@Override
					public PreparedStatement buildPreparedStatement(Connection connection, String sql)
							throws SQLException {
						return connection.prepareStatement( sql );
					}
				};
			}

			@Override
			public ParameterBindings getUpdateParameterBindings() {
				return new ParameterBindings() {
					@Override
					public void bindParameters(PreparedStatement statement) throws SQLException {
						statement.setLong( 2, 1 );
						statement.setString( 1, "0123" );
					}
				};
			}

			@Override
			public ParameterBindings getInsertParameterBindings() {
				return new ParameterBindings() {
					@Override
					public void bindParameters(PreparedStatement statement) throws SQLException {
						statement.setLong( 1, 1 );
						statement.setString( 2, "0123" );
					}
				};
			}

			@Override
			public String getUpdateSql() {
				return UPDATE_CREDIT_CARD;
			}

			@Override
			public String getInsertSql() {
				return CREDIT_CARD_INSERT_SQL;
			}
		};

		jdbcSession.accept(
				new BatchableOperationSpec() {

					private BatchKey key = new BatchKeyImpl( "UPDATE#CREDITCARD" );
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
					public List<OperationStep> getSteps() {
						return Arrays.asList( step1, step2 );
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
}
