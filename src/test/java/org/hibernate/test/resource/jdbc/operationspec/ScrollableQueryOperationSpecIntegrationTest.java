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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.hibernate.JDBCException;
import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.QueryOperationSpec;
import org.hibernate.resource.jdbc.ScrollableQueryOperationSpec;
import org.hibernate.resource.jdbc.internal.BatchFactoryImpl;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;
import org.hibernate.resource.jdbc.spi.JdbcSessionFactory;
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;
import org.hibernate.resource.jdbc.spi.StatementExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.hibernate.test.resource.common.DatabaseConnectionInfo;
import org.hibernate.test.resource.jdbc.common.JdbcSessionOwnerTestingImpl;

import static org.hamcrest.core.Is.is;
import static org.hibernate.resource.jdbc.ScrollableQueryOperationSpec.Result;
import static org.junit.Assert.assertThat;

/**
 * @author Andrea Boriero
 */
public class ScrollableQueryOperationSpecIntegrationTest {
	private static final String BILLING_ADDRESS_INSERT_SQL = "INSERT INTO BillingAddress (ADDRESS_ID, owner) values  (1,'owner')";

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
		jdbcSession.close();
	}

	@Test
	public void testTheResultSetContainsTheExpectedValues() throws SQLException {
		execute( BILLING_ADDRESS_INSERT_SQL );

		ScrollableQueryOperationSpec operationSpec = new ScrollableQueryOperationSpec() {
			@Override
			public QueryStatementBuilder<? extends PreparedStatement> getQueryStatementBuilder() {
				return new SimpleStatementBuilder();
			}

			@Override
			public StatementExecutor getStatementExecutor() {
				return new SimpleStatementExecutor();
			}

			@Override
			public ParameterBindings getParameterBindings() {
				return new SimpleParameterBindings();
			}

			@Override
			public int getQueryTimeout() {
				return 0;
			}

			@Override
			public String getSql() {
				return "Select ADDRESS_ID, owner from  BillingAddress where ADDRESS_ID = ?";
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

		Result result = null;

		try {
			result = jdbcSession.accept( operationSpec );
			ResultSet resultSet = result.getResultSet();
			localConnection.commit();

			assertThat( resultSet.next(), is( true ) );
			assertThat( resultSet.getLong( "ADDRESS_ID" ), is( 1L ) );
		}
		catch (JDBCException e) {
			localConnection.rollback();
			throw e;
		}
		finally {
			if ( result != null ) {
				result.close();
			}
		}
	}

	private class SimpleStatementBuilder implements QueryStatementBuilder {

		@Override
		public Statement buildQueryStatement(
				Connection connection,
				String sql,
				QueryOperationSpec.ResultSetType resultSetType,
				QueryOperationSpec.ResultSetConcurrency resultSetConcurrency) throws SQLException {
			return connection.prepareStatement(
					sql,
					resultSetType.getJdbcConstantValue(),
					resultSetConcurrency.getJdbcConstantValue()
			);
		}
	}

	private class SimpleParameterBindings implements ParameterBindings {

		@Override
		public void bindParameters(PreparedStatement statement) throws SQLException {
			statement.setLong( 1, 1 );
		}
	}

	private class SimpleStatementExecutor implements StatementExecutor {
		@Override
		public ResultSet execute(PreparedStatement statement) throws SQLException {
			return statement.executeQuery();
		}
	}

	private JdbcSession createJdbSession() {
		JdbcSessionOwnerTestingImpl JDBC_SESSION_OWNER = new JdbcSessionOwnerTestingImpl();
		JDBC_SESSION_OWNER.setBatchFactory( new BatchFactoryImpl( 1 ) );
		return JdbcSessionFactory.INSTANCE.create( JDBC_SESSION_OWNER, new ResourceRegistryStandardImpl() );
	}

	private void createTables() throws SQLException {
		final String createBillingAddressTableSql = "create table BillingAddress (" +
				"        ADDRESS_ID bigint not null," +
				"        owner varchar(255)," +
				"        primary key (ADDRESS_ID) )";
		execute( createBillingAddressTableSql );
	}

	private void dropTables() throws SQLException {
		execute( "DROP table BillingAddress IF EXISTS" );
	}

	private void execute(String sql) throws SQLException {
		PreparedStatement statement = localConnection.prepareStatement( sql );
		statement.execute();
	}
}
