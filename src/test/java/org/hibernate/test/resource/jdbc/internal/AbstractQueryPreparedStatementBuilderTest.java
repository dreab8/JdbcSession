package org.hibernate.test.resource.jdbc.internal;

import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;

import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.JdbcSessionContextStandardTestingImpl;

import static org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec.ResultSetConcurrency;
import static org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec.ResultSetType;
import static org.mockito.Mockito.mock;

public abstract class AbstractQueryPreparedStatementBuilderTest {

	private Connection connection = mock( Connection.class );
	protected QueryStatementBuilder queryBuilder;
	private final String sql = "Select * from item";
	private final JdbcSessionContext context = JdbcSessionContextStandardTestingImpl.INSTANCE;
	private ConnectionMethodCallCheck verifier;

	protected interface ConnectionMethodCallCheck {
		void verify(Connection c, String expectedSql) throws SQLException;

		void verify(
				Connection c,
				String expectedSql,
				ResultSetType expectedResultSetType,
				ResultSetConcurrency expectedResultSetConcurrency) throws SQLException;
	}

	protected void setMethodCallCheck(ConnectionMethodCallCheck v) {
		this.verifier = v;
	}

	@Test
	public void prepareCallIsInvokedWithoutResultTypeAndResultConcurrenecy() throws SQLException {
		queryBuilder.buildQueryStatement( connection, sql, context, null, null );

		verifier.verify( connection, sql );

		queryBuilder.buildQueryStatement(
				connection, sql, context,
				ResultSetType.FORWARD_ONLY, null
		);

		verifier.verify( connection, sql );

		queryBuilder.buildQueryStatement(
				connection, sql, context,
				null, ResultSetConcurrency.READ_ONLY
		);

		verifier.verify( connection, sql );
	}

	@Test
	public void prepareCallIsInvokedWithTheExpectedResultTypeAndResultConcurrenecy() throws SQLException {
		ResultSetType expectedResultSetType = ResultSetType.FORWARD_ONLY;
		ResultSetConcurrency expectedResultSetConcurrency = ResultSetConcurrency.READ_ONLY;

		queryBuilder.buildQueryStatement(
				connection, sql, context,
				expectedResultSetType, expectedResultSetConcurrency
		);

		verifier.verify( connection, sql, expectedResultSetType, expectedResultSetConcurrency );

	}

}