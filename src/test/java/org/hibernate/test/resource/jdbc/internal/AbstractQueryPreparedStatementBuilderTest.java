package org.hibernate.test.resource.jdbc.internal;

import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.resource.jdbc.spi.JdbcObserver;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;

import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.JdbcObserverNoOpImpl;
import org.hibernate.test.resource.jdbc.common.StatementInspectorNoOpImpl;

import static org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec.ResultSetConcurrency;
import static org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec.ResultSetType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractQueryPreparedStatementBuilderTest {

	protected Connection connection = mock( Connection.class );
	protected QueryStatementBuilder queryBuilder;
	private final String sql = "Select * from item";
	protected ConnectionMethodCallCheck verifier;
	protected JdbcSessionContext context = mock( JdbcSessionContext.class );

	protected void onSetUp() {
		when( context.getObserver() ).thenReturn( new JdbcObserverNoOpImpl() );
		when( context.getSqlStatementLogger() ).thenReturn( new SqlStatementLogger() );
		when( context.getStatementInspector() ).thenReturn( new StatementInspectorNoOpImpl() );
	}

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

		queryBuilder.buildQueryStatement( connection, context, sql, null, null );

		verifier.verify( connection, sql );

		queryBuilder.buildQueryStatement(
				connection,
				context,
				sql,
				ResultSetType.FORWARD_ONLY,
				null
		);

		verifier.verify( connection, sql );

		queryBuilder.buildQueryStatement(
				connection,
				context,
				sql,
				null,
				ResultSetConcurrency.READ_ONLY
		);

		verifier.verify( connection, sql );
	}

	@Test
	public void prepareCallIsInvokedWithTheExpectedResultTypeAndResultConcurrenecy() throws SQLException {
		ResultSetType expectedResultSetType = ResultSetType.FORWARD_ONLY;
		ResultSetConcurrency expectedResultSetConcurrency = ResultSetConcurrency.READ_ONLY;

		queryBuilder.buildQueryStatement(
				connection,
				context,
				sql,
				expectedResultSetType,
				expectedResultSetConcurrency
		);

		verifier.verify( connection, sql, expectedResultSetType, expectedResultSetConcurrency );
	}

}