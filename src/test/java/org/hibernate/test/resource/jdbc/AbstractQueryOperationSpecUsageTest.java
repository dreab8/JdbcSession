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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.mockito.InOrder;

import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.QueryOperationSpec;
import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;
import org.hibernate.resource.jdbc.spi.JdbcSessionFactory;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;
import org.hibernate.resource.jdbc.spi.ResultSetProcessor;
import org.hibernate.resource.jdbc.spi.StatementExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.JdbcSessionOwnerTestingImpl;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Andrea Borierprotectedo
 */
public abstract class AbstractQueryOperationSpecUsageTest<T extends QueryOperationSpec> {
	protected static final int UNIMPORTANT_INT_VALUE = -1;

	protected T operationSpec;
	protected JdbcSession jdbcSession;

	protected final JdbcSessionOwnerTestingImpl jdbcSessionOwner = new JdbcSessionOwnerTestingImpl();
	protected final ResourceRegistry resourceRegistry = new ResourceRegistryStandardImpl();

	protected final QueryStatementBuilder queryStatementBuilder = mock( QueryStatementBuilder.class );
	protected final StatementExecutor statementExecutor = mock( StatementExecutor.class );
	protected final ResultSetProcessor resultSetProcessor = mock( ResultSetProcessor.class );
	protected final PreparedStatement statement = mock( PreparedStatement.class );
	protected final ResultSet resultSet = mock( ResultSet.class );
	protected final ParameterBindings parameterBindings = mock( ParameterBindings.class );

	protected abstract void mockQueryOperationSpec();

	protected abstract void jdbSessionAccept();

	@Before
	public void setUp() throws SQLException {
		jdbcSession = JdbcSessionFactory.INSTANCE.create( jdbcSessionOwner, resourceRegistry );

		mockQueryOperationSpec();

		when( operationSpec.getParameterBindings() ).thenReturn( parameterBindings );
		when( operationSpec.getQueryStatementBuilder() ).thenReturn( queryStatementBuilder );
		when( operationSpec.getStatementExecutor() ).thenReturn( statementExecutor );
		when( statementExecutor.execute( any( PreparedStatement.class ) ) ).thenReturn(
				resultSet
		);
		when(
				queryStatementBuilder.buildQueryStatement(
						any( Connection.class ),
						anyString(),
						any( QueryOperationSpec.ResultSetType.class ),
						any( QueryOperationSpec.ResultSetConcurrency.class )
				)
		).thenReturn(
				statement
		);
	}

	@After
	public void tearDown() {
		if ( jdbcSession != null ) {
			jdbcSession.close();
		}
	}

	@Test
	public void bindindgParamnetersMehtodsAreCalledInRightOrder() throws SQLException {
		jdbSessionAccept();

		InOrder inOrder = inOrder( parameterBindings );

		inOrder.verify( parameterBindings ).bindParameters( statement );
	}

	@Test
	public void buildQueryStatementBuilderMethodIsCalledWithTheExpectedParameters() throws SQLException {
		String expectedSql = "select * from SomeEntity";
		QueryOperationSpec.ResultSetConcurrency expectedResultSetConcurrency = QueryOperationSpec.ResultSetConcurrency.READ_ONLY;
		QueryOperationSpec.ResultSetType expectedResultSetType = QueryOperationSpec.ResultSetType.FORWARD_ONLY;
		mockOperationMethods(
				UNIMPORTANT_INT_VALUE,
				expectedSql,
				expectedResultSetType,
				expectedResultSetConcurrency
		);

		jdbSessionAccept();

		verify( queryStatementBuilder ).buildQueryStatement(
				((LogicalConnectionImplementor) jdbcSession.getLogicalConnection()).getPhysicalConnection(),
				expectedSql,
				QueryOperationSpec.ResultSetType.FORWARD_ONLY,
				QueryOperationSpec.ResultSetConcurrency.READ_ONLY
		);
	}

	@Test
	public void bindParametersReceiveTheCorrectPreparedStatement() throws SQLException {
		jdbSessionAccept();

		verify( parameterBindings ).bindParameters( statement );
	}

	@Test
	public void statementExecutorMethodIsCalledWithTheExpectedParameters() throws SQLException {
		jdbSessionAccept();

		verify( statementExecutor ).execute( statement );
	}

	private void mockOperationMethods(
			int queryTimeout,
			String sql,
			QueryOperationSpec.ResultSetType resultSetType,
			QueryOperationSpec.ResultSetConcurrency resultSetConcurrency) {
		when( operationSpec.getQueryTimeout() ).thenReturn( queryTimeout );
		when( operationSpec.getSql() ).thenReturn( sql );
		when( operationSpec.getResultSetType() ).thenReturn( resultSetType );
		when( operationSpec.getResultSetConcurrency() ).thenReturn( resultSetConcurrency );
	}
}
