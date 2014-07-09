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
import java.sql.Statement;

import org.mockito.InOrder;

import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.QueryOperationSpec;
import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.ScrollableQueryOperationSpec;
import org.hibernate.resource.jdbc.internal.JdbcSessionImpl;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;
import org.hibernate.resource.jdbc.spi.JdbcSessionFactory;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;
import org.hibernate.resource.jdbc.spi.StatementExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.JdbcSessionOwnerTestingImpl;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Andrea Boriero
 */
public class BasicScrollableQueryOperationSpecTest {

	private static final int UNIMPORTANT_INT_VALUE = -1;

	private JdbcSession jdbcSession;
	private ResourceRegistry resourceRegistry;

	private final JdbcSessionOwnerTestingImpl jdbcSessionOwner = new JdbcSessionOwnerTestingImpl();
	private final ScrollableQueryOperationSpec operationSpec = mock( ScrollableQueryOperationSpec.class );
	private final QueryStatementBuilder queryStatementBuilder = mock( QueryStatementBuilder.class );
	private final StatementExecutor statementExecutor = mock( StatementExecutor.class );
	private final PreparedStatement statement = mock( PreparedStatement.class );
	private final ResultSet resultSet = mock( ResultSet.class );
	private final ParameterBindings parameterBindings = mock( ParameterBindings.class );

	@Before
	public void setUp() throws SQLException {
		resourceRegistry = new ResourceRegistryStandardImpl();
		jdbcSession = JdbcSessionFactory.INSTANCE.create( jdbcSessionOwner, resourceRegistry );

		when( operationSpec.getParameterBindings() ).thenReturn( parameterBindings );
		when( operationSpec.getQueryStatementBuilder() ).thenReturn( queryStatementBuilder );
		when( operationSpec.getStatementExecutor() ).thenReturn( statementExecutor );
		when( statementExecutor.execute( any( Statement.class ), eq( (JdbcSessionImpl) jdbcSession ) ) ).thenReturn(
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
	public void operationSpecMethodsAreCalledInRightOrder() throws SQLException {
		jdbcSession.accept( operationSpec );

		InOrder inOrder = inOrder( operationSpec );
		inOrder.verify( operationSpec ).getQueryStatementBuilder();
		inOrder.verify( operationSpec ).getParameterBindings();
		inOrder.verify( operationSpec ).getStatementExecutor();

		verify( queryStatementBuilder ).buildQueryStatement(
				any( Connection.class ),
				anyString(),
				any( QueryOperationSpec.ResultSetType.class ),
				any( QueryOperationSpec.ResultSetConcurrency.class )
		);
		verify( statementExecutor ).execute( statement, (JdbcSessionImpl) jdbcSession );
	}

	@Test
	public void bindindgParamnetersMehtodsAreCalledInRightOrder() {
		final int BIND_LINIT_OFFSET_AT_START_RETURN_INDEX = 0;
		final int BIND_PARAMETERS_RETURN_INDEX = 2;

		when( parameterBindings.bindLimitOffsetParametersAtStartOfQuery( any( PreparedStatement.class ) ) ).thenReturn(
				BIND_LINIT_OFFSET_AT_START_RETURN_INDEX
		);
		when( parameterBindings.bindParameters( any( PreparedStatement.class ), anyInt() ) ).thenReturn(
				BIND_PARAMETERS_RETURN_INDEX
		);

		jdbcSession.accept( operationSpec );

		InOrder inOrder = inOrder( parameterBindings );

		inOrder.verify( parameterBindings ).bindLimitOffsetParametersAtStartOfQuery( statement );
		inOrder.verify( parameterBindings ).bindParameters( statement, BIND_LINIT_OFFSET_AT_START_RETURN_INDEX );
		inOrder.verify( parameterBindings ).bindLimitOffsetParametersAtEndOfQuery(
				statement,
				BIND_PARAMETERS_RETURN_INDEX
		);
		inOrder.verify( parameterBindings ).setMaxRow( statement );
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

		jdbcSession.accept( operationSpec );

		verify( queryStatementBuilder ).buildQueryStatement(
				((LogicalConnectionImplementor) jdbcSession.getLogicalConnection()).getPhysicalConnection(),
				expectedSql,
				QueryOperationSpec.ResultSetType.FORWARD_ONLY,
				QueryOperationSpec.ResultSetConcurrency.READ_ONLY
		);
	}

	@Test
	public void statementExecutorMethodIsCalledWithTheExpectedParameters() {
		jdbcSession.accept( operationSpec );

		verify( statementExecutor ).execute( statement, (JdbcSessionImpl) jdbcSession );
	}

	@Test
	public void bindParametersReceiveTheCorrectPreparedStatement() {
		final int BIND_LINIT_OFFSET_AT_START_RETURN_INDEX = 0;

		when( parameterBindings.bindLimitOffsetParametersAtStartOfQuery( statement ) ).thenReturn(
				BIND_LINIT_OFFSET_AT_START_RETURN_INDEX
		);

		jdbcSession.accept( operationSpec );

		verify( parameterBindings ).bindParameters( statement, BIND_LINIT_OFFSET_AT_START_RETURN_INDEX );
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

	@Test
	public void closeMethodShouldCloseStatementAndResultSet() throws SQLException {
		ScrollableQueryOperationSpec.OperationSpecResult result = jdbcSession.accept( operationSpec );

		result.close();

		assertThat( result.getResultSet(), is( resultSet ) );

		verify( statement ).close();
		verify( resultSet ).close();
	}
}
