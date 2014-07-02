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
import java.sql.Statement;

import org.mockito.InOrder;

import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec;
import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.internal.JdbcSessionImpl;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.JdbcSessionFactory;
import org.hibernate.resource.jdbc.spi.LogicalConnectionImplementor;
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.QueryPreparedStatementBuilder;
import org.hibernate.resource.jdbc.spi.ResultSetProcessor;
import org.hibernate.resource.jdbc.spi.StatementExecutor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.JdbcSessionOwnerTestingImpl;

import static org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec.ResultSetConcurrency;
import static org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec.ResultSetType;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Andrea Boriero
 */
public class BasicPreparedStatementQueryOperationSpecTest {

	private static final int UNIMPORTANT_INT_VALUE = -1;

	private final JdbcSessionOwnerTestingImpl jdbcSessionOwner = new JdbcSessionOwnerTestingImpl();
	private JdbcSession jdbcSession;
	private PreparedStatementQueryOperationSpec operationSpec = mock( PreparedStatementQueryOperationSpec.class );
	private QueryPreparedStatementBuilder queryStatementBuilder = mock( QueryPreparedStatementBuilder.class );
	private StatementExecutor statementExecutor = mock( StatementExecutor.class );
	private ResultSetProcessor resultSetProcessor = mock( ResultSetProcessor.class );
	private ResourceRegistry resourceRegistry = mock( ResourceRegistry.class );
	private PreparedStatement statement = mock( PreparedStatement.class );
	private ResultSet resultSet = mock( ResultSet.class );
	private ParameterBindings parameterBindings = mock( ParameterBindings.class );

	@Before
	public void setUp() {
		jdbcSession = JdbcSessionFactory.INSTANCE.create( jdbcSessionOwner, resourceRegistry );

		when( operationSpec.holdOpenResources() ).thenReturn( false );
		when( operationSpec.getParameterBindings() ).thenReturn( parameterBindings );
		when( operationSpec.getQueryStatementBuilder() ).thenReturn( queryStatementBuilder );
		when( operationSpec.getStatementExecutor() ).thenReturn( statementExecutor );
		when( operationSpec.getResultSetProcessor() ).thenReturn( resultSetProcessor );
		when( statementExecutor.execute( any( Statement.class ), eq( (JdbcSessionImpl) jdbcSession ) ) ).thenReturn(
				resultSet
		);
		when(
				queryStatementBuilder.buildQueryStatement(
						any( Connection.class ),
						anyString(),
						any( JdbcSessionContext.class )
				)
		).thenReturn(
				statement
		);
	}

	@After
	public void tearDown() {
		jdbcSession.close();
	}

	@Test
	public void operationSpecMethodsAreCalledInRightOrder() {
		jdbcSession.accept( operationSpec );

		InOrder inorder = inOrder( operationSpec );
		inorder.verify( operationSpec ).getQueryStatementBuilder();
		inorder.verify( operationSpec ).getParameterBindings();
		inorder.verify( operationSpec ).getStatementExecutor();
		inorder.verify( operationSpec ).getResultSetProcessor();

		verify( queryStatementBuilder ).buildQueryStatement(
				any( Connection.class ), anyString(), any( JdbcSessionContext.class )
		);
		verify( statementExecutor ).execute( statement, (JdbcSessionImpl) jdbcSession );
		verify( resultSetProcessor ).extractResults( resultSet, (JdbcSessionImpl) jdbcSession );
	}

	@Test
	public void buildQueryStatementBuilderMethodIsCalledWithTheExpectedParameters() {
		String expectedSql = "select * from SomeEntity";
		ResultSetConcurrency expectedResultSetConcurrency = ResultSetConcurrency.READ_ONLY;
		ResultSetType expectedResultSetType = ResultSetType.FORWARD_ONLY;
		mockOperationMethods(
				UNIMPORTANT_INT_VALUE,
				expectedSql,
				expectedResultSetType,
				expectedResultSetConcurrency,
				false
		);

		jdbcSession.accept( operationSpec );

		verify( queryStatementBuilder ).buildQueryStatement(
				((LogicalConnectionImplementor) jdbcSession.getLogicalConnection()).getPhysicalConnection(),
				expectedSql,
				jdbcSessionOwner.getJdbcSessionContext()
		);
	}

	@Test
	public void statementExecutorMethodIsCalledWithTheExpectedParameters() {
		jdbcSession.accept( operationSpec );

		verify( statementExecutor ).execute( statement, (JdbcSessionImpl) jdbcSession );
	}

	@Test
	public void resultSetProcessorMethodIsCalledWithTheExpectedParameters() {
		jdbcSession.accept( operationSpec );

		verify( resultSetProcessor ).extractResults( resultSet, (JdbcSessionImpl) jdbcSession );
	}

	@Test
	public void resourcesAreReleasedIfHoldResourcesIsFalse() {
		setHoldResources( false );

		jdbcSession.accept( operationSpec );

		verify( resourceRegistry ).release( resultSet, statement );
	}

	@Test
	public void resourcesAreNOTReleasedIfHoldResourcesIsFalse() {
		setHoldResources( true );

		jdbcSession.accept( operationSpec );

		verify( resourceRegistry, never() ).release( resultSet, statement );
	}

	@Test
	public void bindParametersReceiveTheCorrectPreparedStatement() {
		jdbcSession.accept( operationSpec );
		verify( parameterBindings ).bindParameters( statement );
	}

	private void setHoldResources(boolean holdResources) {
		when( operationSpec.holdOpenResources() ).thenReturn( holdResources );
	}

	private void mockOperationMethods(
			int queryTimeout,
			String sql,
			ResultSetType resultSetType,
			ResultSetConcurrency resultSetConcurrency,
			boolean holdResources) {
		when( operationSpec.getQueryTimeout() ).thenReturn( queryTimeout );
		when( operationSpec.getSql() ).thenReturn( sql );
		when( operationSpec.getResultSetType() ).thenReturn( resultSetType );
		when( operationSpec.getResultSetConcurrency() ).thenReturn( resultSetConcurrency );
		setHoldResources( holdResources );
	}
}
