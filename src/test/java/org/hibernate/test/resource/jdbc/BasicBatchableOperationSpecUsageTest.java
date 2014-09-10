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
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.resource.jdbc.BatchableOperationSpec;
import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.internal.BatchFactoryImpl;
import org.hibernate.resource.jdbc.internal.Batching;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchExpectation;
import org.hibernate.resource.jdbc.spi.BatchFactory;
import org.hibernate.resource.jdbc.spi.BatchKey;
import org.hibernate.resource.jdbc.spi.JdbcSessionFactory;
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.StatementBuilder;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;
import org.hibernate.test.resource.jdbc.common.JdbcSessionOwnerTestingImpl;

import static org.hibernate.resource.jdbc.BatchableOperationSpec.OperationStep;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Andrea Boriero
 */
public class BasicBatchableOperationSpecUsageTest {

	private static final String SQL_1 = "INSERT INTO ENTITY_1 (ID) VALUES (?) ";
	private static final String SQL_2 = "INSERT INTO ENTITY_2 (ID) VALUES (?) ";

	private static final JdbcSessionOwnerTestingImpl JDBC_SESSION_OWNER = new JdbcSessionOwnerTestingImpl();

	private Batch batch;
	private Batch batch2;
	private BatchKey batchKey;
	private JdbcSession jdbcSession;

	private final BatchableOperationSpec operationSpec = mock( BatchableOperationSpec.class );
	private final StatementBuilder queryStatementBuilder = mock( StatementBuilder.class );
	private final PreparedStatement statementSql1 = mock( PreparedStatement.class );
	private final PreparedStatement statementSql2 = mock( PreparedStatement.class );
	private final ParameterBindings parameterBindings1 = mock( ParameterBindings.class );
	private final ParameterBindings parameterBindings2 = mock( ParameterBindings.class );
	private final OperationStep step = mock( BatchableOperationSpec.GenericOperationStep.class );

	@Before
	public void setUp() throws SQLException {
		batchKey = new BatchKeyImpl( "{SomeEntity.INSERT}", mock( BatchExpectation.class ) );
		when( operationSpec.getBatchKey() ).thenReturn( batchKey );

		initBatching( mockBatchFactory() );

		List<OperationStep> steps = Arrays.asList( step );
		when( operationSpec.getSteps() ).thenReturn( steps );

		when( ((BatchableOperationSpec.GenericOperationStep)step).getQueryStatementBuilder() ).thenReturn(
				queryStatementBuilder
		);
		when( ((BatchableOperationSpec.GenericOperationStep)step).getSql() ).thenReturn( SQL_1 );

		when(
				queryStatementBuilder.buildPreparedStatement(
						any( Connection.class ),
						eq( SQL_1 )
				)
		).thenReturn(
				statementSql1
		);

		when(
				queryStatementBuilder.buildPreparedStatement(
						any( Connection.class ),
						eq( SQL_2 )
				)
		).thenReturn(
				statementSql2
		);

		when( statementSql1.executeBatch() ).thenReturn( new int[] {0} );
		when( statementSql2.executeBatch() ).thenReturn( new int[] {0} );

		when( ((BatchableOperationSpec.GenericOperationStep)step).getParameterBindings() ).thenReturn( parameterBindings1 );
	}

	@After
	public void tearDown() {
		if ( jdbcSession != null ) {
			jdbcSession.close();
		}
	}

	@Test
	public void whenAnOperationSpecIsAcceptedTheStatementIsCreatedAndTheParametersAreBinded()
			throws SQLException {
		jdbcSession.accept( operationSpec );

		verify( ((BatchableOperationSpec.GenericOperationStep)step) ).getQueryStatementBuilder();

		verify( parameterBindings1 ).bindParameters( statementSql1 );
	}

	@Test
	public void theStatementIaAddedToBatch()
			throws SQLException {
		jdbcSession.accept( operationSpec );

		verify( batch ).addBatch( SQL_1, statementSql1 );
	}

	@Test
	public void whenASecondOperationSpecWithTheSameSqlIsAddedANewStatementIsNotCreated()
			throws SQLException {
		jdbcSession.accept( operationSpec );

		when( batch.getStatement( anyString() ) ).thenReturn( statementSql1 );

		jdbcSession.accept( operationSpec );

		verify( batch, times( 2 ) ).addBatch( SQL_1, statementSql1 );

		verify( queryStatementBuilder, times( 1 ) ).buildPreparedStatement( any( Connection.class ), anyString() );
	}

	@Test
	public void whenASecondOperationSpectWithDifferentSqlIsAddedANewStatementIsCreated()
			throws SQLException {
		jdbcSession.accept( operationSpec );

		when( ((BatchableOperationSpec.GenericOperationStep)step).getSql() ).thenReturn( SQL_2 );

		jdbcSession.accept( operationSpec );

		verify( queryStatementBuilder, times( 2 ) ).buildPreparedStatement( any( Connection.class ), anyString() );
	}

	@Test
	public void previousBatchStatementExecutionIsForcedWhenABatchWithADifferentKeyIsSet() throws SQLException {
		jdbcSession.accept( operationSpec );

		when( operationSpec.getBatchKey() ).thenReturn(
				new BatchKeyImpl(
						"{SomeEntity.UPDATE}",
						mock( BatchExpectation.class )
				)
		);
		jdbcSession.accept( operationSpec );

		verify( batch, times( 1 ) ).execute();
	}

	@Test
	public void theStatmentsOfAllTheBatchableOperationStepsAreAddedToTheBatch() throws SQLException {
		BatchableOperationSpec.GenericOperationStep step2 = mock( BatchableOperationSpec.GenericOperationStep.class );
		when( step2.getParameterBindings() ).thenReturn( parameterBindings2 );
		when( step2.getQueryStatementBuilder() ).thenReturn( queryStatementBuilder );
		when( step2.getSql() ).thenReturn( SQL_2 );

		List<OperationStep> steps = Arrays.asList( step, step2 );
		when( operationSpec.getSteps() ).thenReturn( steps );

		jdbcSession.accept( operationSpec );

		verify( batch ).addBatch( SQL_1, statementSql1 );
		verify( batch ).addBatch( SQL_2, statementSql2 );
	}

	@Test
	public void BatchFactoryShouldBeCallWithTheCorrectParameters() {
		BatchFactory factory = mockBatchFactory();
		initBatching( factory );

		jdbcSession.accept( operationSpec );

		verify( factory ).buildBatch(
				operationSpec.getBatchKey(),
				JDBC_SESSION_OWNER.getJdbcSessionContext().getSqlExceptionHelper(),
				operationSpec.foregoBatching()
		);
	}

	@Test
	public void aNewBatchIsRegisterd() {
		ResourceRegistry resourceRegistry = mock( ResourceRegistry.class );
		initJdbSession( mockBatchFactory(), resourceRegistry );

		jdbcSession.accept( operationSpec );

		verify( resourceRegistry ).register( batch );
	}

	@Test
	public void whenAnewBatchKeyIsProvidedThePreviuosBatchIsUnregistered() {
		ResourceRegistry resourceRegistry = mock( ResourceRegistry.class );
		when( resourceRegistry.getCurrentBatch() ).thenReturn( null, batch );

		initJdbSession( mockBatchFactory(), resourceRegistry );

		jdbcSession.accept( operationSpec );

		when( operationSpec.getBatchKey() ).thenReturn(
				new BatchKeyImpl(
						"{SomeEntity.UPDATE}",
						mock( BatchExpectation.class )
				)
		);

		jdbcSession.accept( operationSpec );

		verify( resourceRegistry ).releaseCurrentBatch();
		verify( resourceRegistry ).register( batch2 );
	}

	private void initBatching(BatchFactory factory) {
		initJdbSession( factory );
	}

	private void initJdbSession(BatchFactory factory) {
		JDBC_SESSION_OWNER.setBatchFactory( factory );
		jdbcSession = JdbcSessionFactory.INSTANCE.create( JDBC_SESSION_OWNER, new ResourceRegistryStandardImpl() );

		when( operationSpec.foregoBatching() ).thenReturn( true );
	}

	private void initJdbSession(BatchFactory factory, ResourceRegistry registry) {
		JDBC_SESSION_OWNER.setBatchFactory( factory );
		jdbcSession = JdbcSessionFactory.INSTANCE.create( JDBC_SESSION_OWNER, registry );

		when( operationSpec.foregoBatching() ).thenReturn( true );
	}

	private BatchFactory mockBatchFactory() {
		batch = mock( Batching.class );
		batch2 = mock( Batching.class );
		when( batch.getKey() ).thenReturn( batchKey );
		final BatchFactory factory = mock( BatchFactoryImpl.class );
		when( factory.buildBatch( any( BatchKey.class ), any( SqlExceptionHelper.class ), anyBoolean() ) ).thenReturn(
				batch, batch2
		);
		return factory;
	}
}
