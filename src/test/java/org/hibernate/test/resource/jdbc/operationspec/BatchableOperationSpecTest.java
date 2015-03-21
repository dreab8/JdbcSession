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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.mockito.InOrder;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.jdbc.Expectation;
import org.hibernate.resource.jdbc.BatchableOperationSpec;
import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.internal.BatchBuilderImpl;
import org.hibernate.resource.jdbc.internal.Batching;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchBuilder;
import org.hibernate.resource.jdbc.spi.BatchKey;
import org.hibernate.resource.jdbc.spi.JdbcSessionFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;
import org.hibernate.test.resource.jdbc.common.JdbcSessionOwnerTestingImpl;

import static org.hibernate.resource.jdbc.BatchableOperationSpec.BatchableOperationStep;
import static org.hibernate.resource.jdbc.BatchableOperationSpec.BatchableOperationStep.Context;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Andrea Boriero
 */
public class BatchableOperationSpecTest {
	private static final JdbcSessionOwnerTestingImpl JDBC_SESSION_OWNER = new JdbcSessionOwnerTestingImpl();

	private Batch batch;
	private Batch batch2;
	private BatchKey batchKey;
	private JdbcSession jdbcSession;

	private final BatchableOperationSpec operationSpec = mock( BatchableOperationSpec.class );
	private final BatchableOperationStep step = mock( BatchableOperationStep.class );
	private final Context batchContext = mock( Context.class );

	private final Expectation expectation = mock( Expectation.class );
	private Connection connection;

	@Before
	public void setUp() throws SQLException {
		connection = JDBC_SESSION_OWNER.getJdbcConnectionAccess().obtainConnection();
		batchKey = new BatchKeyImpl( "{SomeEntity.INSERT}" );
		when( operationSpec.getBatchKey() ).thenReturn( batchKey );

		initBatching( mockBatchFactory() );

		List<BatchableOperationStep> steps = Arrays.asList( step );
		when( operationSpec.getSteps() ).thenReturn( steps );
	}

	@After
	public void tearDown() {
		if ( jdbcSession != null ) {
			jdbcSession.close();
		}
	}

	@Test
	public void previousBatchStatementExecutionIsForcedWhenABatchWithADifferentKeyIsSet() throws SQLException {
		ArrayList<Context> contexts = new ArrayList<Context>();
		contexts.add( batchContext );

		jdbcSession.accept( operationSpec, contexts );

		when( operationSpec.getBatchKey() ).thenReturn(
				new BatchKeyImpl( "{SomeEntity.UPDATE}" )
		);

		jdbcSession.accept( operationSpec, contexts );

		verify( batch, times( 1 ) ).execute();
	}

	@Test
	public void theApplyMethodOfEachStepIsCalled() throws SQLException {
		final Context batchContextStep2 = mock( Context.class );
		final BatchableOperationStep step2 = mock( BatchableOperationStep.class );

		final List<BatchableOperationStep> steps = Arrays.asList( step, step2 );
		when( operationSpec.getSteps() ).thenReturn( steps );

		ArrayList<Context> contexts = new ArrayList<Context>();
		contexts.add( batchContext );
		contexts.add( batchContextStep2 );

		jdbcSession.accept( operationSpec, contexts );

		InOrder inOrder = inOrder( step, step2 );
		inOrder.verify( step ).apply( jdbcSession, batch, connection, batchContext );
		inOrder.verify( step2 ).apply( jdbcSession, batch, connection, batchContextStep2 );
	}

	@Test
	public void BatchFactoryShouldBeCallWithTheCorrectParameters() {
		final BatchBuilder factory = mockBatchFactory();
		initBatching( factory );

		ArrayList<Context> contexts = new ArrayList<Context>();
		contexts.add( batchContext );

		jdbcSession.accept( operationSpec, contexts );

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

		ArrayList<Context> contexts = new ArrayList<Context>();
		contexts.add( batchContext );

		jdbcSession.accept( operationSpec, contexts );

		verify( resourceRegistry ).register( batch );
	}

	@Test
	public void whenAnewBatchKeyIsProvidedThePreviuosBatchIsUnregistered() {
		ResourceRegistry resourceRegistry = mock( ResourceRegistry.class );
		when( resourceRegistry.getCurrentBatch() ).thenReturn( null, batch );

		initJdbSession( mockBatchFactory(), resourceRegistry );

		ArrayList<Context> contexts = new ArrayList<Context>();
		contexts.add( batchContext );

		jdbcSession.accept( operationSpec, contexts );

		when( operationSpec.getBatchKey() ).thenReturn(
				new BatchKeyImpl( "{SomeEntity.UPDATE}" )
		);

		jdbcSession.accept( operationSpec, contexts );

		verify( resourceRegistry ).releaseCurrentBatch();
		verify( resourceRegistry ).register( batch2 );
	}

	private void initBatching(BatchBuilder factory) {
		initJdbSession( factory );
	}

	private void initJdbSession(BatchBuilder factory) {
		JDBC_SESSION_OWNER.setBatchBuilder( factory );
		jdbcSession = JdbcSessionFactory.INSTANCE.create( JDBC_SESSION_OWNER, new ResourceRegistryStandardImpl() );

		when( operationSpec.foregoBatching() ).thenReturn( true );
	}

	private void initJdbSession(BatchBuilder factory, ResourceRegistry registry) {
		JDBC_SESSION_OWNER.setBatchBuilder( factory );
		jdbcSession = JdbcSessionFactory.INSTANCE.create( JDBC_SESSION_OWNER, registry );

		when( operationSpec.foregoBatching() ).thenReturn( true );
	}

	private BatchBuilder mockBatchFactory() {
		batch = mock( Batching.class );
		batch2 = mock( Batching.class );
		when( batch.getKey() ).thenReturn( batchKey );
		final BatchBuilder factory = mock( BatchBuilderImpl.class );
		when( factory.buildBatch( any( BatchKey.class ), any( SqlExceptionHelper.class ), anyBoolean() ) ).thenReturn(
				batch, batch2
		);
		return factory;
	}
}
