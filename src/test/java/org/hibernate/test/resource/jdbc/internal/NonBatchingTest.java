package org.hibernate.test.resource.jdbc.internal;

import java.sql.SQLException;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.resource.jdbc.internal.NonBatching;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;
import org.hibernate.resource.jdbc.spi.Batch;

import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class NonBatchingTest extends AbstractBatchingTest {

	@Test
	public void statementIsExecutedWhenAdded() throws SQLException {
		Batch batch = new NonBatching(
				new BatchKeyImpl( "{SomeEntity.INSERT}", expectation ),
				mock( SqlExceptionHelper.class )
		);

		batch.addBatch( SQL_1, statementSql1 );

		verify( statementSql1 ).executeUpdate();
	}

	@Test
	public void statementIsClosedAfterExecution() throws SQLException {
		Batch batch = new NonBatching(
				new BatchKeyImpl( "{SomeEntity.INSERT}", expectation ),
				mock( SqlExceptionHelper.class )
		);

		batch.addBatch( SQL_1, statementSql1 );

		verify( statementSql1 ).close();
	}


	@Test
	public void statementIsClosedANewBatchIsAdded() throws SQLException {
		Batch batch = new NonBatching(
				new BatchKeyImpl( "{SomeEntity.INSERT}", expectation ),
				mock( SqlExceptionHelper.class )
		);

		batch.addBatch( SQL_1, statementSql1 );
		batch.addBatch( SQL_2, statementSql2 );

		verify( statementSql1 ).close();
	}

	@Override
	protected void setResourceRegistry() {
		resourceRegistry = new ResourceRegistryStandardImpl();
	}
}