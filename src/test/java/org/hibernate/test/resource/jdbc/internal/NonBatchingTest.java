package org.hibernate.test.resource.jdbc.internal;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.resource.jdbc.internal.NonBatching;
import org.hibernate.resource.jdbc.internal.ResourceRegistryStandardImpl;
import org.hibernate.resource.jdbc.spi.Batch;

import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class NonBatchingTest extends AbstractBatchingTest {

	final Batch batch = createBatch();

	@Test
	public void statementIsExecutedWhenAdded() throws SQLException {
		final PreparedStatement statement1 = getStatement( batch, SQL_1 );

		batch.addBatch( SQL_1, statement1 );

		verify( statement1 ).executeUpdate();
	}

	@Test
	public void statementIsClosedAfterExecution() throws SQLException {
		final PreparedStatement statement1 = getStatement( batch, SQL_1 );

		batch.addBatch( SQL_1, statement1 );

		verify( statement1 ).close();
	}


	@Test
	public void statementIsClosedWhenANewBatchIsAdded() throws SQLException {
		final PreparedStatement statement1 = getStatement( batch, SQL_1 );

		batch.addBatch( SQL_1, statement1 );

		final PreparedStatement statement2 = getStatement( batch, SQL_2 );

		batch.addBatch( SQL_2, statement2 );

		verify( statement1 ).close();
	}

	private NonBatching createBatch() {
		return new NonBatching(
				new BatchKeyImpl( "{SomeEntity.INSERT}" ),
				mock( SqlExceptionHelper.class )
		);
	}

	@Override
	protected void setResourceRegistry() {
		resourceRegistry = new ResourceRegistryStandardImpl();
	}
}