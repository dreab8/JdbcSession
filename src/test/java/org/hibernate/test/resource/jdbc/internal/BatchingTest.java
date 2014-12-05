package org.hibernate.test.resource.jdbc.internal;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.internal.Batching;
import org.hibernate.resource.jdbc.spi.Batch;

import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BatchingTest extends AbstractBatchingTest {

	@Test
	public void statementsWithSameSqlAreExecutedWhenTheBatchSizeIsReached() throws SQLException {
		final Batch batch = createBatch( 2, "{SomeEntity.INSERT}" );

		final PreparedStatement statement = getStatement( batch, SQL_1 );

		batch.addBatch( SQL_1, statement );
		batch.advance();

		verify( statement ).addBatch();
		verify( statement, never() ).executeBatch();

		batch.addBatch( SQL_1, statement );
		batch.advance();

		verify( statement, times( 2 ) ).addBatch();
		verify( statement ).executeBatch();
		verify( statement ).clearBatch();
	}

	@Test
	public void afterTheFirstExecutionTheBatchIsReExecutedOnlyWhenTheBatchSizeIsReachedAgain()
			throws SQLException {
		final Batch batch = createBatch( 2, "{SomeEntity.INSERT}" );

		final PreparedStatement statement = getStatement( batch, SQL_1 );

		batch.addBatch( SQL_1, statement );
		batch.advance();

		batch.addBatch( SQL_1, statement );
		batch.advance();

		batch.addBatch( SQL_1, statement );
		batch.advance();

		verify( statement, times( 3 ) ).addBatch();
		verify( statement, times( 1 ) ).executeBatch();
		verify( statement, times( 1 ) ).clearBatch();

		batch.addBatch( SQL_1, statement );
		batch.advance();

		verify( statement, times( 4 ) ).addBatch();
		verify( statement, times( 2 ) ).executeBatch();
		verify( statement, times( 2 ) ).clearBatch();
	}

	@Test
	public void batchWithDifferentsSqlStatementsExecutesAllTheStatementsWhenTheBatchSizeIsReached()
			throws SQLException {
		final Batch batch = createBatch( 2, "{SomeEntity.INSERT}" );

		final PreparedStatement statement1 = getStatement( batch, SQL_1 );

		batch.addBatch( SQL_1, statement1 );
		batch.advance();

		verify( statement1 ).addBatch();
		verify( statement1, never() ).executeBatch();

		final PreparedStatement statement2 = getStatement( batch, SQL_2 );

		batch.addBatch( SQL_2, statement2 );
		batch.advance();

		verify( statement2 ).addBatch();

		verify( statement1 ).executeBatch();
		verify( statement2 ).executeBatch();

		verify( statement1 ).clearBatch();
		verify( statement2 ).clearBatch();
	}

	@Test
	public void batchExecutionClosesAllTheStatements() throws SQLException {
		final Batch batch = createBatch( 2, "{SomeEntity.INSERT}" );

		final PreparedStatement statement1 = getStatement( batch, SQL_1 );

		batch.addBatch( SQL_1, statement1 );
		batch.advance();

		final PreparedStatement statement2 = getStatement( batch, SQL_2 );

		batch.addBatch( SQL_2, statement2 );
		batch.advance();

		verify( statement1 ).close();
		verify( statement1 ).close();
	}

	@Test
	public void batchReleaseClosesAllTheStatements() throws SQLException {
		final Batch batch = createBatch( 3, "{SomeEntity.INSERT}" );

		final PreparedStatement statement1 = getStatement( batch, SQL_1 );

		batch.addBatch( SQL_1, statement1 );
		batch.advance();

		final PreparedStatement statement2 = getStatement( batch, SQL_2 );

		batch.addBatch( SQL_2, statement2 );
		batch.advance();

		batch.release();

		verify( statement1 ).close();
		verify( statement2 ).close();
	}

	@Test
	public void batchReleaseNotExecuteTheStatements() throws SQLException {
		final Batch batch = createBatch( 3, "{SomeEntity.INSERT}" );

		final PreparedStatement statement1 = getStatement( batch, SQL_1 );

		batch.addBatch( SQL_1, statement1 );
		batch.advance();

		final PreparedStatement statement2 = getStatement( batch, SQL_2 );

		batch.addBatch( SQL_2, statement2 );
		batch.advance();

		batch.release();

		verify( statement1, never() ).executeBatch();
		verify( statement2, never() ).executeBatch();
	}

	private Batch createBatch(int batchSize, String keyComparision) {
		return new Batching(
				new BatchKeyImpl( keyComparision ),
				batchSize,
				mock( SqlExceptionHelper.class )
		);
	}

	@Override
	protected void setResourceRegistry() {
		resourceRegistry = mock( ResourceRegistry.class );
	}
}