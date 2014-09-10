package org.hibernate.test.resource.jdbc.internal;

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
		Batch batch = getBatch( 2, "{SomeEntity.INSERT}" );

		batch.addBatch( SQL_1, statementSql1 );

		verify( statementSql1 ).addBatch();
		verify( statementSql1, never() ).executeBatch();

		batch.addBatch( SQL_1, statementSql1 );

		verify( statementSql1, times( 2 ) ).addBatch();
		verify( statementSql1 ).executeBatch();
		verify( statementSql1 ).clearBatch();
	}

	@Test
	public void afterTheFirstExecutionTheBatchIsReExecutedOnlyWhenTheBatchSizeIsReachedAgain()
			throws SQLException {
		Batch batch = getBatch( 2, "{SomeEntity.INSERT}" );

		batch.addBatch( SQL_1, statementSql1 );
		batch.addBatch( SQL_1, statementSql1 );
		batch.addBatch( SQL_1, statementSql1 );

		verify( statementSql1, times( 3 ) ).addBatch();
		verify( statementSql1, times( 1 ) ).executeBatch();
		verify( statementSql1, times( 1 ) ).clearBatch();

		batch.addBatch( SQL_1, statementSql1 );

		verify( statementSql1, times( 4 ) ).addBatch();
		verify( statementSql1, times( 2 ) ).executeBatch();
		verify( statementSql1, times( 2 ) ).clearBatch();
	}

	@Test
	public void batchWithDifferentsSqlStatementsExecutesAllTheStatementsWhenTheBatchSizeIsReached()
			throws SQLException {
		Batch batch = getBatch( 2, "{SomeEntity.INSERT}" );

		batch.addBatch( SQL_1, statementSql1 );

		verify( statementSql1 ).addBatch();
		verify( statementSql1, never() ).executeBatch();

		batch.addBatch( SQL_2, statementSql2 );

		verify( statementSql2 ).addBatch();
		verify( statementSql1 ).executeBatch();
		verify( statementSql2 ).executeBatch();
		verify( statementSql1 ).clearBatch();
		verify( statementSql2 ).clearBatch();
	}

	@Test
	public void batchExecutionClosesAllTheStatements() throws SQLException {
		Batch batch = getBatch( 2, "{SomeEntity.INSERT}" );
		batch.addBatch( SQL_1, statementSql1 );
		batch.addBatch( SQL_2, statementSql2 );

		verify( statementSql1 ).close();
		verify( statementSql2 ).close();
	}

	@Test
	public void batchReleaseClosesAllTheStatements() throws SQLException {
		Batch batch = getBatch( 3, "{SomeEntity.INSERT}" );
		batch.addBatch( SQL_1, statementSql1 );
		batch.addBatch( SQL_2, statementSql2 );

		batch.release();

		verify( statementSql1 ).close();
		verify( statementSql2 ).close();
	}

	@Test
	public void batchReleaseNotExecuteTheStatements() throws SQLException {
		Batch batch = getBatch( 3, "{SomeEntity.INSERT}" );
		batch.addBatch( SQL_1, statementSql1 );
		batch.addBatch( SQL_2, statementSql2 );

		batch.release();

		verify( statementSql1, never() ).executeBatch();
		verify( statementSql2, never() ).executeBatch();
	}

	private Batch getBatch(int batchSize, String keyComparision) {
		return new Batching(
				new BatchKeyImpl( keyComparision, expectation ),
				batchSize,
				mock( SqlExceptionHelper.class )
		);
	}

	@Override
	protected void setResourceRegistry() {
		resourceRegistry = mock( ResourceRegistry.class );
	}
}