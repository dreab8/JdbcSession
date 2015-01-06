package org.hibernate.test.resource.jdbc.internal;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.HibernateException;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.jdbc.Expectation;
import org.hibernate.resource.jdbc.internal.BatchBuilderImpl;
import org.hibernate.resource.jdbc.internal.Batching;
import org.hibernate.resource.jdbc.internal.NonBatching;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchBuilder;
import org.hibernate.resource.jdbc.spi.BatchKey;

import org.junit.Before;
import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class BatchBuilderImplTest {
	private BatchKey key;

	@Before
	public void setUp() {
		final Expectation batchExpectation = new Expectation() {
			@Override
			public void verifyOutcome(int rowCount, PreparedStatement statement, int batchPosition)
					throws SQLException, HibernateException {
			}

			@Override
			public int prepare(PreparedStatement statement) throws SQLException, HibernateException {
				return 0;
			}

			@Override
			public boolean canBeBatched() {
				return false;
			}
		};

		key = new BatchKeyImpl( "UNIMPORTANT" );
	}

	@Test
	public void shouldReturnABatchingInstanceIfBattchSizeIsGreaterThanOneAndForegoBatchingIsTrue() {
		BatchBuilder factory = new BatchBuilderImpl( 2 );

		Batch batch = factory.buildBatch(
				key,
				new SqlExceptionHelper(),
				true
		);

		assertThat( batch, instanceOf( Batching.class ) );
	}

	@Test
	public void shouldReturnANonBatchingInstanceIfBattchSizeIsGreaterThanOneAndForegoBatchingIsFalse() {
		BatchBuilder factory = new BatchBuilderImpl( 2 );

		Batch batch = factory.buildBatch(
				key,
				new SqlExceptionHelper(),
				false
		);

		assertThat( batch, instanceOf( NonBatching.class ) );
	}

	@Test
	public void shouldReturnANonBatchingInstanceIfBattchSiezeIsOneForegoBatchingIsTrue() {
		BatchBuilder factory = new BatchBuilderImpl( 1 );

		Batch batch = factory.buildBatch(
				key,
				new SqlExceptionHelper(),
				true
		);

		assertThat( batch, instanceOf( NonBatching.class ) );
	}

	@Test
	public void shouldReturnANonBatchingInstanceIfBattchSiezeIsOneForegoBatchingIsFalse() {
		BatchBuilder factory = new BatchBuilderImpl( 1 );

		Batch batch = factory.buildBatch(
				key,
				new SqlExceptionHelper(),
				false
		);

		assertThat( batch, instanceOf( NonBatching.class ) );
	}
}