package org.hibernate.test.resource.jdbc.internal;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.HibernateException;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.resource.jdbc.internal.BatchFactoryImpl;
import org.hibernate.resource.jdbc.internal.Batching;
import org.hibernate.resource.jdbc.internal.NonBatching;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchExpectation;
import org.hibernate.resource.jdbc.spi.BatchFactory;
import org.hibernate.resource.jdbc.spi.BatchKey;

import org.junit.Before;
import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class BatchFactoryImplTest {
	private BatchKey key;

	@Before
	public void setUp() {
		final BatchExpectation batchExpectation = new BatchExpectation() {
			@Override
			public void verifyOutcome(int rowCount, PreparedStatement statement, int batchPosition)
					throws SQLException, HibernateException {
			}
		};

		key = new BatchKeyImpl( "UNIMPORTANT", batchExpectation );
	}

	@Test
	public void shouldReturnABatchingInstanceIfBattchSizeIsGreaterThanOneAndForegoBatchingIsTrue() {
		BatchFactory factory = new BatchFactoryImpl( 2 );

		Batch batch = factory.buildBatch(
				key,
				new SqlExceptionHelper(),
				true
		);

		assertThat( batch, instanceOf( Batching.class ) );
	}

	@Test
	public void shouldReturnANonBatchingInstanceIfBattchSizeIsGreaterThanOneAndForegoBatchingIsFalse() {
		BatchFactory factory = new BatchFactoryImpl( 2 );

		Batch batch = factory.buildBatch(
				key,
				new SqlExceptionHelper(),
				false
		);

		assertThat( batch, instanceOf( NonBatching.class ) );
	}

	@Test
	public void shouldReturnANonBatchingInstanceIfBattchSiezeIsOneForegoBatchingIsTrue() {
		BatchFactory factory = new BatchFactoryImpl( 1 );

		Batch batch = factory.buildBatch(
				key,
				new SqlExceptionHelper(),
				true
		);

		assertThat( batch, instanceOf( NonBatching.class ) );
	}

	@Test
	public void shouldReturnANonBatchingInstanceIfBattchSiezeIsOneForegoBatchingIsFalse() {
		BatchFactory factory = new BatchFactoryImpl( 1 );

		Batch batch = factory.buildBatch(
				key,
				new SqlExceptionHelper(),
				false
		);

		assertThat( batch, instanceOf( NonBatching.class ) );
	}
}