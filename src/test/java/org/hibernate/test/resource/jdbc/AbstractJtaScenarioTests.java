/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2013, Red Hat Inc. or third-party contributors as
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
import java.sql.SQLException;
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.internal.JdbcSessionImpl;
import org.hibernate.resource.jdbc.internal.LogicalConnectionManagedImpl;
import org.hibernate.resource.jdbc.spi.JdbcConnectionAccess;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilderFactory;
import org.hibernate.resource.transaction.backend.jta.internal.JtaTransactionCoordinatorImpl;

import org.hibernate.test.resource.common.SynchronizationCollectorImpl;
import org.hibernate.test.resource.jdbc.common.ConnectionProviderJtaAwareImpl;
import org.hibernate.test.resource.jdbc.common.JdbcSessionContextStandardTestingImpl;
import org.hibernate.test.resource.transaction.common.JtaPlatformStandardTestingImpl;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Steve Ebersole
 */
public abstract class AbstractJtaScenarioTests {
	protected abstract boolean preferUserTransactions();

	private ConnectionProviderJtaAwareImpl connectionProvider;

	@Before
	public void setUp() throws Exception {
		connectionProvider = new ConnectionProviderJtaAwareImpl();
	}

	private JdbcSession buildJdbcSession() {
		return buildJdbcSession( true );
	}

	private JdbcSession buildJdbcSession(final boolean autoJoin) {
		return  new JdbcSessionImpl(
				JdbcSessionContextStandardTestingImpl.INSTANCE,
				new LogicalConnectionManagedImpl(
						new JdbcConnectionAccess() {
							@Override
							public Connection obtainConnection() throws SQLException {
								return connectionProvider.getConnection();
							}

							@Override
							public void releaseConnection(Connection connection) throws SQLException {
								connectionProvider.closeConnection( connection );
							}
						},
						JdbcSessionContextStandardTestingImpl.INSTANCE
				),
				TransactionCoordinatorBuilderFactory.INSTANCE.forJta()
						.setJtaPlatform( JtaPlatformStandardTestingImpl.INSTANCE )
						.setAutoJoinTransactions( autoJoin )
						.setPreferUserTransactions( preferUserTransactions() )
						.setPerformJtaThreadTracking( false )
		);
	}


	@Test
	public void basicBmtUsageTest() throws Exception {
		final JdbcSession jdbcSession = buildJdbcSession();
		final JtaTransactionCoordinatorImpl transactionCoordinator = (JtaTransactionCoordinatorImpl) jdbcSession.getTransactionCoordinator();

		try {
			final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
			assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
			assertFalse( transactionCoordinator.isSynchronizationRegistered() );

			jdbcSession.getTransactionCoordinator().getTransactionDriverControl().begin();
			assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );
			assertTrue( transactionCoordinator.isSynchronizationRegistered() );
			SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
			transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

			jdbcSession.getTransactionCoordinator().getTransactionDriverControl().commit();
			assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
			assertFalse( transactionCoordinator.isSynchronizationRegistered() );
			assertEquals( 1, localSync.getBeforeCompletionCount() );
			assertEquals( 1, localSync.getSuccessfulCompletionCount() );
			assertEquals( 0, localSync.getFailedCompletionCount() );
		}
		finally {
			jdbcSession.close();
		}
	}

	@Test
	public void basicCmtUsageTest() throws Exception {
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );

		tm.begin();

		assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );

		final JdbcSession jdbcSession = buildJdbcSession();
		final JtaTransactionCoordinatorImpl transactionCoordinator = (JtaTransactionCoordinatorImpl) jdbcSession.getTransactionCoordinator();

		try {
			assertTrue( transactionCoordinator.isSynchronizationRegistered() );

			SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
			transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

			tm.commit();
			assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
			assertFalse( transactionCoordinator.isSynchronizationRegistered() );
			assertEquals( 1, localSync.getBeforeCompletionCount() );
			assertEquals( 1, localSync.getSuccessfulCompletionCount() );
			assertEquals( 0, localSync.getFailedCompletionCount() );
		}
		finally {
			jdbcSession.close();
		}
	}

	@Test
	public void explicitJoiningTest() throws Exception {
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );

		tm.begin();

		assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );

		final JdbcSession jdbcSession = buildJdbcSession( false );
		final JtaTransactionCoordinatorImpl transactionCoordinator = (JtaTransactionCoordinatorImpl) jdbcSession.getTransactionCoordinator();

		try {
			assertFalse( transactionCoordinator.isSynchronizationRegistered() );
			transactionCoordinator.explicitJoin();
			assertTrue( transactionCoordinator.isSynchronizationRegistered() );

			SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
			transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

			tm.commit();
			assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
			assertFalse( transactionCoordinator.isSynchronizationRegistered() );
			assertEquals( 1, localSync.getBeforeCompletionCount() );
			assertEquals( 1, localSync.getSuccessfulCompletionCount() );
			assertEquals( 0, localSync.getFailedCompletionCount() );
		}
		finally {
			jdbcSession.close();
		}
	}

	@Test
	public void jpaJoiningTest() throws Exception {
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );

		final JdbcSession jdbcSession = buildJdbcSession( false );
		final JtaTransactionCoordinatorImpl transactionCoordinator = (JtaTransactionCoordinatorImpl) jdbcSession.getTransactionCoordinator();

		try {

			assertFalse( transactionCoordinator.isSynchronizationRegistered() );

			tm.begin();

			assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );
			assertFalse( transactionCoordinator.isSynchronizationRegistered() );

			transactionCoordinator.explicitJoin();
			assertTrue( transactionCoordinator.isSynchronizationRegistered() );

			SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
			transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

			tm.commit();
			assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
			assertFalse( transactionCoordinator.isSynchronizationRegistered() );
			assertEquals( 1, localSync.getBeforeCompletionCount() );
			assertEquals( 1, localSync.getSuccessfulCompletionCount() );
			assertEquals( 0, localSync.getFailedCompletionCount() );
		}
		finally {
			jdbcSession.close();
		}
	}
}
