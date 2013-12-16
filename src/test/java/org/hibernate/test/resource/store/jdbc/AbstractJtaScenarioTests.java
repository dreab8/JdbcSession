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
package org.hibernate.test.resource.store.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.resource.store.jdbc.JdbcSession;
import org.hibernate.resource.store.jdbc.internal.JdbcSessionImpl;
import org.hibernate.resource.store.jdbc.internal.LogicalConnectionManagedImpl;
import org.hibernate.resource.store.jdbc.spi.JdbcConnectionAccess;
import org.hibernate.resource.transaction.TransactionCoordinatorBuilderFactory;
import org.hibernate.resource.transaction.internal.TransactionCoordinatorJtaImpl;

import org.hibernate.test.resource.store.jdbc.common.ConnectionProviderJtaAwareImpl;
import org.hibernate.test.resource.store.jdbc.common.JdbcSessionContextStandardTestingImpl;
import org.hibernate.test.resource.transaction.common.JtaPlatformStandardTestingImpl;
import org.hibernate.test.resource.common.SynchronizationCollectorImpl;
import org.junit.After;
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
		HashMap<String,String> props = new HashMap<String,String>();
		props.put( AvailableSettings.DRIVER, "org.h2.Driver" );
		props.put( AvailableSettings.URL, "jdbc:h2:mem:db1" );
		props.put( AvailableSettings.USER, "sa" );
		props.put( AvailableSettings.PASS, "" );

		connectionProvider = new ConnectionProviderJtaAwareImpl();
		connectionProvider.configure( props );
	}

	@After
	public void tearDown() throws Exception {
		connectionProvider.stop();
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
		final TransactionCoordinatorJtaImpl transactionCoordinator = (TransactionCoordinatorJtaImpl) jdbcSession.getTransactionCoordinator();

		try {
			final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
			assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
			assertFalse( transactionCoordinator.isSynchronizationRegistered() );

			jdbcSession.getTransactionCoordinator().getPhysicalTransactionDelegate().begin();
			assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );
			assertTrue( transactionCoordinator.isSynchronizationRegistered() );
			SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
			transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

			jdbcSession.getTransactionCoordinator().getPhysicalTransactionDelegate().commit();
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
		final TransactionCoordinatorJtaImpl transactionCoordinator = (TransactionCoordinatorJtaImpl) jdbcSession.getTransactionCoordinator();

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
		final TransactionCoordinatorJtaImpl transactionCoordinator = (TransactionCoordinatorJtaImpl) jdbcSession.getTransactionCoordinator();

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
		final TransactionCoordinatorJtaImpl transactionCoordinator = (TransactionCoordinatorJtaImpl) jdbcSession.getTransactionCoordinator();

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
