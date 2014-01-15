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
package org.hibernate.test.resource.transaction;

import javax.transaction.Status;
import javax.transaction.TransactionManager;

import org.hibernate.resource.transaction.builder.TransactionCoordinatorBuilderFactory;
import org.hibernate.resource.transaction.backend.jta.internal.JtaTransactionCoordinatorImpl;
import org.hibernate.resource.transaction.backend.jta.internal.synchronization.SynchronizationCallbackCoordinatorTrackingImpl;

import org.hibernate.test.resource.common.SynchronizationCollectorImpl;
import org.hibernate.test.resource.transaction.common.JtaPlatformStandardTestingImpl;
import org.hibernate.test.resource.transaction.common.TransactionCoordinatorOwnerTestingImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Steve Ebersole
 */
public abstract class AbstractBasicJtaTestScenarios {
	private final TransactionCoordinatorOwnerTestingImpl owner = new TransactionCoordinatorOwnerTestingImpl();

	protected abstract boolean preferUserTransactions();

	public JtaTransactionCoordinatorImpl buildTransactionCoordinator(boolean autoJoin) {
		return (JtaTransactionCoordinatorImpl) TransactionCoordinatorBuilderFactory.INSTANCE.forJta()
				.setJtaPlatform( JtaPlatformStandardTestingImpl.INSTANCE )
				.setAutoJoinTransactions( autoJoin )
				.setPreferUserTransactions( preferUserTransactions() )
				.setPerformJtaThreadTracking( false )
				.buildTransactionCoordinator( owner );
	}

	@Test
	public void basicBmtUsageTest() throws Exception {
		final JtaTransactionCoordinatorImpl transactionCoordinator = buildTransactionCoordinator( true );

		// pre conditions
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );

		// begin the transaction
		transactionCoordinator.getPhysicalTransactionDelegate().begin();
		assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );
		assertTrue( transactionCoordinator.isSynchronizationRegistered() );

		// create and add a local Synchronization
		SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
		transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

		// commit the transaction
		transactionCoordinator.getPhysicalTransactionDelegate().commit();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );
		assertEquals( 1, localSync.getBeforeCompletionCount() );
		assertEquals( 1, localSync.getSuccessfulCompletionCount() );
		assertEquals( 0, localSync.getFailedCompletionCount() );
	}

	@Test
	public void rollbackBmtUsageTest() throws Exception {
		final JtaTransactionCoordinatorImpl transactionCoordinator = buildTransactionCoordinator( true );

		// pre conditions
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );

		// begin the transaction
		transactionCoordinator.getPhysicalTransactionDelegate().begin();
		assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );
		assertTrue( transactionCoordinator.isSynchronizationRegistered() );

		// create and add a local Synchronization
		SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
		transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

		// rollback the transaction
		transactionCoordinator.getPhysicalTransactionDelegate().rollback();

		// post conditions
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );
		assertEquals( 0, localSync.getBeforeCompletionCount() );
		assertEquals( 0, localSync.getSuccessfulCompletionCount() );
		assertEquals( 1, localSync.getFailedCompletionCount() );
	}

	@Test
	public void basicCmtUsageTest() throws Exception {
		// pre conditions
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );

		// begin the transaction
		tm.begin();

		final JtaTransactionCoordinatorImpl transactionCoordinator = buildTransactionCoordinator( true );
		assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );
		// NOTE : because of auto-join
		assertTrue( transactionCoordinator.isSynchronizationRegistered() );

		// create and add a local Synchronization
		SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
		transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

		// commit the transaction
		tm.commit();

		// post conditions
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );
		assertEquals( 1, localSync.getBeforeCompletionCount() );
		assertEquals( 1, localSync.getSuccessfulCompletionCount() );
		assertEquals( 0, localSync.getFailedCompletionCount() );
	}

	@Test
	public void basicCmtUsageWithPulseTest() throws Exception {
		// pre conditions
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );

		final JtaTransactionCoordinatorImpl transactionCoordinator = buildTransactionCoordinator( true );

		// begin the transaction
		tm.begin();
		assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );

		transactionCoordinator.pulse();
		// NOTE : because of auto-join
		assertTrue( transactionCoordinator.isSynchronizationRegistered() );
		transactionCoordinator.pulse();
		assertTrue( transactionCoordinator.isSynchronizationRegistered() );

		// create and add a local Synchronization
		SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
		transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

		// commit the transaction
		tm.commit();

		// post conditions
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );
		assertEquals( 1, localSync.getBeforeCompletionCount() );
		assertEquals( 1, localSync.getSuccessfulCompletionCount() );
		assertEquals( 0, localSync.getFailedCompletionCount() );
	}

	@Test
	public void rollbackCmtUsageTest() throws Exception {
		// pre conditions
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );

		// begin the transaction
		tm.begin();
		assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );

		final JtaTransactionCoordinatorImpl transactionCoordinator = buildTransactionCoordinator( true );
		// NOTE : because of auto-join
		assertTrue( transactionCoordinator.isSynchronizationRegistered() );

		// create and add a local Synchronization
		SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
		transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

		// rollback the transaction
		tm.rollback();

		// post conditions
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );
		assertEquals( 0, localSync.getBeforeCompletionCount() );
		assertEquals( 0, localSync.getSuccessfulCompletionCount() );
		assertEquals( 1, localSync.getFailedCompletionCount() );
	}

	@Test
	public void explicitJoiningTest() throws Exception {
		// pre conditions
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );

		final JtaTransactionCoordinatorImpl transactionCoordinator = buildTransactionCoordinator( false );

		// begin the transaction
		tm.begin();

		assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );

		// no auto-join now
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );
		transactionCoordinator.explicitJoin();
		assertTrue( transactionCoordinator.isSynchronizationRegistered() );

		// create and add a local Synchronization
		SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
		transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

		// commit the transaction
		tm.commit();

		// post conditions
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );
		assertEquals( 1, localSync.getBeforeCompletionCount() );
		assertEquals( 1, localSync.getSuccessfulCompletionCount() );
		assertEquals( 0, localSync.getFailedCompletionCount() );
	}

	@Test
	public void jpaExplicitJoiningTest() throws Exception {
		// pre conditions
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );

		// begin the transaction
		tm.begin();

		assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );

		final JtaTransactionCoordinatorImpl transactionCoordinator = buildTransactionCoordinator( false );
		// no auto-join now
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );
		transactionCoordinator.explicitJoin();
		assertTrue( transactionCoordinator.isSynchronizationRegistered() );

		// create and add a local Synchronization
		SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
		transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

		// commit the transaction
		tm.commit();

		// post conditions
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );
		assertEquals( 1, localSync.getBeforeCompletionCount() );
		assertEquals( 1, localSync.getSuccessfulCompletionCount() );
		assertEquals( 0, localSync.getFailedCompletionCount() );
	}

	@Test
	public void assureMultipleJoinCallsNoOp() throws Exception {
		// pre conditions
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );

		// begin the transaction
		tm.begin();

		assertEquals( Status.STATUS_ACTIVE, tm.getStatus() );

		final JtaTransactionCoordinatorImpl transactionCoordinator = buildTransactionCoordinator( false );
		// no auto-join now
		assertFalse( transactionCoordinator.isJoined() );
		transactionCoordinator.explicitJoin();
		assertTrue( transactionCoordinator.isJoined() );
		transactionCoordinator.explicitJoin();
		transactionCoordinator.explicitJoin();
		transactionCoordinator.explicitJoin();
		transactionCoordinator.explicitJoin();
		assertTrue( transactionCoordinator.isJoined() );

		// create and add a local Synchronization
		SynchronizationCollectorImpl localSync = new SynchronizationCollectorImpl();
		transactionCoordinator.getLocalSynchronizations().registerSynchronization( localSync );

		// commit the transaction
		tm.commit();

		// post conditions
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
		assertFalse( transactionCoordinator.isSynchronizationRegistered() );
		assertEquals( 1, localSync.getBeforeCompletionCount() );
		assertEquals( 1, localSync.getSuccessfulCompletionCount() );
		assertEquals( 0, localSync.getFailedCompletionCount() );
	}

	@Test
	public void basicThreadCheckingUsage() throws Exception {
		JtaTransactionCoordinatorImpl transactionCoordinator = (JtaTransactionCoordinatorImpl) TransactionCoordinatorBuilderFactory.INSTANCE.forJta()
				.setJtaPlatform( JtaPlatformStandardTestingImpl.INSTANCE )
				.setAutoJoinTransactions( true )
				.setPreferUserTransactions( preferUserTransactions() )
				.setPerformJtaThreadTracking( true )
				.buildTransactionCoordinator( owner );

		// pre conditions
		final TransactionManager tm = JtaPlatformStandardTestingImpl.INSTANCE.transactionManager();
		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );

		// begin the transaction
		tm.begin();
		transactionCoordinator.explicitJoin();
		assertEquals(
				SynchronizationCallbackCoordinatorTrackingImpl.class,
				transactionCoordinator.getSynchronizationCallbackCoordinator().getClass()
		);

		tm.commit();


		assertEquals( Status.STATUS_NO_TRANSACTION, tm.getStatus() );
		assertFalse( transactionCoordinator.isJoined() );

		tm.begin();
		transactionCoordinator.explicitJoin();
		assertEquals(
				SynchronizationCallbackCoordinatorTrackingImpl.class,
				transactionCoordinator.getSynchronizationCallbackCoordinator().getClass()
		);

		tm.rollback();
	}
}
