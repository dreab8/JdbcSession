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

import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.builder.TransactionCoordinatorBuilderFactory;
import org.hibernate.resource.transaction.backend.local.internal.ResourceLocalTransactionCoordinatorImpl;

import org.hibernate.test.resource.common.SynchronizationCollectorImpl;
import org.hibernate.test.resource.transaction.common.ResourceLocalTransactionAccessTestingImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Steve Ebersole
 */
public class BasicResourceLocalTransactionsTest {

	@Test
	public void basicUsageTest() throws Exception {
		final ResourceLocalTransactionAccessTestingImpl owner = new ResourceLocalTransactionAccessTestingImpl();

		final TransactionCoordinator tc = TransactionCoordinatorBuilderFactory.INSTANCE.forResourceLocal()
				.buildTransactionCoordinator( owner );
		final ResourceLocalTransactionCoordinatorImpl transactionCoordinator = (ResourceLocalTransactionCoordinatorImpl) tc;

		SynchronizationCollectorImpl sync = new SynchronizationCollectorImpl();
		transactionCoordinator.getLocalSynchronizations().registerSynchronization( sync );

		transactionCoordinator.getPhysicalTransactionDelegate().begin();
		assertFalse( owner.getJdbcConnection().getAutoCommit() );
		assertEquals( 0, sync.getBeforeCompletionCount() );
		assertEquals( 0, sync.getSuccessfulCompletionCount() );
		assertEquals( 0, sync.getFailedCompletionCount() );

		transactionCoordinator.getPhysicalTransactionDelegate().commit();
		assertEquals( 1, sync.getBeforeCompletionCount() );
		assertEquals( 1, sync.getSuccessfulCompletionCount() );
		assertEquals( 0, sync.getFailedCompletionCount() );

	}
}
