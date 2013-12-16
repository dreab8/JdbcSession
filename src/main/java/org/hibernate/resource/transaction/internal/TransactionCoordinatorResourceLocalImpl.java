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
package org.hibernate.resource.transaction.internal;

import javax.transaction.Status;

import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.resource.transaction.PhysicalTransactionDelegate;
import org.hibernate.resource.transaction.ResourceLocalTransaction;
import org.hibernate.resource.transaction.SynchronizationRegistry;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.spi.ResourceLocalTransactionCoordinatorOwner;

import static org.hibernate.internal.CoreLogging.messageLogger;

/**
 * An implementation of TransactionCoordinator based on managing a transaction through the data-store
 * specific ResourceLocalTransaction.
 *
 * @author Steve Ebersole
 *
 * @see org.hibernate.resource.transaction.ResourceLocalTransaction
 */
public class TransactionCoordinatorResourceLocalImpl implements TransactionCoordinator {
	private static final CoreMessageLogger log = messageLogger( TransactionCoordinatorResourceLocalImpl.class );

	private final ResourceLocalTransactionCoordinatorOwner owner;
	private final SynchronizationRegistryStandardImpl synchronizationRegistry = new SynchronizationRegistryStandardImpl();

	private PhysicalTransactionDelegateImpl physicalTransactionDelegate;

	/**
	 * Construct a TransactionCoordinatorResourceLocalImpl instance.  package-protected to ensure access goes through
	 * builder.
	 *
	 * @param owner The owner
	 */
	TransactionCoordinatorResourceLocalImpl(ResourceLocalTransactionCoordinatorOwner owner) {
		this.owner = owner;
	}

	@Override
	public PhysicalTransactionDelegate getPhysicalTransactionDelegate() {
		// Again, this PhysicalTransactionDelegate will act as the bridge from the local transaction back into the
		// coordinator.  We lazily build it as we invalidate each delegate after each transaction (a delegate is
		// valid for just one transaction)
		if ( physicalTransactionDelegate == null ) {
			physicalTransactionDelegate = new PhysicalTransactionDelegateImpl( owner.getResourceLocalTransaction() );
		}
		return physicalTransactionDelegate;
	}

	@Override
	public void explicitJoin() {
		// nothing to do here, but log a warning
		log.callingJoinTransactionOnNonJtaEntityManager();
	}

	@Override
	public boolean isJoined() {
		log.debug( "Calling TransactionCoordinator#isJoined in resource-local mode always returns false" );
		return false;
	}

	@Override
	public void pulse() {
		// nothing to do here
	}

	@Override
	public SynchronizationRegistry getLocalSynchronizations() {
		return synchronizationRegistry;
	}


	// PhysicalTransactionDelegate ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

	private void afterBeginCallback() {
		log.trace( "TransactionCoordinatorResourceLocalImpl#afterBeginCallback" );
	}
	private void beforeCompletionCallback() {
		log.trace( "TransactionCoordinatorResourceLocalImpl#beforeCompletionCallback" );
		owner.beforeTransactionCompletion();
		synchronizationRegistry.notifySynchronizationsBeforeTransactionCompletion();
	}

	private void afterCompletionCallback(boolean successful) {
		log.tracef( "TransactionCoordinatorResourceLocalImpl#afterCompletionCallback(%s)", successful );
		final int statusToSend =  successful ? Status.STATUS_COMMITTED : Status.STATUS_UNKNOWN;
		synchronizationRegistry.notifySynchronizationsAfterTransactionCompletion( statusToSend );

		owner.afterTransactionCompletion( successful );

		invalidateDelegate();
	}

	private void invalidateDelegate() {
		if ( physicalTransactionDelegate == null ) {
			throw new IllegalStateException( "Physical-transaction delegate not known on attempt to invalidate" );
		}

		physicalTransactionDelegate.invalidate();
		physicalTransactionDelegate = null;
	}


	/**
	 * The delegate bridging between the local (application facing) transaction and the "physical" notion of a
	 * transaction via the JDBC Connection.
	 */
	public class PhysicalTransactionDelegateImpl extends AbstractPhysicalTransactionDelegate {
		private final ResourceLocalTransaction resourceLocalTransaction;

		public PhysicalTransactionDelegateImpl(ResourceLocalTransaction resourceLocalTransaction) {
			super();
			this.resourceLocalTransaction = resourceLocalTransaction;
		}

		@Override
		protected void doBegin() {
			// initiate the transaction start with the JDBC Connection
			resourceLocalTransaction.begin();
		}

		@Override
		protected void afterBegin() {
			super.afterBegin();
			TransactionCoordinatorResourceLocalImpl.this.afterBeginCallback();
		}

		@Override
		protected void beforeCompletion() {
			super.beforeCompletion();
			TransactionCoordinatorResourceLocalImpl.this.beforeCompletionCallback();
		}

		@Override
		protected void doCommit() {
			// initiate the transaction completion with the JDBC Connection
			resourceLocalTransaction.commit();
		}

		@Override
		protected void afterCompletion(boolean successful) {
			super.afterCompletion( successful );
			TransactionCoordinatorResourceLocalImpl.this.afterCompletionCallback( successful );
		}

		@Override
		protected void doRollback() {
			// initiate the transaction completion with the JDBC Connection
			resourceLocalTransaction.rollback();
		}
	}
}
