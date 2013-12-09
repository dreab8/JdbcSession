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

import org.hibernate.resource.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource.jdbc.spi.PhysicalJdbcTransaction;
import org.hibernate.resource.transaction.PhysicalTransactionDelegate;
import org.hibernate.resource.transaction.SynchronizationRegistry;
import org.hibernate.resource.transaction.TransactionCoordinator;

import org.jboss.logging.Logger;

/**
 * An implementation of TransactionCoordinator based on managing a transaction through the JDBC Connection
 *
 * @author Steve Ebersole
 */
public class TransactionCoordinatorJdbcImpl implements TransactionCoordinator {
	private static final Logger log = Logger.getLogger( TransactionCoordinatorJdbcImpl.class );

	private final JdbcSessionImplementor jdbcSession;
	private final SynchronizationRegistryStandardImpl synchronizationRegistry = new SynchronizationRegistryStandardImpl();

	private PhysicalTransactionDelegateImpl physicalTransactionDelegate;

	public TransactionCoordinatorJdbcImpl(JdbcSessionImplementor jdbcSession) {
		this.jdbcSession = jdbcSession;
	}

	@Override
	public PhysicalTransactionDelegate getPhysicalTransactionDelegate() {
		// Again, this PhysicalTransactionDelegate will act as the bridge from the local transaction back into the
		// coordinator.  We lazily build it as we invalidate each delegate after each transaction (a delegate is
		// valid for just one transaction)
		if ( physicalTransactionDelegate == null ) {
			physicalTransactionDelegate = new PhysicalTransactionDelegateImpl( jdbcSession.getPhysicalJdbcTransaction() );
		}
		return physicalTransactionDelegate;
	}

	@Override
	public void pulse() {
		// nothing to do here
	}

	private void afterBegin() {
		log.trace( "TransactionCoordinatorJdbcImpl#afterBegin" );
	}

	private void beforeCompletion() {
		log.trace( "TransactionCoordinatorJdbcImpl#beforeCompletion" );
		synchronizationRegistry.notifySynchronizationsBeforeTransactionCompletion();
	}

	private void afterCompletion(boolean successful) {
		log.tracef( "TransactionCoordinatorJdbcImpl#afterCompletion(%s)", successful );
		final int statusToSend =  successful ? Status.STATUS_COMMITTED : Status.STATUS_UNKNOWN;
		synchronizationRegistry.notifySynchronizationsAfterTransactionCompletion( statusToSend );

		invalidateDelegate();
	}

	private void invalidateDelegate() {
		if ( physicalTransactionDelegate == null ) {
			throw new IllegalStateException( "Physical-transaction delegate not known on attempt to invalidate" );
		}

		physicalTransactionDelegate.invalidate();
		physicalTransactionDelegate = null;
	}

	@Override
	public SynchronizationRegistry getLocalSynchronizations() {
		return synchronizationRegistry;
	}

	/**
	 * The delegate bridging between the local (application facing) transaction and the "physical" notion of a
	 * transaction via the JDBC Connection.
	 */
	public class PhysicalTransactionDelegateImpl implements PhysicalTransactionDelegate {
		private final PhysicalJdbcTransaction physicalJdbcTransaction;
		private boolean valid = true;

		public PhysicalTransactionDelegateImpl(PhysicalJdbcTransaction physicalJdbcTransaction) {
			this.physicalJdbcTransaction = physicalJdbcTransaction;
		}

		private void invalidate() {
			valid = false;
		}

		@Override
		public void begin() {
			errorIfInvalid();

			// initiate the transaction start with the JDBC Connection
			physicalJdbcTransaction.begin();
			// initiate any transaction-related 'after begin' processing
			TransactionCoordinatorJdbcImpl.this.afterBegin();
		}

		private void errorIfInvalid() {
			if ( !valid ) {
				throw new IllegalStateException( "Physical-transaction delegate is no longer valid" );
			}
		}

		@Override
		public void commit() {
			errorIfInvalid();

			// initiate any transaction-related 'before completion' processing (flushes, synchronization notifications, etc)
			TransactionCoordinatorJdbcImpl.this.beforeCompletion();
			// initiate the transaction completion with the JDBC Connection
			physicalJdbcTransaction.commit();
			// initiate any transaction-related 'after completion' processing (closing, synchronization notifications, etc)
			TransactionCoordinatorJdbcImpl.this.afterCompletion( true );

			// NOTE : that the above is very naive currently wrt exception handling
		}

		@Override
		public void rollback() {
			errorIfInvalid();

			// following the JTA spec, we do not perform 'before completion' on rollbacks...

			// initiate the transaction completion with the JDBC Connection
			physicalJdbcTransaction.rollback();
			// initiate any transaction-related 'after completion' processing (closing, synchronization notifications, etc)
			TransactionCoordinatorJdbcImpl.this.afterCompletion( false );
		}
	}
}
