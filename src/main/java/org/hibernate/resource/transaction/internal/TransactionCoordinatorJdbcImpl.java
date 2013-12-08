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

/**
 * An implementation of TransactionCoordinator based on managing a transaction through the JDBC Connection
 *
 * @author Steve Ebersole
 */
public class TransactionCoordinatorJdbcImpl implements TransactionCoordinator {
	private final JdbcSessionImplementor jdbcSession;
	private final SynchronizationRegistryStandardImpl synchronizationRegistry = new SynchronizationRegistryStandardImpl();

	private PhysicalTransactionDelegateImpl physicalTransactionDelegate;

	public TransactionCoordinatorJdbcImpl(JdbcSessionImplementor jdbcSession) {
		this.jdbcSession = jdbcSession;
	}

	@Override
	public PhysicalTransactionDelegate getPhysicalTransactionDelegate() {
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
	}

	private void beforeCompletion() {
		synchronizationRegistry.notifySynchronizationsBeforeTransactionCompletion();
	}

	private void afterCompletion(boolean successful) {
		final int statusToSend =  successful ? Status.STATUS_COMMITTED : Status.STATUS_UNKNOWN;
		synchronizationRegistry.notifySynchronizationsAfterTransactionCompletion( statusToSend );

		invalidateInflow();
	}

	private void invalidateInflow() {
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

			physicalJdbcTransaction.begin();
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

			TransactionCoordinatorJdbcImpl.this.beforeCompletion();
			physicalJdbcTransaction.commit();
			TransactionCoordinatorJdbcImpl.this.afterCompletion( true );
		}

		@Override
		public void rollback() {
			errorIfInvalid();

			physicalJdbcTransaction.rollback();
			TransactionCoordinatorJdbcImpl.this.afterCompletion( false );
		}
	}
}
