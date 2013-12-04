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
package org.hibernate.resource2.transaction.internal;

import javax.transaction.Status;

import org.hibernate.resource2.jdbc.spi.JdbcSessionImplementor;
import org.hibernate.resource2.jdbc.spi.PhysicalJdbcTransaction;
import org.hibernate.resource2.transaction.PhysicalTransactionInflow;
import org.hibernate.resource2.transaction.SynchronizationRegistry;
import org.hibernate.resource2.transaction.TransactionCoordinator;

/**
 * An implementation of TransactionCoordinator based on managing a transaction through the JDBC Connection
 *
 * @author Steve Ebersole
 */
public class TransactionCoordinatorJdbcImpl implements TransactionCoordinator {
	private final JdbcSessionImplementor jdbcSession;
	private final SynchronizationRegistryStandardImpl synchronizationRegistry = new SynchronizationRegistryStandardImpl();

	private PhysicalTransactionInflowImpl inflow;

	public TransactionCoordinatorJdbcImpl(JdbcSessionImplementor jdbcSession) {
		this.jdbcSession = jdbcSession;
	}

	@Override
	public PhysicalTransactionInflow getPhysicalTransactionInflow() {
		if ( inflow == null ) {
			inflow = new PhysicalTransactionInflowImpl( jdbcSession.getPhysicalJdbcTransaction() );
		}
		return inflow;
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
		if ( inflow == null ) {
			throw new IllegalStateException( "transaction inflow handle not known on attempt to invalidate" );
		}

		inflow.invalidate();
		inflow = null;
	}

	@Override
	public SynchronizationRegistry getLocalSynchronizations() {
		return synchronizationRegistry;
	}

	public class PhysicalTransactionInflowImpl implements PhysicalTransactionInflow {
		private final PhysicalJdbcTransaction jdbcTransaction;
		private boolean valid = true;

		public PhysicalTransactionInflowImpl(PhysicalJdbcTransaction jdbcTransaction) {
			this.jdbcTransaction = jdbcTransaction;
		}

		private void invalidate() {
			valid = false;
		}

		@Override
		public void begin() {
			errorIfInvalid();

			jdbcTransaction.begin();
			TransactionCoordinatorJdbcImpl.this.afterBegin();
		}

		private void errorIfInvalid() {
			if ( !valid ) {
				throw new IllegalStateException( "transaction inflow handle is no longer valid" );
			}
		}

		@Override
		public void commit() {
			errorIfInvalid();

			TransactionCoordinatorJdbcImpl.this.beforeCompletion();
			jdbcTransaction.commit();
			TransactionCoordinatorJdbcImpl.this.afterCompletion( true );
		}

		@Override
		public void rollback() {
			errorIfInvalid();

			jdbcTransaction.rollback();
			TransactionCoordinatorJdbcImpl.this.afterCompletion( false );
		}
	}
}
