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
package org.hibernate.resource.store;

import org.hibernate.resource.transaction.TransactionCoordinator;

/**
 * Models the context of a connection and transaction with the underlying data store.  This combination of
 * connection and transaction is commonly called a "session" in the  database world (relational, at least).
 *
 * @author Steve Ebersole
 */
public interface DataStoreSession {
	/**
	 * Is this DataStoreSession still open/active.  In other words, has {@link #close} not been called yet?
	 *
	 * @return {@code true} if the DataStoreSession is still open; {@code false} if it is closed.
	 */
	public boolean isOpen();

	/**
	 * Closes the DataStoreSession, making it inactive and forcing release of any held resources.
	 *
	 * @throws org.hibernate.ResourceClosedException if the DataStoreSession is already closed; todo make this no-op?
	 */
	public void close();

	/**
	 * Is the DataStoreSession in a state that would allow it to be serialized?
	 *
	 * @return {@code true} indicates that the DataStoreSession, as is, can be serialized.
	 */
	public boolean isReadyToSerialize();

	/**
	 * Provides access to the TransactionCoordinator for this DataStoreSession
	 *
	 * @return The TransactionCoordinator for this DataStoreSession
	 *
	 * @throws org.hibernate.ResourceClosedException if the DataStoreSession is closed
	 */
	public TransactionCoordinator getTransactionCoordinator();

	/**
	 * Accept an operation to be performed within bounds of this DataStoreSession
	 *
	 * @param operation The operation to perform
	 * @param <T> The operation result type
	 *
	 * @return The operation result, see {@link Operation#perform}
	 */
	public <T> T accept(Operation<T> operation);
}
