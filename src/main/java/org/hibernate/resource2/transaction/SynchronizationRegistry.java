/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2011, Red Hat Inc. or third-party contributors as
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
package org.hibernate.resource2.transaction;

import java.io.Serializable;
import javax.transaction.Synchronization;

/**
 * Manages a registry of (local) JTA {@link javax.transaction.Synchronization} instances
 *
 * @author Steve Ebersole
 */
public interface SynchronizationRegistry extends Serializable {
	/**
	 * Register a {@link javax.transaction.Synchronization} callback for this transaction.
	 *
	 * @param synchronization The synchronization callback to register.
	 */
	void registerSynchronization(Synchronization synchronization);

	/**
	 * Delegates the {@link javax.transaction.Synchronization#beforeCompletion} call to each registered Synchronization
	 */
	void notifySynchronizationsBeforeTransactionCompletion();

	/**
	 * Delegates the {@link javax.transaction.Synchronization#afterCompletion} call to each registered Synchronization.  Will also
	 * clear the registered Synchronizations after all have been notified.
	 *
	 * @param status The transaction status, per {@link javax.transaction.Status} constants
	 */
	void notifySynchronizationsAfterTransactionCompletion(int status);
}
