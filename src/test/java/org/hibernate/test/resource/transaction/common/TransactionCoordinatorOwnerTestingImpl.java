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
package org.hibernate.test.resource.transaction.common;

import org.hibernate.resource.transaction.TransactionCoordinatorOwner;

/**
 * @author Steve Ebersole
 */
public class TransactionCoordinatorOwnerTestingImpl implements TransactionCoordinatorOwner {
	private boolean active;

	private int beforeCompletionCount;
	private int successfulCompletionCount;
	private int failedCompletionCount;

	public TransactionCoordinatorOwnerTestingImpl() {
		this( true );
	}

	public TransactionCoordinatorOwnerTestingImpl(boolean active) {
		this.active = active;
	}

	public int getBeforeCompletionCount() {
		return beforeCompletionCount;
	}

	public int getSuccessfulCompletionCount() {
		return successfulCompletionCount;
	}

	public int getFailedCompletionCount() {
		return failedCompletionCount;
	}

	public void reset() {
		beforeCompletionCount = 0;
		successfulCompletionCount = 0;
		failedCompletionCount = 0;
	}

	public void deactivate() {
		active = false;
	}

	@Override
	public boolean isActive() {
		return active;
	}

	@Override
	public void beforeTransactionCompletion() {
		beforeCompletionCount++;
	}

	@Override
	public void afterTransactionCompletion(boolean successful) {
		if ( successful ) {
			successfulCompletionCount++;
		}
		else {
			failedCompletionCount++;
		}

	}
}
