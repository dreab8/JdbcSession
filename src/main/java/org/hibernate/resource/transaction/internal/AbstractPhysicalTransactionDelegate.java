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

import org.hibernate.resource.transaction.PhysicalTransactionDelegate;

import org.jboss.logging.Logger;

/**
 * Template support for PhysicalTransactionDelegate implementations
 *
 * @author Steve Ebersole
 */
public abstract class AbstractPhysicalTransactionDelegate implements PhysicalTransactionDelegate {
	private static final Logger log = Logger.getLogger( AbstractPhysicalTransactionDelegate.class );

	private boolean invalid;

	protected AbstractPhysicalTransactionDelegate() {
	}

	protected void invalidate() {
		invalid = true;
	}

	public boolean isValid() {
		return !invalid;
	}

	@Override
	public void begin() {
		errorIfInvalid();

		doBegin();
		afterBegin();
	}

	protected void errorIfInvalid() {
		if ( invalid ) {
			throw new IllegalStateException( "Physical-transaction delegate is no longer valid" );
		}
	}

	protected abstract void doBegin();

	protected void afterBegin() {
		log.trace( "AbstractPhysicalTransactionDelegate#afterBegin" );
	}

	@Override
	public void commit() {
		errorIfInvalid();

		beforeCompletion();
		doCommit();
		afterCompletion( true );
	}

	protected void beforeCompletion() {
		log.trace( "AbstractPhysicalTransactionDelegate#beforeCompletion" );
	}

	protected abstract void doCommit();

	protected void afterCompletion(boolean successful) {
		log.tracef( "AbstractPhysicalTransactionDelegate#afterCompletion(%s)", successful );
	}

	@Override
	public void rollback() {
		errorIfInvalid();

		doRollback();
		afterCompletion( false );
	}

	protected abstract void doRollback();
}
