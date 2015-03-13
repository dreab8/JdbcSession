/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2014, Red Hat Inc. or third-party contributors as
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
package org.hibernate.resource.transaction.backend.jta.internal;

import javax.transaction.TransactionManager;

import org.hibernate.TransactionException;
import org.hibernate.engine.transaction.internal.jta.JtaStatusHelper;

import org.jboss.logging.Logger;

/**
 * JtaTransactionAdapter for coordinating with the JTA TransactionManager
 *
 * @author Steve Ebersole
 */
public class JtaTransactionAdapterTransactionManagerImpl implements JtaTransactionAdapter {
	private static final Logger log = Logger.getLogger( JtaTransactionAdapterTransactionManagerImpl.class );

	private final TransactionManager transactionManager;

	public JtaTransactionAdapterTransactionManagerImpl(TransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	@Override
	public void begin() {
		try {
			log.trace( "Calling TransactionManager#begin" );
			transactionManager.begin();
			log.trace( "Called TransactionManager#begin" );
		}
		catch (Exception e) {
			throw new TransactionException( "JTA TransactionManager#begin failed", e );
		}
	}

	@Override
	public void commit() {
		try {
			log.trace( "Calling TransactionManager#commit" );
			transactionManager.commit();
			log.trace( "Called TransactionManager#commit" );
		}
		catch (Exception e) {
			throw new TransactionException( "JTA TransactionManager#commit failed", e );
		}
	}

	@Override
	public void rollback() {
		try {
			log.trace( "Calling TransactionManager#rollback" );
			transactionManager.rollback();
			log.trace( "Called TransactionManager#rollback" );
		}
		catch (Exception e) {
			throw new TransactionException( "JTA TransactionManager#rollback failed", e );
		}
	}

	@Override
	public boolean isActive() {
		return JtaStatusHelper.isActive(transactionManager);
	}

	@Override
	public boolean isInitiator() {
		return false;
	}
}
