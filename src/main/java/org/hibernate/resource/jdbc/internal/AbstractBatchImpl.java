/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) {DATE}, Red Hat Inc. or third-party contributors as
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
package org.hibernate.resource.jdbc.internal;

import java.sql.Statement;
import java.util.LinkedHashSet;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.jdbc.Expectation;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchKey;
import org.hibernate.resource.jdbc.spi.BatchObserver;

/**
 * @author Andrea Boriero
 */
public abstract class AbstractBatchImpl implements Batch {
	private static final CoreMessageLogger log = CoreLogging.messageLogger( AbstractBatchImpl.class );

	private BatchKey key;

	private LinkedHashSet<BatchObserver> observers = new LinkedHashSet<BatchObserver>();
	private SqlExceptionHelper sqlExceptionHelper;

	public AbstractBatchImpl(
			BatchKey key,
			SqlExceptionHelper sqlExceptionHelper) {
		this.key = key;
		this.sqlExceptionHelper = sqlExceptionHelper;
	}

	@Override
	public BatchKey getKey() {
		return key;
	}

	@Override
	public void addObserver(BatchObserver observer) {
		observers.add( observer );
	}

	protected SqlExceptionHelper getSqlExceptionHelper() {
		return sqlExceptionHelper;
	}

	protected final void notifyObserversExplicitExecution() {
		for ( BatchObserver observer : observers ) {
			observer.batchExplicitlyExecuted();
		}
	}

	protected final void notifyObserversImplicitExecution() {
		for ( BatchObserver observer : observers ) {
			observer.batchImplicitlyExecuted();
		}
	}

	protected Expectation getExpectation() {
		return getKey().getExpectation();
	}

	protected void close(Statement statement) {
		if ( statement != null ) {
			ResourceRegistryStandardImpl.close( statement );
		}
	}
}
