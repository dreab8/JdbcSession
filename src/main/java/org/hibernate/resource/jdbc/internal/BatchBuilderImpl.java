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

import java.util.Map;

import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchBuilder;
import org.hibernate.resource.jdbc.spi.BatchKey;
import org.hibernate.service.spi.Configurable;

/**
 * @author Andrea Boriero
 */
public class BatchBuilderImpl implements BatchBuilder, Configurable {

	private int batchSize;

	/**
	 * Constructs a BatchBuilderImpl
	 */
	public BatchBuilderImpl() {
	}

	/**
	 * Constructs a BatchBuilderImpl
	 *
	 * @param batchSize The batch size to use.
	 */
	public BatchBuilderImpl(int batchSize) {
		this.batchSize = batchSize;
	}

	@Override
	public void configure(Map configurationValues) {
		batchSize = ConfigurationHelper.getInt( Environment.STATEMENT_BATCH_SIZE, configurationValues, batchSize );
	}

	@Override
	public Batch buildBatch(
			BatchKey key, SqlExceptionHelper exceptionHelper, boolean foregoBatching) {
		return batchSize > 1 && foregoBatching
				? new Batching( key, batchSize, exceptionHelper )
				: new NonBatching( key, exceptionHelper );
	}

	@Override
	public String getManagementDomain() {
		// use Hibernate default domain
		return null;
	}

	@Override
	public String getManagementServiceType() {
		// use Hibernate default scheme
		return null;
	}

	@Override
	public Object getManagementBean() {
		return this;
	}

}
