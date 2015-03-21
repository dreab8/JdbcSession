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
package org.hibernate.test.resource.jdbc.operationspec;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.jdbc.Expectation;
import org.hibernate.jdbc.Expectations;
import org.hibernate.resource.jdbc.internal.BatchBuilderImpl;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchBuilder;

import static org.hibernate.resource.jdbc.BatchableOperationSpec.BatchableOperationStep.Context;
import static org.hibernate.resource.jdbc.BatchableOperationSpec.BatchableOperationStep.InsertContext;

/**
 * @author Andrea Boriero
 */
public abstract class AbstractBatchableOperationSpecIntegrationTest extends AbstractOperationSpecIntegrationTest {

	public PreparedStatement getStatement(Batch batch, Connection connection, String sql) throws SQLException {
		return getStatement( batch, connection, sql, Expectations.NONE );
	}

	public PreparedStatement getStatement(Batch batch, Connection connection, String sql, Expectation expectation)
			throws SQLException {
		PreparedStatement statement = batch.getStatement( sql, expectation );
		if ( statement == null ) {
			statement = connection.prepareStatement( sql );
		}
		return statement;
	}

	protected abstract int getBatchSize();

	@Override
	protected BatchBuilder getBatchFactory() {
		return new BatchBuilderImpl( getBatchSize() );
	}

	protected List<Context> buildInsertContext(final Serializable id, int steps) {
		List<Context> contexts = new ArrayList<Context>();
		for ( int i = 0; i < steps; i++ ) {
			contexts.add( buildInsertContext( id ) );
		}
		return contexts;
	}

	protected List<Context> buildInsertContext(int steps) {
		return buildInsertContext( null, steps );
	}

	protected Context buildInsertContext(final Serializable id) {
		return new InsertContext() {
			@Override
			public Serializable getId() {
				return id;
			}

			@Override
			public Object[] getState() {
				return new Object[0];
			}

			@Override
			public Object getEntity() {
				return null;
			}

			@Override
			public SessionImplementor getSessionImplementor() {
				return null;
			}

		};
	}

	protected Context buildInsertContext() {
		return buildInsertContext( null );
	}
}
