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
 * Free Software Foundation, Inc.o
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.hibernate.resource.jdbc.internal;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.jdbc.Expectation;
import org.hibernate.resource.jdbc.spi.BatchKey;

/**
 * @author Andrea Boriero
 */
public class Batching extends AbstractBatchImpl {
	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( Batching.class );

	private int batchSize;
	private int batchPosition;

	private Map<String, PreparedStatement> batchStatements = new LinkedHashMap<String, PreparedStatement>();
	private Map<String, Expectation> expectations = new LinkedHashMap<String, Expectation>();

	public Batching(
			BatchKey key,
			int batchSize,
			SqlExceptionHelper sqlExceptionHelper) {
		super( key, sqlExceptionHelper );
		this.batchSize = batchSize;
	}

	@Override
	public void advance() {
		batchPosition++;
		if ( batchPosition == batchSize ) {
			notifyObserversImplicitExecution();
			performExecution();
		}
	}

	@Override
	public PreparedStatement getStatement(String sql, Expectation expectation) {
		expectations.put( sql, expectation );
		return batchStatements.get( sql );
	}

	@Override
	public void addBatch(String sql, PreparedStatement statement) throws SQLException {
		statement.addBatch();
		batchStatements.put( sql, statement );
	}

	@Override
	public Integer getRowCount(String sql) {
		return -1;
	}

	@Override
	public void execute() {
		notifyObserversExplicitExecution();

		if ( batchPosition == 0 ) {
			LOG.debug( "No batched statements to execute" );
		}
		else {
			LOG.debugf( "Executing batch size: %s", batchPosition );
			performExecution();
		}
	}

	@Override
	public void release() {
		for ( Map.Entry<String, PreparedStatement> entry : batchStatements.entrySet() ) {
			final PreparedStatement statement = entry.getValue();
			clearStatementBatch( statement );
			close( statement );
		}
		batchStatements.clear();
	}

	private void performExecution() {
		try {
			for ( Map.Entry<String, PreparedStatement> entry : batchStatements.entrySet() ) {
				try {
					final PreparedStatement statement = entry.getValue();
					final int[] rowCounts = statement.executeBatch();
					verifyOutcome( expectations.get( entry.getKey() ), statement, rowCounts );
				}
				catch (SQLException e) {
					throw getSqlExceptionHelper().convert( e, "could not execute batch", entry.getKey() );
				}
			}
			batchPosition = 0;
		}
		finally {
			release();
		}
	}

	private void verifyOutcome(Expectation expectation, PreparedStatement statement, int[] rowCounts)
			throws SQLException {
		final int numberOfRowCounts = rowCounts.length;
		if ( numberOfRowCounts != batchPosition ) {
			LOG.unexpectedRowCounts();
		}
		for ( int i = 0; i < numberOfRowCounts; i++ ) {
			expectation.verifyOutcome( rowCounts[i], statement, i );
		}
	}

	private void clearStatementBatch(PreparedStatement statement) {
		try {
			statement.clearBatch();
		}
		catch (SQLException e) {
			LOG.unableToReleaseBatchStatement();
		}
	}
}
