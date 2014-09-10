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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.resource.jdbc.spi.BatchKey;

/**
 * @author Andrea Boriero
 */
public class NonBatching extends AbstractBatchImpl {

	private int rowCount;

	public NonBatching(
			BatchKey key,
			SqlExceptionHelper sqlExceptionHelper) {
		super( key, sqlExceptionHelper );
	}

	@Override
	public PreparedStatement getStatement(String sql) {
		return null;
	}

	@Override
	public void addBatch(String sql, PreparedStatement statement) throws SQLException {
		notifyObserversImplicitExecution();

		rowCount = statement.executeUpdate();
		getExpectation().verifyOutcome( rowCount, statement, 0 );

		close(statement);
	}

	@Override
	public Integer getRowCount(String sql) {
		return rowCount;
	}

	@Override
	public void execute() {
	}

	@Override
	public void release() {
	}
}
