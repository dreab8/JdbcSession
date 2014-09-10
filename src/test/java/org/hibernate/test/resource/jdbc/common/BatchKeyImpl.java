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
package org.hibernate.test.resource.jdbc.common;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.HibernateException;
import org.hibernate.resource.jdbc.spi.BatchExpectation;
import org.hibernate.resource.jdbc.spi.BatchKey;

/**
 * @author Andrea Boriero
 */
public class BatchKeyImpl implements BatchKey {
	private BatchExpectation expectation;
	private String comparison;

	public BatchKeyImpl(String comparison, BatchExpectation expectation) {
		this.expectation = expectation;
		this.comparison = comparison;
	}

	public BatchKeyImpl(String comparison) {
		this.expectation = new BatchExpectation() {
			@Override
			public void verifyOutcome(int rowCount, PreparedStatement statement, int batchPosition)
					throws SQLException, HibernateException {

			}
		};
		this.comparison = comparison;
	}

	@Override
	public BatchExpectation getExpectation() {
		return expectation;
	}

	@Override
	public boolean equals(Object o) {
		if ( this == o ) {
			return true;
		}
		if ( o == null || getClass() != o.getClass() ) {
			return false;
		}

		final BatchKeyImpl that = (BatchKeyImpl) o;
		return comparison.equals( that.comparison );
	}

	@Override
	public int hashCode() {
		return comparison.hashCode();
	}
}
