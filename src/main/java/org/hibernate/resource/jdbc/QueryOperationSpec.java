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
package org.hibernate.resource.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;
import org.hibernate.resource.jdbc.spi.StatementExecutor;

/**
 * @author Andrea Boriero
 */
public interface QueryOperationSpec extends OperationSpec {

	public QueryStatementBuilder<? extends PreparedStatement> getQueryStatementBuilder();

	public StatementExecutor getStatementExecutor();

	public ParameterBindings getParameterBindings();

	public int getQueryTimeout();

	public String getSql();

	public ResultSetType getResultSetType();

	public ResultSetConcurrency getResultSetConcurrency();

	public int getOffset();

	public int getLimit();

	public enum ResultSetConcurrency {
		READ_ONLY( ResultSet.CONCUR_READ_ONLY ),
		UPDATABLE( ResultSet.CONCUR_UPDATABLE );

		private final int jdbcConstantValue;

		ResultSetConcurrency(int jdbcConstantValue) {
			this.jdbcConstantValue = jdbcConstantValue;
		}

		public int getJdbcConstantValue() {
			return jdbcConstantValue;
		}
	}

	public enum ResultSetType {
		FORWARD_ONLY( ResultSet.TYPE_FORWARD_ONLY ),
		SCROLL_INSENSITIVE( ResultSet.TYPE_SCROLL_INSENSITIVE ),
		SCROLL_SENSITIVE( ResultSet.TYPE_SCROLL_SENSITIVE );

		private final int jdbcConstantValue;

		ResultSetType(int jdbcConstantValue) {
			this.jdbcConstantValue = jdbcConstantValue;
		}

		public int getJdbcConstantValue() {
			return jdbcConstantValue;
		}
	}
}
