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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.resource.jdbc.spi.InsertWithGeneratedKeyStatementBuilder;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;

/**
 * @author Andrea Boriero
 */
public class StandardInsertWithGeneratedKeyStatementBuilder
		implements InsertWithGeneratedKeyStatementBuilder<PreparedStatement> {
	@Override
	public PreparedStatement buildQueryStatement(
			final Connection connection, JdbcSessionContext context, final String sql, final String[] columnNames)
			throws SQLException {
		return new StatementPreparationTemplate<PreparedStatement>() {
			@Override
			protected PreparedStatement doPrepare() throws SQLException {
				return connection.prepareStatement( sql, columnNames );
			}
		}.prepare( context, sql );
	}
}
