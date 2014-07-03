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
package org.hibernate.resource.jdbc.internal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;

import static org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec.ResultSetConcurrency;
import static org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec.ResultSetType;

/**
 * @author Andrea Boriero
 */
public class StandardQueryPreparedStatementBuilderImpl implements QueryStatementBuilder<PreparedStatement> {

	public static final StandardQueryPreparedStatementBuilderImpl INSTANCE = new StandardQueryPreparedStatementBuilderImpl();

	private StandardQueryPreparedStatementBuilderImpl() {
	}

	@Override
	public PreparedStatement buildQueryStatement(
			Connection connection,
			String sql,
			JdbcSessionContext context,
			ResultSetType resultSetType,
			ResultSetConcurrency resultSetConcurrency) {
		try {
			if ( resultSetType != null && resultSetConcurrency != null ) {
				return connection.prepareStatement(
						sql,
						resultSetType.getJdbcConstantValue(),
						resultSetConcurrency.getJdbcConstantValue()
				);
			}
			else {
				return connection.prepareStatement( sql );
			}
		}
		catch (SQLException e) {
			throw context.getSqlExceptionHelper().convert( e, "Unexpected error creating Statement" );
		}
	}
}
