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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;

import org.hibernate.resource.jdbc.JdbcSession;
import org.hibernate.resource.jdbc.QueryOperationSpec;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;

/**
 * @author Andrea Boriero
 */
public class CallableQueryPreparedStatementBuilderImpl implements QueryStatementBuilder<CallableStatement> {

	@Override
	public CallableStatement buildQueryStatement(
			final Connection connection,
			final JdbcSessionContext context,
			final String sql,
			final QueryOperationSpec.ResultSetType resultSetType,
			final QueryOperationSpec.ResultSetConcurrency resultSetConcurrency) {
		return new StatementPreparationTemplate<CallableStatement>() {
			@Override
			protected CallableStatement doPrepare() throws SQLException {
				if ( resultSetType != null && resultSetConcurrency != null ) {
					return connection.prepareCall(
							sql,
							resultSetType.getJdbcConstantValue(),
							resultSetConcurrency.getJdbcConstantValue()
					);
				}
				else {
					return connection.prepareCall( sql );
				}
			}
		}.prepare( context, sql );
	}

}
