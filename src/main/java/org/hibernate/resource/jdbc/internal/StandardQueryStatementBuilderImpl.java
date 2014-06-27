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
import java.sql.Statement;

import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;

/**
 * @author Andrea Boriero
 */
public class StandardQueryStatementBuilderImpl implements QueryStatementBuilder {

	public static final StandardQueryStatementBuilderImpl INSTANCE = new StandardQueryStatementBuilderImpl();

	private StandardQueryStatementBuilderImpl() {
	}

	@Override
	public Statement buildQueryStatement(
			Connection connection,
			String sql,
			JdbcSessionContext context) {
		try {
			PreparedStatement statement = connection.prepareStatement( sql );
			return statement;
		}
		catch (SQLException e) {
			throw context.getSqlExceptionHelper().convert( e, "Unexpected error creating Statement" );
		}
	}
}
