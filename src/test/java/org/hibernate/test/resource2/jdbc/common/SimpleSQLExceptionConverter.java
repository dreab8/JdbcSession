/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * Copyright (c) 2013, Red Hat Inc. or third-party contributors as
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
package org.hibernate.test.resource2.jdbc.common;

import java.sql.SQLException;

import org.hibernate.JDBCException;
import org.hibernate.exception.GenericJDBCException;
import org.hibernate.exception.spi.SQLExceptionConverter;

/**
 * @author Steve Ebersole
 */
public class SimpleSQLExceptionConverter implements SQLExceptionConverter {
	/**
	 * Singleton access
	 */
	public static final SimpleSQLExceptionConverter INSTANCE = new SimpleSQLExceptionConverter();

	@Override
	public JDBCException convert(SQLException sqlException, String message, String sql) {
		return new GenericJDBCException( message, sqlException, sql );
	}
}
