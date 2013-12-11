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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import org.hibernate.resource2.jdbc.spi.JdbcConnectionAccess;

/**
 * @author Steve Ebersole
 */
public class JdbcConnectionAccessTestingImpl implements JdbcConnectionAccess {
	/**
	 * Singleton access
	 */
	public static final JdbcConnectionAccessTestingImpl INSTANCE = new JdbcConnectionAccessTestingImpl();

	private final Driver driver;
	private final String url;
	private final Properties connectionProperties;

	public JdbcConnectionAccessTestingImpl() {
		try {
			driver = (Driver) Class.forName( "org.h2.Driver" ).newInstance();

			url = "jdbc:h2:mem:db1";

			connectionProperties = new Properties();
			connectionProperties.setProperty( "username", "sa" );
			connectionProperties.setProperty( "password", "" );
		}
		catch (Exception e) {
			throw new RuntimeException( "Unable to configure JdbcConnectionAccessTestingImpl", e );
		}
	}

	@Override
	public Connection obtainConnection() throws SQLException {
		return driver.connect( url, connectionProperties );
	}

	@Override
	public void releaseConnection(Connection connection) throws SQLException {
		connection.close();
	}

	@Override
	public boolean supportsAggressiveRelease() {
		return false;
	}
}
