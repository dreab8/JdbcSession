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
package org.hibernate.test.resource.common;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import org.hibernate.HibernateException;
import org.hibernate.cfg.AvailableSettings;

/**
 * @author Steve Ebersole
 */
public class DatabaseConnectionInfo {
	/**
	 * Singleton access
	 */
	public static final DatabaseConnectionInfo INSTANCE = new DatabaseConnectionInfo();

	public static final String DRIVER = "org.h2.Driver";
	public static final String URL = "jdbc:h2:mem:db1";
	public static final String USER = "sa";

	public Properties createDriverManagerProperties() {
		Properties props = new Properties();
		props.setProperty( AvailableSettings.USER, USER );
		return props;
	}

	private Driver driver;
	private Connection connection;

	public Connection makeConnection() throws SQLException {
		if(connection == null || connection.isClosed()) {
			if ( driver == null ) {
				try {
					driver = (Driver) Class.forName( DRIVER ).newInstance();
				}
				catch (Exception e) {
					throw new HibernateException( "Unable to load JDBC Driver [" + DRIVER + "]", e );
				}
			}
			connection = driver.connect( URL, createDriverManagerProperties() );
		}
		return connection;
	}
}
