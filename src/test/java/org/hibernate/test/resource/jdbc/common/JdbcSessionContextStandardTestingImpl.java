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
package org.hibernate.test.resource.jdbc.common;

import org.hibernate.ConnectionReleaseMode;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.resource.jdbc.spi.JdbcObserver;
import org.hibernate.resource.jdbc.spi.JdbcSessionContext;
import org.hibernate.resource.jdbc.spi.StatementInspector;

/**
 * @author Steve Ebersole
 */
public class JdbcSessionContextStandardTestingImpl implements JdbcSessionContext {

	/**
	 * Singleton access
	 */
	public static final JdbcSessionContextStandardTestingImpl INSTANCE = new JdbcSessionContextStandardTestingImpl();

	private final SqlStatementLogger sqlStatementLogger = new SqlStatementLogger();
	private final SqlExceptionHelper sqlExceptionHelper = new SqlExceptionHelper( SimpleSQLExceptionConverter.INSTANCE );

	@Override
	public boolean isScrollableResultSetsEnabled() {
		return false;
	}

	@Override
	public boolean isGetGeneratedKeysEnabled() {
		return false;
	}

	@Override
	public int getFetchSize() {
		return -1;
	}

	@Override
	public ConnectionReleaseMode getConnectionReleaseMode() {
		return ConnectionReleaseMode.ON_CLOSE;
	}

	@Override
	public ConnectionAcquisitionMode getConnectionAcquisitionMode() {
		return ConnectionAcquisitionMode.DEFAULT;
	}

	@Override
	public StatementInspector getStatementInspector() {
		return StatementInspectorNoOpImpl.INSTANCE;
	}

	@Override
	public SqlExceptionHelper getSqlExceptionHelper() {
		return sqlExceptionHelper;
	}

	@Override
	public SqlStatementLogger getSqlStatementLogger() {
		return sqlStatementLogger;
	}

	@Override
	public JdbcObserver getObserver() {
		return JdbcObserverNoOpImpl.INSTANCE;
	}
}
