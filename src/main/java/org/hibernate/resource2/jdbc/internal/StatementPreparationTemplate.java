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
package org.hibernate.resource2.jdbc.internal;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Acts as a template (template pattern) for preparing JDBC PreparedStatement instances.
 * {@link #prepareStatement()} is the main template method.  {@link #doPrepare()} and {@link #postProcess} are
 * template hooks.
 *
 * @author Steve Ebersole
 */
public abstract class StatementPreparationTemplate {
	private final JdbcSessionImpl jdbcSession;
	private final String sql;

	public StatementPreparationTemplate(JdbcSessionImpl jdbcSession, String sql) {
		this.jdbcSession = jdbcSession;
		final String replacement = jdbcSession.getStatementInspector().inspect( sql );
		this.sql = replacement == null ? sql : replacement;
	}

	public JdbcSessionImpl getJdbcSession() {
		return jdbcSession;
	}

	public String getSql() {
		return sql;
	}

	/**
	 * The main template method called by StatementPreparerImpl
	 *
	 * @return
	 */
	public PreparedStatement prepareStatement() {
		try {
			jdbcSession.getContext().getSqlStatementLogger().logStatement( sql );

			final PreparedStatement preparedStatement;
			try {
				// todo : if stats are enabled make sure Session calls StatisticsImplementor.prepareStatement()
				// 		via context.getObserver().jdbcPrepareStatementEnd
				jdbcSession.getContext().getObserver().jdbcPrepareStatementStart();
				preparedStatement = doPrepare();
				setStatementTimeout( preparedStatement );
			}
			finally {
				jdbcSession.getContext().getObserver().jdbcPrepareStatementEnd();
			}
			postProcess( preparedStatement );
			jdbcSession.register( preparedStatement );
			return preparedStatement;
		}
		catch ( SQLException e ) {
			throw jdbcSession.getContext().getSqlExceptionHelper().convert( e, "could not prepare statement", sql );
		}
	}

	protected abstract PreparedStatement doPrepare() throws SQLException;

	public void postProcess(PreparedStatement preparedStatement) throws SQLException {
	}

	private void setStatementTimeout(PreparedStatement preparedStatement) throws SQLException {
		final int remainingTransactionTimeOutPeriod = jdbcSession.determineRemainingTransactionTimeOutPeriod();
		if ( remainingTransactionTimeOutPeriod > 0 ) {
			preparedStatement.setQueryTimeout( remainingTransactionTimeOutPeriod );
		}
	}
}
