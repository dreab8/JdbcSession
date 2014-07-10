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
package org.hibernate.test.resource.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.mockito.InOrder;

import org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec;
import org.hibernate.resource.jdbc.internal.JdbcSessionImpl;

import org.junit.Test;

import static org.hibernate.resource.jdbc.QueryOperationSpec.ResultSetConcurrency;
import static org.hibernate.resource.jdbc.QueryOperationSpec.ResultSetType;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Andrea Boriero
 */
public class BasicPreparedStatementQueryOperationSpecUsageTest
		extends AbstractQueryOperationSpecUsageTest<PreparedStatementQueryOperationSpec> {

	@Test
	public void operationSpecMethodsAreCalledInRightOrder() throws SQLException {
		jdbcSession.accept( operationSpec );

		InOrder inOrder = inOrder( operationSpec );
		inOrder.verify( operationSpec ).getQueryStatementBuilder();
		inOrder.verify( operationSpec ).getParameterBindings();
		inOrder.verify( operationSpec ).getStatementExecutor();
		inOrder.verify( operationSpec ).getResultSetProcessor();

		verify( queryStatementBuilder ).buildQueryStatement(
				any( Connection.class ),
				anyString(),
				any( ResultSetType.class ),
				any( ResultSetConcurrency.class )
		);
		verify( statementExecutor ).execute( statement, (JdbcSessionImpl) jdbcSession );
		verify( resultSetProcessor ).extractResults( resultSet, (JdbcSessionImpl) jdbcSession );
	}

	@Test
	public void resultSetProcessorMethodIsCalledWithTheExpectedParameters() {
		jdbcSession.accept( operationSpec );

		verify( resultSetProcessor ).extractResults( resultSet, (JdbcSessionImpl) jdbcSession );
	}

	@Test
	public void resultSetAndStatementAreClosed() throws SQLException {
		jdbcSession.accept( operationSpec );

		verify( resultSet ).close();
		verify( statement ).close();
	}

	@Override
	protected void mockQueryOperationSpec() {
		operationSpec = mock( PreparedStatementQueryOperationSpec.class );
		when( operationSpec.getResultSetProcessor() ).thenReturn( resultSetProcessor );
	}

	@Override
	protected void jdbSessionAccept() {
		jdbcSession.accept( operationSpec );
	}
}
