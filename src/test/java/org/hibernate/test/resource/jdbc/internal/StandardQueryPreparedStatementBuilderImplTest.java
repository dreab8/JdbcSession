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
package org.hibernate.test.resource.jdbc.internal;

import java.sql.Connection;
import java.sql.SQLException;

import org.mockito.Mockito;

import org.hibernate.resource.jdbc.internal.StandardQueryPreparedStatementBuilderImpl;

import org.junit.Before;

import static org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec.ResultSetConcurrency;
import static org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec.ResultSetType;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

/**
 * @author Andrea Boriero
 */
public class StandardQueryPreparedStatementBuilderImplTest
		extends AbstractQueryPreparedStatementBuilderTest {
	@Before
	public void setUp() throws SQLException {
		super.onSetUp();
		queryBuilder = StandardQueryPreparedStatementBuilderImpl.INSTANCE;
		setMethodCallCheck(
				new ConnectionMethodCallCheck() {

					@Override
					public void verify(Connection c, String expectedSql) throws SQLException {
						Mockito.verify( c ).prepareStatement( expectedSql );
						reset( c );
					}

					@Override
					public void verify(
							Connection c,
							String expectedSql,
							ResultSetType expectedResultSetType,
							ResultSetConcurrency expectedResultSetConcurrency)
							throws SQLException {
						Mockito.verify( c ).prepareStatement(
								expectedSql,
								expectedResultSetType.getJdbcConstantValue(),
								expectedResultSetConcurrency.getJdbcConstantValue()
						);

					}
				}
		);
	}
}
