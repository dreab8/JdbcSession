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

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.jdbc.Expectation;
import org.hibernate.jdbc.Expectations;
import org.hibernate.resource.jdbc.ResourceRegistry;
import org.hibernate.resource.jdbc.spi.Batch;

import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrea Boriero
 */
public abstract class AbstractBatchingTest {

	protected static final String SQL_1 = "INSERT INTO ENTITY_1 (ID) VALUES (?) ";
	protected static final String SQL_2 = "INSERT INTO ENTITY_2 (ID) VALUES (?) ";

	protected ResourceRegistry resourceRegistry;
	protected final Expectation expectation = mock( Expectation.class );

	@Before
	public void setUp() throws SQLException {
		setResourceRegistry();
	}

	protected abstract void setResourceRegistry();

	protected PreparedStatement getStatement(Batch batch, String sql) throws SQLException {
		PreparedStatement statement = batch.getStatement( sql, expectation );
		if ( statement == null ) {
			statement = mock( PreparedStatement.class );
			when( statement.executeBatch() ).thenReturn( new int[] {0} );
		}
		return statement;
	}

}