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
package org.hibernate.resource.jdbc;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.jdbc.Expectation;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchKey;
import org.hibernate.resource.jdbc.spi.BatchObserver;

/**
 * @author Andrea Borie,ro
 */
public interface BatchableOperationSpec {
	public BatchKey getBatchKey();

	public boolean foregoBatching();

	public List<BatchObserver> getObservers();

	public List<BatchableOperationStep> getSteps();

	public interface BatchableOperationStep {
		public void apply(JdbcSession session, Batch batch, Connection connection, Context context)
				throws SQLException;

		//??? remove this method and add a retlurn type to apply method ???
		public Serializable getGeneratedId();

		// ???Operation Parameter or Values instead of Context ???
		public interface Context {
		}

		public interface InsertContext extends Context {
			public Serializable getId();

			public Object[] getState();

			public Object getEntity();

			public SessionImplementor getSessionImplementor();

			public Expectation getExpectation();
		}

		public interface UpdateContext extends Context {
			public Serializable getId();

			public Object[] getState();

			public Object getEntity();

			public int[] getDirtyFields();

			public boolean isDirtyCollection();

			public Object[] getOldFields();

			public Object getOldVersion();

			public Object getrowId();
		}

		public interface DeleteContext extends Context {
			public Serializable getId();

			public Object[] getState();

			public Object getEntity();
		}
	}


}