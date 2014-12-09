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
package org.hibernate.test.resource.jdbc.operationspec;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.hibernate.JDBCException;
import org.hibernate.resource.jdbc.BatchableOperationSpec;
import org.hibernate.resource.jdbc.PreparedStatementQueryOperationSpec;
import org.hibernate.resource.jdbc.internal.StandardQueryPreparedStatementBuilderImpl;
import org.hibernate.resource.jdbc.spi.Batch;
import org.hibernate.resource.jdbc.spi.BatchKey;
import org.hibernate.resource.jdbc.spi.BatchObserver;
import org.hibernate.resource.jdbc.spi.ParameterBindings;
import org.hibernate.resource.jdbc.spi.QueryStatementBuilder;
import org.hibernate.resource.jdbc.spi.ResultSetProcessor;
import org.hibernate.resource.jdbc.spi.StatementExecutor;

import org.junit.Test;

import org.hibernate.test.resource.jdbc.common.BatchKeyImpl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Andrea Boriero
 */
public class BatchableInsertEntityWithGeneratedValueIntegrationTest
		extends AbstractBatchableOperationSpecIntegrationTest {
	private static final int BATCH_SIZE = 10;
	private static final Long DEFAULT_SECURITY_CODE = 123L;

	private static final String CREDIT_CARD_INSERT_SQL = "INSERT INTO CREDITCARD (ID, NUMBER) values  (?,?)";

	@Test
	public void testTheEntityPropertyWithGeneratedValueIsUpdatedAfterTheInsert() throws SQLException {
		final CreditCard creditCardToSave = new CreditCard();
		creditCardToSave.setNumber( "0123" );

		final Serializable entityId = 1L;

		final BatchableOperationSpec.BatchableOperationStep insertIntoCreditCardTableStep = new BatchableOperationSpec.BatchableOperationStep() {
			@Override
			public void apply(
					Batch batch,
					Connection connection,
					Context context)
					throws SQLException {
				final PreparedStatement statement = getStatement( batch, connection, CREDIT_CARD_INSERT_SQL );
				statement.setLong( 1, (Long) entityId );
				statement.setString( 2, creditCardToSave.getNumber() );
				batch.addBatch( CREDIT_CARD_INSERT_SQL, statement );
			}

			@Override
			public Serializable getGeneratedId() {
				return null;
			}

		};

		final BatchableOperationSpec.BatchableOperationStep updateCreditCardEntitySecurityCodeGeneratedValueStep = new BatchableOperationSpec.BatchableOperationStep() {
			@Override
			public void apply(
					Batch batch,
					Connection connection,
					Context context)
					throws SQLException {

				PreparedStatementQueryOperationSpec updateGenerateValueOperationSpec = new UpdateGenerateValueOperationSpec(
						creditCardToSave,
						(Long) entityId
				);

				getJdbcSession().accept( updateGenerateValueOperationSpec );
			}

			@Override
			public Serializable getGeneratedId() {
				return null;
			}
		};

		getJdbcSession().accept(
				new BatchableOperationSpec() {
					@Override
					public BatchKey getBatchKey() {
						return new BatchKeyImpl( "INSERT#CREDITCARD" );
					}

					@Override
					public boolean foregoBatching() {
						/*
						 Cannot be batched because it is necessary to update the saved Entity properties with the
						 db generated values
						  */
						return false;
					}

					@Override
					public List<BatchObserver> getObservers() {
						return null;
					}

					@Override
					public List<BatchableOperationStep> getSteps() {
						return Arrays.asList(
								insertIntoCreditCardTableStep,
								updateCreditCardEntitySecurityCodeGeneratedValueStep
						);
					}
				}, buildContext()
		);

		try {
			getJdbcSession().executeBatch();
			commit();

			assertThat( creditCardToSave.getGeneratedSecurityCode(), is( DEFAULT_SECURITY_CODE ) );
		}
		catch (JDBCException e) {
			rollback();
			throw e;
		}
	}

	public class CreditCard {
		protected Long id;

		protected String number;

		protected Long generatedSecurityCode;

		public Long getId() {
			return id;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public String getNumber() {
			return number;
		}

		public void setNumber(String number) {
			this.number = number;
		}

		public Long getGeneratedSecurityCode() {
			return generatedSecurityCode;
		}

		public void setGeneratedSecurityCode(Long generatedSecurityCode) {
			this.generatedSecurityCode = generatedSecurityCode;
		}
	}

	public class UpdateGenerateValueOperationSpec implements PreparedStatementQueryOperationSpec {
		final String selectGeneratedValueFromCreditcardSlq = "SELECT creditcard_.SECURITY_CODE as securityCode_ " +
				"FROM CreditCard creditcard_ " +
				"WHERE creditcard_.ID=?";

		private CreditCard creditCardToSave;
		private Long entityId;

		public UpdateGenerateValueOperationSpec(CreditCard creditCardToSave, Long entityId) {
			this.creditCardToSave = creditCardToSave;
			this.entityId = entityId;
		}

		@Override
		public ResultSetProcessor getResultSetProcessor() {
			return new ResultSetProcessor() {
				@Override
				public Object extractResults(
						ResultSet resultSet) throws SQLException {
					resultSet.next();
					long securityCode = resultSet.getLong( "SECURITY_CODE" );
					creditCardToSave.setGeneratedSecurityCode( securityCode );
					return creditCardToSave;
				}
			};
		}

		@Override
		public QueryStatementBuilder<? extends PreparedStatement> getQueryStatementBuilder() {
			return new StandardQueryPreparedStatementBuilderImpl();
		}

		@Override
		public StatementExecutor getStatementExecutor() {
			return new StatementExecutor() {
				@Override
				public ResultSet execute(
						PreparedStatement statement) throws SQLException {
					return statement.executeQuery();
				}
			};
		}

		@Override
		public ParameterBindings getParameterBindings() {
			return new ParameterBindings() {
				@Override
				public void bindParameters(PreparedStatement statement) throws SQLException {
					statement.setLong( 1, (Long) entityId );
				}
			};
		}

		@Override
		public int getQueryTimeout() {
			return 0;
		}

		@Override
		public String getSql() {
			return selectGeneratedValueFromCreditcardSlq;
		}

		@Override
		public ResultSetType getResultSetType() {
			return ResultSetType.FORWARD_ONLY;
		}

		@Override
		public ResultSetConcurrency getResultSetConcurrency() {
			return ResultSetConcurrency.READ_ONLY;
		}

		@Override
		public int getOffset() {
			return 0;
		}

		@Override
		public int getLimit() {
			return 0;
		}

		@Override
		public boolean isCancellable() {
			return false;
		}
	}

	@Override
	protected void createTables() throws SQLException {
		String createCreditCardTableSql = "create table CREDITCARD (" +
				"        ID bigint not null," +
				"        NUMBER varchar(255)," +
				"		 SECURITY_CODE integer default " + DEFAULT_SECURITY_CODE + " ," +
				"        primary key (ID) )";
		execute( createCreditCardTableSql );
	}

	@Override
	protected void dropTables() throws SQLException {
		execute( "DROP table CREDITCARD IF EXISTS " );
	}

	@Override
	protected int getBatchSize() {
		return BATCH_SIZE;
	}
}
