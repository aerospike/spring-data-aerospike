package org.springframework.data.aerospike.query;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.Statement;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.model.IndexedField;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.BETWEEN;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.EQ;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.GEO_WITHIN;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.GT;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.GTEQ;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.LT;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.LTEQ;

/**
 * @author peter
 * @author Anastasiia Smirnova
 */
public class StatementBuilder {

	private static final EnumSet<Qualifier.FilterOperation> INDEXED_OPERATIONS = EnumSet.of(
			EQ, BETWEEN, GT, GTEQ, LT, LTEQ, GEO_WITHIN);

	private final IndexesCache indexesCache;

	public StatementBuilder(IndexesCache indexesCache) {
		this.indexesCache = indexesCache;
	}

	public Statement build(String namespace, String set, Filter filter, Qualifier[] qualifiers) {
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(set);
		if (filter != null) {
			stmt.setFilter(filter);
		}
		if (qualifiers != null && qualifiers.length != 0) {
			updateStatement(stmt, qualifiers);
		}
		return stmt;
	}

	private void updateStatement(Statement stmt, Qualifier[] qualifiers) {
		/*
		 *  query with filters
		 */
		for (int i = 0; i < qualifiers.length; i++) {
			Qualifier qualifier = qualifiers[i];

			if (qualifier == null) continue;
			if (qualifier.getOperation() == Qualifier.FilterOperation.AND) {
				for (Qualifier q : qualifier.getQualifiers()) {
					Filter filter = q == null ? null : q.asFilter();
					if (filter != null) {
						stmt.setFilter(filter);
						q.asFilter(true);
						break;
					}
				}
			} else if (isIndexedBin(stmt, qualifier)) {
				Filter filter = qualifier.asFilter();
				if (filter != null) {
					stmt.setFilter(filter);
					qualifier.asFilter(true);
					qualifiers[i] = null;
					/* If this was the only qualifier, we do not need to do anymore work, just return
					 * the query iterator.
					 */
					if (qualifiers.length == 1) {
						return;
					}
					break;
				}
			}
		}

		try {
			PredExp[] predexps;
			predexps = buildPredExp(qualifiers).toArray(new PredExp[0]);
			if (predexps.length > 0) {
				stmt.setPredExp(predexps);
				return;
			} else {
				throw new QualifierException("Failed to build Query");
			}
		} catch (PredExpException e) {
			throw new QualifierException(e.getMessage());
		}
	}

	private boolean isIndexedBin(Statement stmt, Qualifier qualifier) {
		if (null == qualifier.getField()) return false;

		return INDEXED_OPERATIONS.contains(qualifier.getOperation())
				&& indexesCache.hasIndexFor(new IndexedField(stmt.getNamespace(), stmt.getSetName(), qualifier.getField()));
	}

	private static List<PredExp> buildPredExp(Qualifier[] qualifiers) {
		List<PredExp> pes = new ArrayList<PredExp>();
		int qCount = 0;
		for (Qualifier q : qualifiers) {
			if (null != q && !q.queryAsFilter()) {
				List<PredExp> tpes = q.toPredExp();
				if (tpes.size() > 0) {
					pes.addAll(tpes);
					qCount++;
					q = null;
				}
			}
		}

		if (qCount > 1) pes.add(PredExp.and(qCount));
		return pes;
	}
}
