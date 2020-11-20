package org.springframework.data.aerospike.query;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.Statement;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.model.IndexedField;

import java.util.ArrayList;
import java.util.List;

/**
 * @author peter
 * @author Anastasiia Smirnova
 */
public class StatementBuilder {

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
					if(q != null && isIndexedBin(stmt, q)) {
						Filter filter = q.asFilter();
						if (filter != null) {
							stmt.setFilter(filter);
							q.asFilter(true);
							break;
						}
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

		PredExp[] predexps = buildPredExp(qualifiers).toArray(new PredExp[0]);
		if (predexps.length > 0) {
			stmt.setPredExp(predexps);
		} else {
			throw new QualifierException("Failed to build Query");
		}
	}

	private boolean isIndexedBin(Statement stmt, Qualifier qualifier) {
		if (qualifier.getField() == null) return false;

		//TODO: skips check on index-type and index-collection-type
		return indexesCache.hasIndexFor(new IndexedField(stmt.getNamespace(), stmt.getSetName(), qualifier.getField()));
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
