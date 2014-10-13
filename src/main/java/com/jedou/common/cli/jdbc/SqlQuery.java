package com.jedou.common.cli.jdbc;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by tiankai on 14-8-15.
 */
public abstract class SqlQuery<T> {
    protected StringBuffer sql  = new StringBuffer();
    public List<Object> params = new ArrayList<Object>();
    public String countSQL = "";

    protected SqlQuery () { }

    public T concat(Object str) {
        this.sql.append(str).append(' ');
        return (T) this;
    }
    public T setCountSQL(String str) {
        this.countSQL = str;
        return (T) this;
    }
    protected T concatCountSQL(Object str) {
        this.countSQL = countSQL.concat(str + " ");
        return (T) this;
    }
    public String getSQL() {
        return sql.toString();
    }
    public CriteriaFieldEnd criteria(String fn) {
        return new CriteriaFieldEnd(new Criteria(fn));
    }

    public CriteriaContainer criteria() {
        return new CriteriaContainer();
    }

    public T and(Criteria...criteria) {
        if (criteria != null && criteria.length > 0) {
            concat('(');
            if (countSQL != null) concatCountSQL("(");
            for (int i = 0; i < criteria.length; i++) {
                Criteria cr = criteria[i];
                concat(cr.toSting());
                if (countSQL != null) concatCountSQL(cr.toSting());
                if (i < criteria.length - 1) {
                    concat("and");
                    if (countSQL != null) concatCountSQL("and");
                }
                for (Object p : cr.params) params.add(p);
            }
            concat(')');
            if (countSQL != null) concatCountSQL(")");
        }
        return (T) this;
    }
    public T or(Criteria...criteria) {
        if (criteria != null && criteria.length > 0) {
            concat('(');
            if (countSQL != null) concatCountSQL("(");
            for (int i = 0; i < criteria.length; i++) {
                Criteria cr = criteria[i];
                concat(cr.toSting());
                if (countSQL != null) concatCountSQL(cr.toSting());
                if (i < criteria.length - 1) {
                    concat("or");
                    if (countSQL != null) concatCountSQL("or");
                }
                for (Object p : cr.params) params.add(p);
            }
            concat(')');
            if (countSQL != null) concatCountSQL(")");
        }
        return (T) this;
    }
    public static class Criteria {
        StringBuffer criteria = new StringBuffer();
        public List<Object> params = new ArrayList<Object>();
        public Criteria() {}
        public Criteria(String fn) {
            concat(fn);
        }
        public Criteria concat(Object fn) {
            criteria.append(' ').append(fn);
            return this;
        }
        public String toSting() {
            return criteria.toString();
        }
    }

    public static class CriteriaContainer extends Criteria {
        public CriteriaContainer(String fn) {
            super(fn);
        }
        public CriteriaContainer() {
            super();
        }
        public CriteriaContainer and(Criteria...criteria) {
            if (criteria != null && criteria.length > 0) {
                concat('(');
                for (int i = 0; i < criteria.length; i++) {
                    Criteria cr = criteria[i];
                    concat(cr.toSting());
                    if (i < criteria.length - 1)
                        concat("and");
                    for (Object p : cr.params) params.add(p);
                }
                concat(')');
            }
            return this;
        }
        public CriteriaContainer or(Criteria...criteria) {
            if (criteria != null && criteria.length > 0) {
                concat('(');
                for (int i = 0; i < criteria.length; i++) {
                    Criteria cr = criteria[i];
                    concat(cr.toSting());
                    if (i < criteria.length - 1)
                        concat("or");
                    for (Object p : cr.params) params.add(p);
                }
                concat(')');
            }
            return this;
        }
    }

    public static class CriteriaFieldEnd {
        Criteria criteria;

        public CriteriaFieldEnd(Criteria criteria) {
            this.criteria = criteria;
        }
        public Criteria greaterThan(Object val) {
            criteria.concat("> ?");
            criteria.params.add(val);
            return criteria;
        }
        public Criteria greaterThanOrEq(Object val) {
            criteria.concat(">= ?");
            criteria.params.add(val);
            return criteria;
        }
        public Criteria lessThan(Object val) {
            criteria.concat("< ?");
            criteria.params.add(val);
            return criteria;
        }
        public Criteria lessThanOrEq(Object val) {
            criteria.concat("<= ?");
            criteria.params.add(val);
            return criteria;
        }
        public Criteria equal(Object val) {
            criteria.concat("= ?");
            criteria.params.add(val);
            return criteria;
        }
        public Criteria notEqual(Object val) {
            criteria.concat("<> ?");
            criteria.params.add(val);
            return criteria;
        }
        public Criteria like(String pattern) {
            criteria.concat("like ?");
            criteria.params.add(pattern);
            return criteria;
        }
        public Criteria notLike(String pattern) {
            criteria.concat("not like ?");
            criteria.params.add(pattern);
            return criteria;
        }
        public Criteria in(Object...vals) {
            criteria.concat("in (");
            for (int i = 0; i < vals.length; i++) {
                criteria.concat('?');
                if (i < vals.length - 1)
                    criteria.concat(',');
                criteria.params.add(vals[i]);
            }
            criteria.concat(')');
            return criteria;
        }

        public Criteria inList(List vals) {
            criteria.concat("in (");
            for (int i = 0; i < vals.size(); i++) {
                criteria.concat('?');
                if (i < vals.size() - 1)
                    criteria.concat(',');
                criteria.params.add(vals.get(i));
            }
            criteria.concat(')');
            return criteria;
        }
        public Criteria notIn(Object...vals) {
            criteria.concat("not in (");
            for (int i = 0; i < vals.length; i++) {
                criteria.concat('?');
                if (i < vals.length - 1)
                    criteria.concat(',');
                criteria.params.add(vals[i]);
            }
            return criteria;
        }
        public Criteria isNull(){
            criteria.concat("is null");
            return criteria;
        }
        public Criteria isNotNull(){
            criteria.concat("is not null");
            return criteria;
        }

        //for jpql
        public Criteria isEmpty(){
            criteria.concat("is empty ");
            return criteria;
        }
        public Criteria isNotEmpty(){
            criteria.concat("is not empty ");
            return criteria;
        }
        public Criteria member(String collectionField){
            criteria.concat("member ").concat(collectionField);
            return criteria;
        }
        public Criteria notMember(String collectionField){
            criteria.concat("not member ").concat(collectionField);
            return criteria;
        }
    }
}
