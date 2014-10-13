package com.jedou.common.cli.mongodb;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.jedou.common.cli.util.NakedBeanUtil;
import com.mongodb.*;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

public class MongoQuery {
    public static final String ID_FILED  = "_id";
    private String db ;
    private String col;
    public DBObject doc = new BasicDBObject();
    private int limit = 0;
    private int skip  = 0;
    private DBObject sort;
    public DBObject op = new BasicDBObject();
    private boolean upsert = false;
    private boolean multi = true;
    private Mongo mongo;
    private GridFS myFS;


    public GridFS getMyFS() {
        return myFS;
    }


    public void setMyFS(GridFS myFS) {
        this.myFS = myFS;
    }


    public Mongo getMongo() {
        return mongo;
    }


    public void setMongo(Mongo mongo) {
        this.mongo = mongo;
    }


    public MongoQuery(){}

    public MongoQuery(String db, String col, Mongo mongo){
        this.db = db;
        this.col = col;
        this.mongo = mongo;
        this.myFS = new GridFS(mongo.getDB(db));
    }

    public static MongoQuery createQuery(String colName) {
        MongoConfig mc = MongoConfig.getDefaultMagic();
        return createQuery(mc.dbName, colName, mc.getMongo());
    }

    /**
     * 用于手工创建MongoDbColConfig（Mongodb数据集配置对象）情景
     * @param conf
     * @return
     */
    private static MongoQuery createQuery(MongoConfig conf) {
        return createQuery(conf.dbName, conf.collectionName, conf.getMongo());
    }
    /**
     * 用于手工获取Mongo连接配置对象情景
     * @param dbName
     * @param colName
     * @param mongo
     * @return
     */
    private static MongoQuery createQuery(String dbName, String colName, Mongo mongo) {
        return new MongoQuery(dbName, colName, mongo);
    }

    public DBCollection getCollection() {
        return mongo.getDB(db).getCollection(col);
    }

    /**
     * <i>only select use</i>
     * <p></p>
     * @author zhoudd add 2012-11-20
     */
    public MongoQuery concat(String field,Object value, String opCmd){
        Object v = value;
        if(opCmd!=null){
            v = new BasicDBObject(opCmd,value);
        }
        doc.put(field, v);
        return this;
    }
    public MongoQuery clean(){
        doc = new BasicDBObject();
        return this;
    }
    /**
     * <i>select and insert use</i>
     * <p></p>
     * author: zhoudd add 2012-11-20
     * @param field
     * @param value
     * return this
     */
    public MongoQuery concat(String field,Object value){
        return concat(field, value, null);
    }
    public MongoQuery concat(Map<String,Object> fields){
        doc.putAll(fields);
        return this;
    }
    public MongoQuery concat(Object bean,boolean includingNull){
        if(bean!=null){
            Map<String,Object> fields = NakedBeanUtil.toMap(bean, includingNull);
            doc.putAll(fields);
        }
        return this;
    }
    public MongoQuery concat(Object bean){
        return concat(bean, false);
    }
    public MongoQuery concat(DBObject bean){
        doc.putAll(bean);
        return this;
    }
    public MongoQuery or(String field,Object value,String opCmd){
        Object v = value;
        if(opCmd!=null){
            v = new BasicDBObject(opCmd,value);
        }
        List<DBObject> ors = null;
        if(!doc.containsField("$or")){
            ors = new ArrayList<DBObject>();
        }else{
            ors = (List<DBObject>) doc.get("$or");
        }
        ors.add(new BasicDBObject(field, v));
        doc.put("$or",ors);
        return this;
    }
    public MongoQuery or(String field,Object value){
        return or(field, value,null);
    }
    public MongoQuery mod(String field,int mod,int y){
        return concat(field, new int[]{mod,y}, "$mod");
    }

    /**
     * <p>插入数据</p>
     * author: zhoudd add 2012-11-20
     * see: http://www.mongodb.org/display/DOCS/Inserting
     */
    public void insert(){
        getCollection().insert(doc);
    }
    /**
     * <p>插入数据</p>
     * author: zhoudd add 2012-11-20
     * see: http://www.mongodb.org/display/DOCS/Inserting
     */
    public void save(){
        getCollection().save(doc);
    }
    public void insert(DBObject obj){
        getCollection().insert(obj);
    }
    //select
    /**
     * <i>only select has</i>
     * <p>返回的最多数据行数</p>
     * @author zhoudd add 2012-11-20
     * @param limit
     * @return
     */
    public MongoQuery limit(int limit){
        this.limit = limit;
        return this;
    }
    /**
     * <i>only select has</i>
     * <p>跳过数据结果的前skip行</p>
     * @author zhoudd add 2012-11-20
     * @param skip
     * @return
     */
    public MongoQuery skip(int skip){
        this.skip = skip;
        return this;
    }
    /**
     * <i>only select has</i>
     * <p></p>
     * @author zhoudd add 2012-11-20
     * @param key
     * @param s
     * @return
     */
    public MongoQuery sort(String key,Sort s){
        if(sort==null){
            sort = new BasicDBObject();
        }
        sort.put(key, s.v);
        return this;
    }
    /**
     * <i>only select has</i>
     * <p></p>
     * @author zhoudd add 2012-11-20
     * @param key
     * @return
     */
    public MongoQuery sort(String key){
        return sort(key, Sort.asc);
    }

    public DBObject first() {
        return first(DBObject.class);
    }
    public <T> T first(Class<T> cls) {
        List<T> li = this.asList(cls);
        if (li.isEmpty()) return null;
        else
            return li.get(0);
    }
    /**
     * <i>only select has</i>
     * <p></p>
     * @author zhoudd add 2012-11-20
     * @return
     */
    public List<DBObject> asList(){
        return (List<DBObject>) asList(DBObject.class);
    }
    /**
     * <i>only select has</i>
     * <p>查询返回</p>
     * author: zhoudd add 2012-11-20
     * @param cls 转换的对象类型，默认为DBObject。
     * @return 转换类型后的对象列表
     * see: http://www.mongodb.org/display/DOCS/Advanced+Queries
     */
    public <T> List<T> asList(Class<T> cls){
        List<T> list = new ArrayList<T>();
        DBCursor cur = null;
        try {
            cur = null;
            if(skip>0&&limit>0&&sort!=null){
                cur = getCollection().find(doc).sort(sort).skip(skip).limit(limit);
            }else if(skip>0&&limit>0){
                cur = getCollection().find(doc).skip(skip).limit(limit);
            }else if(skip>0&&sort!=null){
                cur = getCollection().find(doc).sort(sort).skip(skip);
            }else if(limit>0&&sort!=null){
                cur = getCollection().find(doc).sort(sort).limit(limit);
            }else if(skip>0){
                cur = getCollection().find(doc).skip(skip);
            }else if(limit>0){
                cur = getCollection().find(doc).limit(limit);
            }else if(sort!=null){
                cur = getCollection().find(doc).sort(sort);
            }else{
                cur = getCollection().find(doc);
            }
            boolean f = cls!=null&&!cls.equals(DBObject.class);
            while(cur.hasNext()){
                DBObject d = cur.next();
                try {
                    if(f){
                        list.add((T) NakedBeanUtil.toBean(d.toMap(), cls));
                    }else{
                        list.add((T) d);
                    }
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        } finally{
            if(cur!=null)cur.close();
        }
        return list;
    }
    /**
     * <i>only select has</i>
     * <p>查询返回</p>
     * author: zhoudd add 2012-11-20
     * @param extractor 行转换器 @see RowExtractor
     * @return 使用行转换器转换后的对象列表
     * see: http://www.mongodb.org/display/DOCS/Advanced+Queries
     */
    public  List asList(RowExtractor extractor){
        List list = new ArrayList();
        DBCursor cur = null;
        try {
            cur = null;
            if(skip>0&&limit>0&&sort!=null){
                cur = getCollection().find(doc).sort(sort).skip(skip).limit(limit);
            }else if(skip>0&&limit>0){
                cur = getCollection().find(doc).skip(skip).limit(limit);
            }else if(skip>0&&sort!=null){
                cur = getCollection().find(doc).sort(sort).skip(skip);
            }else if(limit>0&&sort!=null){
                cur = getCollection().find(doc).sort(sort).limit(limit);
            }else if(skip>0){
                cur = getCollection().find(doc).skip(skip);
            }else if(limit>0){
                cur = getCollection().find(doc).limit(limit);
            }else if(sort!=null){
                cur = getCollection().find(doc).sort(sort);
            }else{
                cur = getCollection().find(doc);
            }
            while(cur.hasNext()){
                Object obj = extractor.extractData(cur.next());
                if(obj!=null){
                    list.add(obj);
                }
            }
        } finally{
            if(cur!=null)cur.close();
        }
        return list;
    }
    public long count(){
        long cnt = getCollection().count(doc);
        return cnt;
    }
    /**
     * <i>only update has</i>
     * <p>如果没有满足条件的更新数据是否添加该条数据</p>
     * if this should be an "upsert" operation; that is, if the record(s) do not exist, insert one.
     * Upsert only inserts a single document.
     * @author zhoudd add 2012-11-20
     * @param upsert 默认：false
     * @return
     */
    public MongoQuery upsert(boolean upsert){
        this.upsert = upsert;
        return this;
    }
    /**
     * <i>only update has</i>
     * <p>如果满足更新条件的数据有多条是否全更新</p>
     *  indicates if all documents matching criteria should be updated rather than just one.
     *  Can be useful with the $ operators below.
     * @author zhoudd add 2012-11-20
     * @param multi 默认：true
     * @return
     */
    public MongoQuery multi(boolean multi){
        this.multi = multi;
        return this;
    }
    /**
     * <i>only update has</i>
     * <p>给指定字段修改值</p>
     * @author zhoudd@hollycrm.com add 2012-11-20
     * @param field 设值字段
     * @param value 修改的新值
     * @return
     */
    public MongoQuery set(String field,Object value){
        if(!op.containsField("$set")){
            op.put("$set",new BasicDBObject(field, value));
        }else{
            DBObject v = (DBObject) op.get("$set");
            v.put(field, value);
            op.put("$set", v);
        }
        return this;
    }
    public MongoQuery set(Map<String, Object> map){
        if(map!=null&&map.size()>0){
            Iterator<String> it = map.keySet().iterator();
            while(it.hasNext()){
                String k = it.next();
                Object v = map.get(k);
                set(k,v);
            }
        }
        return this;
    }
    /**
     * <i>only update has</i>
     * <p>在field字段上再增量添加value</p>
     * increments field by the number value if field is present in the object, otherwise sets field to the number value.
     *  This can also be used to decrement by using a negative value.
     * @author zhoudd@hollycrm.com add 2012-11-20
     * @param field 增量字段
     * @param value 增加的值 ，可为负
     * @return
     */
    public MongoQuery inc(String field,int value){
        if(!op.containsField("$inc")){
            op.put("$inc",new BasicDBObject(field, value));
        }else{
            DBObject v = (DBObject) op.get("$inc");
            v.put(field, value);
            op.put("$inc", v);
        }
        return this;
    }

    /**
     * 有则不添加元素，没有则添加元素的数组修改操作
     * @param field 字段名称
     * @param value 新增的数组元素值
     * @return MongoQuery
     */
    public MongoQuery addToSet(String field, Object value) {
        if (!op.containsField("$addToSet")) {
            op.put("$addToSet", new BasicDBObject(field, value));
        }
        else {
            DBObject v = (DBObject) op.get("$addToSet");
            v.put(field, value);
            op.put("$addToSet", v);
        }
        return this;
    }

    /**
     * 删除数组中的元素，即删除field字段指向的数组中值为value的元素。当值为value的元素不存在的时候$pull修改器不会报错
     * @param field 字段名称
     * @param value 删除的数组元素值
     * @return MongoQuery
     */
    public MongoQuery pull(String field, Object value) {
        if (!op.containsField("$pull")) {
            op.put("$pull", new BasicDBObject(field, value));
        }
        else {
            DBObject v = (DBObject) op.get("$pull");
            v.put(field, value);
            op.put("$pull", v);
        }
        return this;
    }
    /**
     * <p>最后的更新操作</p>
     * see: http://www.mongodb.org/display/DOCS/Updating
     * author: zhoudd@hollycrm.com add 2012-11-20
     */
    public void update(){
        if(getCachedLastError){
            getCollection().update(doc, op,upsert,multi).getCachedLastError();
        }else{
            getCollection().update(doc, op,upsert,multi);
        }
    }
    public DBObject findAndModify() {
        DBObject dbo = getCollection().findAndModify(doc, null, null, false,op,true,false);
        return dbo;
    }
    public <T> T findAndModify(Class<T> clazz) {
        DBObject dbo = findAndModify();
        if (dbo != null) {
            T obj = (T) NakedBeanUtil.toBean(dbo.toMap(), clazz);
            return obj;
        }
        else return null;
    }
    private boolean getCachedLastError = false;
    public void getCachedLastError(boolean b){
        this.getCachedLastError = b;
    }
    /**
     * <p>该collection是否存在</p>
     * @author zhoudd@hollycrm.com add 2012-11-22
     * @return
     */
    public boolean exists(){
        return mongo.getDB(db).collectionExists(col);
    }
    public boolean isCapped(){
        return getCollection().isCapped();
    }
    /**
     * <p>创建Collection</p>
     * @author zhoudd@hollycrm.com add 2012-11-22
     * @param drop   如果已有该Collection是否先删除再重新创建.默认：false
     * @param capped 创建的该Collection是否为一个Capped collections。
     * @param size   Capped collections的大小,该参数只有capped为true是有效。单位：byte.默认：20971520(20M)
     * @return
     */
    public boolean createCol(boolean drop,boolean capped,long size){
        if(drop){
            drop();
        }
        long rSize = 0;//Config.CappedCollectionSize;
        if(size>0){
            rSize = size;
        }
        boolean ex = exists();
        if(ex&&capped&&!isCapped()){
            //3. 普通collection转换成Capped
            //db.runCommand({"convertToCapped": "mycoll", size: 100000});
            System.out.println("convertToCapped.[db="+db+",col="+col+",size="+rSize+"]");
            BasicDBObject cmd = new BasicDBObject("convertToCapped",col).append("size", rSize);
            mongo.getDB(db).command(cmd);
            return true;
        }else{
            if(!ex){
                BasicDBObject options = new BasicDBObject();
                if(capped){
                    options = new BasicDBObject("capped", true).append("size", rSize);
                }
                mongo.getDB(db).createCollection(col, options);
                System.out.println("create collection.[db="+db+",col="+col+",capped="+capped+",size="+rSize+"]");
                return true;
            }
        }
        System.out.println("collection exists.[db="+db+",col="+col+",capped="+capped+",size="+rSize+"]");
        return true;
    }
    /**
     * <p>创建Collection</p>
     * @author zhoudd@hollycrm.com add 2012-11-22
     * @param drop   如果已有该Collection是否先删除再重新创建.默认：false
     * @param capped 创建的该Collection是否为一个Capped collections。
     * @return
     */
    public boolean createCol(boolean drop,boolean capped){
        return createCol(drop,capped,-1);//-1：使用默认值
    }
    /**
     * <p>创建Collection</p>
     * @author zhoudd@hollycrm.com add 2012-11-22
     * @param drop   如果已有该Collection是否先删除再重新创建.默认：false
     * @return
     */
    public boolean createCol(boolean drop){
        return createCol(drop, false);
    }
    /**
     * <p>创建Collection</p>
     * @author zhoudd@hollycrm.com add 2012-11-22
     * @return
     */
    public boolean createCol(){
        return createCol(false);
    }
    public void createIndex() {
        getCollection().ensureIndex(doc);
    }
    /**
     * <p>删除Collection</p>
     * @author zhoudd@hollycrm.com add 2012-11-22
     */
    public void drop(){
        System.out.println("drop collection.[db="+db+",col="+col+"]");
        getCollection().drop();
    }
    public WriteResult delete(){
        WriteResult wr = getCollection().remove(doc);
        return wr;
    }
    public DBObject get(String id){
        return get(ID_FILED, id);
    }
    public DBObject get(String filed,String value){
        return getCollection().findOne(new BasicDBObject(filed, value));
    }

    /**
     * 保存文件到Mongo中
     * @param file  文件对象
     * @param id    id_ 自定义序列
     * @param metaData  元数据类型 Key Value
     * @return
     */
    public boolean concatGridFile(File file, Object id, DBObject metaData){
        GridFSInputFile gridFSInputFile;
        DBObject query  = new BasicDBObject("_id", id);
        GridFSDBFile gridFSDBFile = myFS.findOne(query);
        if(gridFSDBFile!= null)
            return false;
        try {
            gridFSInputFile = myFS.createFile(file);
            gridFSInputFile.put("_id",id);
            gridFSInputFile.setFilename(file.getName());
            gridFSInputFile.setMetaData(metaData);
            gridFSInputFile.setContentType(file.getName().substring(file.getName().lastIndexOf(".")));
            gridFSInputFile.save();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 据id返回文件
     * @param id
     * @return
     */
    public GridFSDBFile getGridFileById(Object id){
        DBObject query  = new BasicDBObject("_id", id);
        GridFSDBFile gridFSDBFile = myFS.findOne(query);
        return gridFSDBFile;
    }

    /**
     * 根据id删除文件
     * @param id
     * @throws Exception
     */
    public void deleteGridFile(Object id) throws Exception {
        DBObject query = new BasicDBObject();
        query.put("_id", id);
        myFS.remove(query);
    }

    /* *** 以下为查询的规范表示法实现 *** */
    public static CriteriaFieldEnd criteria(String fn) {
        return new CriteriaFieldEnd(new Criteria(fn));
    }

    public CriteriaContainer criteria() {
        return new CriteriaContainer();
    }

    public MongoQuery and(Criteria...criteria) {
        if (criteria != null && criteria.length > 0) {
            DBObject[] dbos = new DBObject[criteria.length];
            int i = 0;
            for (Criteria cr : criteria)
                dbos[i++] = cr.criteria;
            doc.put("$and", dbos);
        }
        return this;
    }
    public MongoQuery or(Criteria...criteria) {
        if (criteria != null && criteria.length > 0) {
            DBObject[] dbos = new DBObject[criteria.length];
            int i = 0;
            for (Criteria cr : criteria)
                dbos[i++] = cr.criteria;
            doc.put("$or", dbos);
        }
        return this;
    }
    public static class Criteria {
        public DBObject criteria = new BasicDBObject();
        public String fn;
        public Criteria() {}
        public Criteria(String fn) {
            this.fn = fn;
        }
        public Criteria(String fn, Object value) {
            this(fn, value, null);
        }
        public Criteria(String fn, Object value, String opCmd) {
            DBObject opv = null;
            if (opCmd != null)
                opv = new BasicDBObject(opCmd, value);
            criteria.put(fn, opv == null ? value : opv);
        }
        public String toSting() {
            return criteria.toString();
        }
    }

    public static class CriteriaContainer extends Criteria {
        public CriteriaContainer() {
            super();
        }
        public CriteriaContainer(String fn, Object value) {
            super(fn, value);
        }
        public CriteriaContainer(String fn, Object value, String opCmd) {
            super(fn, value, opCmd);
        }
        public CriteriaContainer and(Criteria...criteria) {
            if (criteria != null && criteria.length > 0) {
                DBObject[] dbos = new DBObject[criteria.length];
                int i = 0;
                for (Criteria cr : criteria)
                    dbos[i++] = cr.criteria;
                this.criteria.put("$and", dbos);
            }
            return this;
        }
        public CriteriaContainer or(Criteria...criteria) {
            if (criteria != null && criteria.length > 0) {
                DBObject[] dbos = new DBObject[criteria.length];
                int i = 0;
                for (Criteria cr : criteria)
                    dbos[i++] = cr.criteria;
                this.criteria.put("$or", dbos);
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
            criteria.criteria.put(criteria.fn, new BasicDBObject("$gt", val));
            return criteria;
        }
        public Criteria greaterThanOrEq(Object val) {
            criteria.criteria.put(criteria.fn, new BasicDBObject("$gte", val));
            return criteria;
        }
        public Criteria lessThan(Object val) {
            criteria.criteria.put(criteria.fn, new BasicDBObject("$lt", val));
            return criteria;
        }
        public Criteria lessThanOrEq(Object val) {
            criteria.criteria.put(criteria.fn, new BasicDBObject("$lte", val));
            return criteria;
        }
        public Criteria equal(Object val) {
            criteria.criteria.put(criteria.fn, val);
            return criteria;
        }
        public Criteria notEqual(Object val) {
            criteria.criteria.put(criteria.fn, new BasicDBObject("$ne", val));
            return criteria;
        }
        public Criteria contains(String pattern) {
            criteria.criteria.put(criteria.fn, pattern);
            return criteria;
        }
        public Criteria in(Object[] val) {
            criteria.criteria.put(criteria.fn, new BasicDBObject("$in", val));
            return criteria;
        }
    }

    public static enum Sort {
        asc(1),desc(-1);
        public int v;
        private Sort(int v){
            this.v = v;
        }
    }

    public static void main(String[] args) {
        MongoQuery.createQuery("xxx")
                .concat("module_name", "base")
                .set("c3p0.idleConnectionTestPeriod.value", 900)
                .update();
		/*
		q.or(
			q.criteria("msgId").greaterThan("X_O7jjtejQmcTgjACUG_"),
			q.criteria("msgId").equal("-GN_apf1iGq3Km30dbQt")
		);
		*/
    }
}

