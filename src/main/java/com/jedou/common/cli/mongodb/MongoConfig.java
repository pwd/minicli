package com.jedou.common.cli.mongodb;

import java.util.*;

import com.jedou.common.cli.util.ConfigUtil;
import com.jedou.common.cli.util.StringUtils;
import com.mongodb.*;

/**
 * mongodb连接配置
 * @since 2013-05-15
 */
public class MongoConfig {
    private static MongoConfig defaultMagic = null;
    public static MongoConfig getDefaultMagic() {
        if (defaultMagic == null) {
        MongoConfig mc = new MongoConfig();
            mc.addr = ConfigUtil.getProperty("mongodb.addr");
            mc.dbName = ConfigUtil.getProperty("mongodb.dbname");
            mc.username = ConfigUtil.getProperty("mongodb.username");
            mc.password = ConfigUtil.getProperty("mongodb.password");
            defaultMagic = mc;
        }
        return defaultMagic;
    }
    public static class Options {
        public boolean autoConnectRetry = true;
        public int connectionsPerHost;
        public int connectTimeout;
        public long maxAutoConnectRetryTime;
        public int maxWaitTime;
        public boolean socketKeepAlive = true;
        public int socketTimeout;
        public int threadsAllowedToBlockForConnectionMultiplier;

        public boolean isAutoConnectRetry() {
            return autoConnectRetry;
        }
        public void setAutoConnectRetry(boolean autoConnectRetry) {
            this.autoConnectRetry = autoConnectRetry;
        }
        public int getConnectionsPerHost() {
            return connectionsPerHost;
        }
        public void setConnectionsPerHost(int connectionsPerHost) {
            this.connectionsPerHost = connectionsPerHost;
        }
        public int getConnectTimeout() {
            return connectTimeout;
        }
        public void setConnectTimeout(int connectTimeout) {
            this.connectTimeout = connectTimeout;
        }
        public long getMaxAutoConnectRetryTime() {
            return maxAutoConnectRetryTime;
        }
        public void setMaxAutoConnectRetryTime(long maxAutoConnectRetryTime) {
            this.maxAutoConnectRetryTime = maxAutoConnectRetryTime;
        }
        public int getMaxWaitTime() {
            return maxWaitTime;
        }
        public void setMaxWaitTime(int maxWaitTime) {
            this.maxWaitTime = maxWaitTime;
        }
        public boolean isSocketKeepAlive() {
            return socketKeepAlive;
        }
        public void setSocketKeepAlive(boolean socketKeepAlive) {
            this.socketKeepAlive = socketKeepAlive;
        }
        public int getSocketTimeout() {
            return socketTimeout;
        }
        public void setSocketTimeout(int socketTimeout) {
            this.socketTimeout = socketTimeout;
        }
        public int getThreadsAllowedToBlockForConnectionMultiplier() {
            return threadsAllowedToBlockForConnectionMultiplier;
        }
        public void setThreadsAllowedToBlockForConnectionMultiplier(
                int threadsAllowedToBlockForConnectionMultiplier) {
            this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
        }
    }


    /**
     * mongodb主机地址串：<host>[:<port>][,<host>[:<port>]]+
     */
    public String addr;
    /**
     * mongodb库名
     */
    public String dbName;
    /**
     * mongodb集合名
     */
    public String collectionName;
    /**
     * mongodb用户名
     */
    public String username;
    /**
     * mongodb密码
     */
    public String password;
    public String desc;
    public Options options;

    private MongoClient mongo;
    private com.mongodb.DB db;

    public static Map<String, MongoClient> mcMap = Collections.synchronizedMap(new HashMap<String, MongoClient>());

    public MongoConfig() {}
    public MongoConfig(String addr, String dbName, String collectionName) {
        this.addr = addr;
        this.dbName = dbName;
        this.collectionName = collectionName;
    }

    public MongoClient getMongo() {
        if(addr != null){
            if(!addr.contains(":")){
                addr = addr + ":27017";
                mongo = mcMap.get(addr);
            }
        }
        if (mongo == null) {
            MongoClientOptions mo = null;
            if (options != null) {
                mo = new MongoClientOptions.Builder()
                        .autoConnectRetry(options.autoConnectRetry)
                        .connectionsPerHost(options.connectionsPerHost)
                        .connectTimeout(options.connectTimeout)
                        .maxAutoConnectRetryTime(options.maxAutoConnectRetryTime)
                        .maxWaitTime(options.maxWaitTime)
                        .socketKeepAlive(options.socketKeepAlive)
                        .socketTimeout(options.socketTimeout)
                        .threadsAllowedToBlockForConnectionMultiplier(options.threadsAllowedToBlockForConnectionMultiplier)
                        .build();
            }
            String[] addrs = addr.split(",");
            List<ServerAddress> saList = new ArrayList<ServerAddress>();
            for (String addr : addrs) {
                String[] hp = addr.split(":");
                String[] hp2 = new String[2];
                hp2[0] = hp[0];
                hp2[1] = hp.length > 1 ? hp[1] : "27017";

                try {
                    ServerAddress sa = new ServerAddress(hp2[0], Integer.parseInt(hp2[1]));
                    saList.add(sa);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(addr, e);
                }
            }
            List<MongoCredential> mcList = null;
            if (StringUtils.isNotEmpty(username)) {
                mcList = new ArrayList<MongoCredential>(1);
                mcList.add(
                        MongoCredential.createMongoCRCredential(
                                username, dbName, password == null ?
                                new char[0] : password.toCharArray()));
            }
            mongo = mo == null ? new MongoClient(saList, mcList) : new MongoClient(saList, mcList, mo);

            mcMap.put(addr, mongo);
        }
        return mongo;
    }

    public com.mongodb.DB getMongoDB() {
        if (db == null) {
            db = getMongo().getDB(dbName);
        }
        return db;
    }

    public DBCollection getDBCollection() {
        return getMongoDB().getCollection(collectionName);
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Options getOptions() {
        return options;
    }

    public void setOptions(Options options) {
        this.options = options;
    }

    @Override
    public String toString() {
        return "MongoConfig{" +
                "addr='" + addr + '\'' +
                ", dbName='" + dbName + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", desc='" + desc + '\'' +
                ", options=" + options +
                '}';
    }

    public static void main(String[] args) {
        MongoConfig mc = new MongoConfig();
        mc.addr = "10.3.33.33,10.3.33.34:27018,";
        String absolutePath = mc.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        System.out.println(mc);
        System.out.println(absolutePath);
    }
}
