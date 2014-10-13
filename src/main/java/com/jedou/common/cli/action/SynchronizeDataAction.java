package com.jedou.common.cli.action;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.jedou.common.cli.jdbc.JdbcQuery;
import com.jedou.common.cli.logger.LogLevel;
import com.jedou.common.cli.logger.Logger;
import com.jedou.common.cli.mongodb.MongoQuery;
import com.jedou.common.cli.util.*;

public class SynchronizeDataAction {
    public interface CONSTs {
        int flag_valid = 1;
        int flag_invalid = 0;
        int valid_insert = 1;
        int valid_update = 0;
    }
    private static class MCOLs {
        static String last_sync_time;
        static void initConfig() {
            last_sync_time = ConfigUtil.getProperty("mongodb.colname.last_sync_time", "last_sync_time");
        }
    }
    static void initCollections() {
        if (!MongoQuery.createQuery(MCOLs.last_sync_time).exists()) {
            MongoQuery.createQuery(MCOLs.last_sync_time).createCol();
        }
    }

    private static final String SYNC_ZERS_CONF="syncdata.synchronizers";

    static Map<String, Long> lastSyncTimestamp = Maps.newHashMap();
    static Map<String, Long> currentSyncTimestamp = Maps.newHashMap();

    List<Synchronizer> zers = Lists.newArrayList();

    public static long getLastSyncTimestamp(String idx) {
        if (lastSyncTimestamp.get(idx) == null) {
            MongoQuery mq = MongoQuery.createQuery(MCOLs.last_sync_time).concat("_id", idx);
            Map<String, Object> rs = mq.first(Map.class);
            Long ts = null;
            if (rs == null) {
                Calendar cal = Calendar.getInstance();
                cal.set(1900, 0, 1);
                ts = cal.getTime().getTime();
                setCurrentSyncTimestamp(idx, ts);
            }
            else {
                ts = (Long) rs.get("ts");
            }
            lastSyncTimestamp.put(idx, ts);
        }
        return lastSyncTimestamp.get(idx);
    }
    static void setCurrentSyncTimestamp(String idx, Long ts) {
        if (currentSyncTimestamp.get(idx) == null)
            currentSyncTimestamp.put(idx, ts);
        else if (ts != null && ts.compareTo(currentSyncTimestamp.get(idx)) > 0)
            currentSyncTimestamp.put(idx, ts);
    }
    static void saveCurrentSyncTimestamp(String idx) {
        if (currentSyncTimestamp.get(idx) != null) {
            MongoQuery.createQuery(MCOLs.last_sync_time).concat("_id", idx).concat("ts", currentSyncTimestamp.get(idx)).save();
            if (Logger.isEnable(LogLevel.debug))
                Logger.Log(LogLevel.debug, "[%s] 保存下次同步开始时间: |%s", idx, currentSyncTimestamp.get(idx));
        }
    }

    int[] processDelta(Synchronizer er, int flag) {
        int[] count = new int[] {0, 0};
        long ts = getLastSyncTimestamp(er.getDataType());
        if (Logger.isEnable(LogLevel.debug))
            Logger.Log(LogLevel.debug, "[%s] 同步数据开始时间：|%s", er.getDataType(), new Date(ts));
        List<Map<String, Object>> delta = er.queryDelta(flag);
        if (Logger.isEnable(LogLevel.debug))
            Logger.Log(LogLevel.debug, "[%s] 查询同步数据 [%s] | Find |%s| rows.", er.getDataType(), flag, ((List)delta).size());
        long bigger = getLastSyncTimestamp(er.getDataType());
        if (!Utils.isEmpty(delta)) {
            for (Map<String, Object> o : delta) {
                try {
                    if (er.isSupportTimestamp())
                        bigger = (Long) o.get(er.getTimestampFieldName());
                    int idx = er.processDelta(flag, o);
                    count[idx]++;
                }
                catch (Throwable e) {
                    Logger.Log(LogLevel.warn, "[%s][%s]| 处理同步数据失败：|%s|%s", er.getDataType(), flag, e.getMessage(), o);
                    Logger.Exception(LogLevel.warn, e);
                }
            }
            if (er.isSupportTimestamp())
                setCurrentSyncTimestamp(er.getDataType(), bigger);
        }
        return count;
    }
    void initSynchronizers() {
        String zerstr = ConfigUtil.getProperty(SYNC_ZERS_CONF);
        if (StringUtils.isNotEmpty(zerstr)) {
            String[] zersplits = zerstr.split(",");
            for (String name : zersplits) {
                String className = name.trim();
                if (className.length() > 0) {
                    try {
                        Logger.info("%s", className);
                        Synchronizer zer = ClassUtil.create(className, Synchronizer.class);
                        zers.add(zer);
                    }
                    catch (Throwable e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }
            }
        }
    }
    void execute() {
        if (Logger.isEnable(LogLevel.showsqlparams))
            JdbcQuery.show_param = true;
        if (Logger.isEnable(LogLevel.showsql))
            JdbcQuery.show_sql = true;
        initSynchronizers();
        if (Utils.isNotEmpty(zers)) {
            ExecutorService executor = Executors.newFixedThreadPool(zers.size());
            for (final Synchronizer er : zers) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        Logger.Log(LogLevel.info, "[%s] 开始同步数据...", er.getDataType());
                        er.init();
                        int[] cnt = processDelta(er, CONSTs.flag_valid);
                        Logger.Log(LogLevel.info, "[%s] 已处理新增同步数据：|%s|条。", er.getDataType(), cnt[CONSTs.valid_insert]);
                        Logger.Log(LogLevel.info, "[%s] 已处理修改同步数据：|%s|条。", er.getDataType(), cnt[CONSTs.valid_update]);
                        if (er.isSupportDelete()) {
                            int[] c1 = processDelta(er, CONSTs.flag_invalid);
                            Logger.Log(LogLevel.info, "[%s] 已处理删除同步数据：|%s|条。", er.getDataType(), c1);
                        }
                    }
                });
            }
            executor.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args != null && args.length > 0) {
            String logLevelName = args[0];
            try {
                Logger.DefaultLogLevel = LogLevel.valueOf(logLevelName);
                if (Logger.isEnable(LogLevel.debug))
                    Logger.Log(LogLevel.debug, "日志级别: %s", logLevelName);
            }
            catch (Exception e) {
                System.out.println("Usage: [loglevel]\n\tLog Level: debug|info|warn|error\n\tDefault log level is \"info\"");
                return;
            }
        }
        if (Logger.isEnable(LogLevel.debug))
            Logger.Log(LogLevel.debug, "Use timezone: %s", TimeZone.getDefault());
        ConfigUtil.initProperties();
        MCOLs.initConfig();
        initCollections();
        new SynchronizeDataAction().execute();
    }
}
