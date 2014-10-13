package com.jedou.common.cli.logger;

/**
 * Created by tiankai on 14-8-15.
 */
public enum LogLevel {
    showsqlparams(-2), showsql(-1), debug(0), info(1), warn(2), error(3);
    int level = 1;
    LogLevel(int level) {
        this.level = level;
    }
    public int getLevel() {
        return level;
    }
}
