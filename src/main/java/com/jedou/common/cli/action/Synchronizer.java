package com.jedou.common.cli.action;

import java.util.List;
import java.util.Map;

/**
 * Created by tiankai on 14-8-18.
 */
public interface Synchronizer<D> {
    void init();
    List<Map<String, D>> queryDelta(int flag);
    String getDataType();
    boolean isSupportTimestamp();
    boolean isSupportDelete();
    String getTimestampFieldName();
    int processDelta(int flag, D data);
}
