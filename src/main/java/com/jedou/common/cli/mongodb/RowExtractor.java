package com.jedou.common.cli.mongodb;

import com.mongodb.DBObject;

/**
 * Created by tiankai on 14-8-15.
 */
interface RowExtractor {
    Object extractData(DBObject data);
}
