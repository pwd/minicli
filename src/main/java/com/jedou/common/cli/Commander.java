package com.jedou.common.cli;

import org.apache.commons.cli.CommandLine;

/**
 * Created by tiankai on 14-10-9.
 */
public interface Commander {
    public void execute(CommandLine commandLine);
    public String getUsage();
    public String getVersion();
}
