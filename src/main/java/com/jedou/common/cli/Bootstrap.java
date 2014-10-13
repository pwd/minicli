package com.jedou.common.cli;

import java.util.Iterator;
import java.util.ServiceLoader;

import org.apache.commons.cli.*;

import com.jedou.common.cli.annonation.Option;
import com.jedou.common.cli.annonation.Options;
import com.jedou.common.cli.util.StringUtils;

/**
 * Created by tiankai on 14-8-15.
 */

public class Bootstrap {
    public static ClassLoader bootstrapcl = null;

    private static Class<? extends Commander> loadCommanderImplClass() throws Exception {
        ServiceLoader loader = ServiceLoader.load(Commander.class);
        Class<? extends Commander> implClass = null;
        Iterator<Commander> iter = loader.iterator();
        while (iter.hasNext()) {
            implClass = iter.next().getClass();
            break;
        }
        return implClass;
    }

    private static void parseOptionsAndExecute(Class<? extends Commander> implClass, String[] args) throws Throwable {
        Commander cmd = implClass == null ? null : implClass.newInstance();
        if (cmd == null) throw new RuntimeException("Commander is null.");
        CommandLineParser parser = new GnuParser();
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();
        Options opts = implClass.getAnnotation(Options.class);
        if (opts != null && opts.value() != null) {
            Option[] ops = opts.value();
            for (Option op : ops) {
                if (op != null)
                    options.addOption(op.value(), op.longOpt(), op.hasArg(), op.description());
            }
        }
        Option op = implClass.getAnnotation(Option.class);
        if (op != null)
            options.addOption(op.value(), op.longOpt(), op.hasArg(), op.description());

        try {
            CommandLine commandLine = parser.parse(options, args);
            cmd.execute(commandLine);
        }
        catch (UnrecognizedOptionException uoe) {
            System.out.println("Unrecognized option: " + uoe.getOption());
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp(
                    StringUtils.isEmpty(cmd.getUsage()) ? "Commander" : cmd.getUsage(),
                    null,
                    options,
                    StringUtils.isEmpty(cmd.getVersion()) ? null : "Version: " + cmd.getVersion(),
                    true);
        }
    }

    public static void main(String[] args) throws Throwable {
        String activeClassName = System.getProperty("active.class.name");
        if (activeClassName == null) {
            Class<? extends Commander> implClass = loadCommanderImplClass();
            parseOptionsAndExecute(implClass, args);
        }
        else {
            Class activeClass = Class.forName(activeClassName);
            Class[] interfaces = activeClass.getInterfaces();
            if (interfaces != null) {
                for (Class iface : interfaces) {
                    if (Commander.class.equals(iface)) {
                        parseOptionsAndExecute((Class<? extends Commander>) activeClass, args);
                        System.exit(1);
                    }
                }
            }
            activeClass.getMethod("main", String[].class).invoke(null, (Object) args);
            System.exit(1);
        }
    }

}
