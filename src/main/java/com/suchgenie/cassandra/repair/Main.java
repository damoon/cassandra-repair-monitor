package com.suchgenie.cassandra.repair;

import java.io.IOException;

public class Main
{
    public static void main(final String[] args) throws IOException
    {
        String keyspacePattern = ".*";
        String tablePattern = ".*";
        if (args.length > 0)
        {
            keyspacePattern = args[0];
        }
        if (args.length > 1)
        {
            tablePattern = args[1];
        }

        System.out.println(new Keyspaces("/var/lib/cassandra/data", keyspacePattern, tablePattern));

    }
}
