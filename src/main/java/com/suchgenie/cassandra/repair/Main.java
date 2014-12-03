package com.suchgenie.cassandra.repair;

import java.io.IOException;

import dnl.utils.text.table.TextTable;

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

        final String[] columnNames = { "Keyspace", "Columnfamily", "last repair", "min Timestamp", "max Timestamp", "oldest unrepaired",
                "unrepaired Days", "repaired size", "unrepaired size" };

        new TextTable(columnNames, new Keyspaces("/var/lib/cassandra/data", keyspacePattern, tablePattern).getData()).printTable();

        System.out.println();

    }
}
