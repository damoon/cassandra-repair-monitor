package com.suchgenie.cassandra.repair;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

public class Keyspace
{
    String name;
    long repairedAt;
    long oldestUnrepaired = Long.MAX_VALUE;
    long repairedSize;
    long unrepairedSize;
    long repairedFilesCount;
    long unrepairedFilesCount;

    List<Table> tables = new LinkedList<>();

    Keyspace(final String basedir, final String name, final String tablePattern) throws IOException
    {
        this.name = name;

        for (final File fileEntry : new File(basedir + "/" + name).listFiles())
        {
            if (fileEntry.isDirectory() && fileEntry.getName().matches(tablePattern))
            {
                final Table table = new Table(basedir, name, fileEntry.getName());

                repairedAt = Math.max(repairedAt, table.repairedAt);
                repairedSize += table.repairedSize;
                repairedFilesCount += table.repairedFilesCount;

                oldestUnrepaired = Math.min(oldestUnrepaired, table.oldestUnrepaired);
                unrepairedSize += table.unrepairedSize;
                unrepairedFilesCount += table.unrepairedFilesCount;

                tables.add(table);
            }
        }
        Collections.sort(tables, new Comparator<Table>() {
            @Override
            public int compare(final Table o1, final Table o2)
            {
                return o1.name.compareTo(o2.name);
            }
        });
    }

    @Override
    public String toString()
    {
        final StringBuilder buffer = new StringBuilder();
        for (final Table table : tables)
        {
            buffer.append(table);
        }
        buffer.append("keyspace " + name + ":\n");
        Formater.render(buffer, repairedAt, oldestUnrepaired, repairedSize, unrepairedSize);
        return buffer.toString();
    }
}
