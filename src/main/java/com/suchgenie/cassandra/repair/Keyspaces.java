package com.suchgenie.cassandra.repair;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

public class Keyspaces
{
    long repairedAt;
    long maxTimestamp = 0;
    long minTimestamp = Long.MAX_VALUE;
    long oldestUnrepaired = Long.MAX_VALUE;
    long repairedSize;
    long unrepairedSize;
    long repairedFilesCount;
    long unrepairedFilesCount;

    List<Keyspace> keyspaces = new LinkedList<>();

    Keyspaces(final String basedir, final String keyspacePattern, final String tablePattern) throws IOException
    {
        for (final File fileEntry : new File(basedir).listFiles())
        {
            if (fileEntry.isDirectory() && !fileEntry.getName().startsWith("system") && fileEntry.getName().matches(keyspacePattern))
            {
                final Keyspace keyspace = new Keyspace(basedir, fileEntry.getName(), tablePattern);

                repairedAt = Math.max(repairedAt, keyspace.repairedAt);
                repairedSize += keyspace.repairedSize;
                repairedFilesCount += keyspace.repairedFilesCount;

                oldestUnrepaired = Math.min(oldestUnrepaired, keyspace.oldestUnrepaired);
                unrepairedSize += keyspace.unrepairedSize;
                unrepairedFilesCount += keyspace.unrepairedFilesCount;

                maxTimestamp = Math.max(maxTimestamp, keyspace.maxTimestamp);
                minTimestamp = Math.min(minTimestamp, keyspace.minTimestamp);

                keyspaces.add(keyspace);
            }
        }
        Collections.sort(keyspaces, new Comparator<Keyspace>() {
            @Override
            public int compare(final Keyspace o1, final Keyspace o2)
            {
                return o1.name.compareTo(o2.name);
            }
        });
    }

    @Override
    public String toString()
    {
        final StringBuilder buffer = new StringBuilder();
        for (final Keyspace keyspace : keyspaces)
        {
            buffer.append(keyspace);
        }
        for (final Keyspace keyspace : keyspaces)
        {

            buffer.append("keyspace " + keyspace.name + ":\n");
            Formater.render(buffer, keyspace.repairedAt, keyspace.oldestUnrepaired, keyspace.repairedSize, keyspace.unrepairedSize,
                    keyspace.minTimestamp, keyspace.maxTimestamp);
        }
        buffer.append("all keyspaces:\n");
        Formater.render(buffer, repairedAt, oldestUnrepaired, repairedSize, unrepairedSize, minTimestamp, maxTimestamp);
        return buffer.toString();
    }

    public Object[][] getData()
    {
        int c = 0;
        for (final Keyspace keyspace : keyspaces)
        {
            c += 1;
            c += keyspace.tables.size();
        }
        c += 1;

        final Object[][] data = new Object[c][];

        c = 0;
        for (final Keyspace keyspace : keyspaces)
        {
            c = keyspace.addData(c, data);
        }

        data[c++] = Formater.toRow("---", "---", repairedAt, oldestUnrepaired, repairedSize, unrepairedSize, minTimestamp, maxTimestamp);

        return data;
    }
}
