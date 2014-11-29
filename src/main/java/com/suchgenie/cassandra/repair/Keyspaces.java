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
            buffer.append("  repairedAt:           " + keyspace.repairedAt + "\n");
            buffer.append("  oldestUnrepaired:     " + (keyspace.oldestUnrepaired == Long.MAX_VALUE ? "-" : keyspace.oldestUnrepaired)
                    + "\n");
            buffer.append("  repairedSize:         " + keyspace.repairedSize + "\n");
            buffer.append("  unrepairedSize:       " + keyspace.unrepairedSize + "\n");
        }
        buffer.append("all keyspaces:\n");
        buffer.append("  repairedAt:           " + repairedAt + "\n");
        buffer.append("  oldestUnrepaired:     " + (oldestUnrepaired == Long.MAX_VALUE ? "-" : oldestUnrepaired) + "\n");
        buffer.append("  repairedSize:         " + repairedSize + "\n");
        buffer.append("  unrepairedSize:       " + unrepairedSize + "\n");
        // buffer.append("  repairedFilesCount:   " + repairedFilesCount +
        // "\n");
        // buffer.append("  unrepairedFilesCount: " + unrepairedFilesCount +
        // "\n");
        return buffer.toString();
    }
}
