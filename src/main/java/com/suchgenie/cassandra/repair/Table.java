package com.suchgenie.cassandra.repair;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;

public class Table
{
    String keyspace;
    String name;
    long repairedAt;
    long oldestUnrepaired = Long.MAX_VALUE;
    long repairedSize;
    long unrepairedSize;
    long repairedFilesCount;
    long unrepairedFilesCount;

    Table(final String basedir, final String keyspace, final String name) throws IOException
    {
        this.keyspace = keyspace;
        this.name = name;

        for (final File fileEntry : new File(basedir + "/" + keyspace + "/" + name).listFiles())
        {
            if (fileEntry.isFile() && !fileEntry.getName().contains("tmp") && fileEntry.getName().endsWith("Data.db"))
            {

                final Descriptor descriptor = Descriptor.fromFilename(basedir + "/" + keyspace + "/" + name + "/" + fileEntry.getName());
                final Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor,
                        EnumSet.allOf(MetadataType.class));
                final StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);

                if (stats.repairedAt != 0)
                {
                    repairedAt = Math.max(repairedAt, stats.repairedAt);
                    repairedSize += fileEntry.length();
                    repairedFilesCount++;
                }
                else
                {
                    oldestUnrepaired = Math.min(oldestUnrepaired, stats.minTimestamp / 1000);
                    unrepairedSize += fileEntry.length();
                    unrepairedFilesCount++;
                }
            }
        }
    }

    @Override
    public String toString()
    {
        final StringBuilder buffer = new StringBuilder();
        buffer.append("table " + keyspace + " " + name.substring(0, name.length() - 33) + ":\n");
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
