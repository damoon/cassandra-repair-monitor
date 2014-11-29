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
    long maxTimestamp = 0;
    long minTimestamp = Long.MAX_VALUE;
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

                maxTimestamp = Math.max(maxTimestamp, stats.maxTimestamp);
                minTimestamp = Math.min(minTimestamp, stats.minTimestamp);

                if (stats.repairedAt != 0)
                {
                    repairedAt = Math.max(repairedAt, stats.repairedAt);
                    repairedSize += fileEntry.length();
                    repairedFilesCount++;
                }
                else
                {
                    oldestUnrepaired = Math.min(oldestUnrepaired, stats.minTimestamp);
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
        Formater.render(buffer, repairedAt, oldestUnrepaired, repairedSize, unrepairedSize, minTimestamp, maxTimestamp);
        return buffer.toString();
    }
}
