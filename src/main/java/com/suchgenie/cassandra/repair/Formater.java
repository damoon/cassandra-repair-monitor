package com.suchgenie.cassandra.repair;

public class Formater
{
    static void render(final StringBuilder buffer, final long repairedAt, final long oldestUnrepaired, final long repairedSize,
            final long unrepairedSize, final long minTs, final long maxTs)
    {
        buffer.append("  last repair:       " + (repairedAt == 0 ? "-" : convertMS(System.currentTimeMillis() - repairedAt)) + "\n");
        buffer.append("  min Timestamp:     " + (minTs == Long.MAX_VALUE ? "-" : minTs) + " (raw)\n");
        buffer.append("  max Timestamp:     " + (maxTs == 0 ? "-" : maxTs) + " (raw)\n");
        buffer.append("  oldest unrepaired: " + (oldestUnrepaired == Long.MAX_VALUE ? "-" : oldestUnrepaired) + " (raw)\n");
        buffer.append("  oldest unrepaired: "
                + (oldestUnrepaired == Long.MAX_VALUE ? "-" : convertMS(System.currentTimeMillis() - oldestUnrepaired / 1000)) + "\n");
        buffer.append("  repaired size:     " + humanReadableByteCount(repairedSize, false) + "\n");
        buffer.append("  unrepaired size:   " + humanReadableByteCount(unrepairedSize, false) + "\n");
    }

    static String convertMS(final long ms)
    {
        final long seconds = ms / 1000 % 60;
        final long minutes = ms / 1000 / 60 % 60;
        final long hours = ms / 1000 / 60 / 60 % 24;
        final long days = ms / 1000 / 60 / 60 / 24;

        String sec, min, hrs;
        if (seconds < 10)
        {
            sec = "0" + seconds;
        }
        else
        {
            sec = "" + seconds;
        }
        if (minutes < 10)
        {
            min = "0" + minutes;
        }
        else
        {
            min = "" + minutes;
        }
        if (hours < 10)
        {
            hrs = "0" + hours;
        }
        else
        {
            hrs = "" + hours;
        }

        if (days != 0)
        {
            return days + " days";
        }

        if (hours == 0)
        {
            return min + ":" + sec;
        }
        else
        {
            return hrs + ":" + min + ":" + sec;
        }

    }

    static String humanReadableByteCount(final long bytes, final boolean si)
    {
        final int unit = si ? 1000 : 1024;
        if (bytes < unit)
        {
            return bytes + " B";
        }
        final int exp = (int) (Math.log(bytes) / Math.log(unit));
        final String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}
