package org.apache.kyuubi.common.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

public class KyuubiUtils {
    private static String KYUUBI_ID_FILE = "kyuubi_id.txt";
    private static String KYUUBI_ID = null;
    private static String LOCAL_IP = null;

    /**
     * load current server id
     */
    public static synchronized String loadServerId() {
        if (KYUUBI_ID != null) {
            return KYUUBI_ID;
        }
        File serverIdFile = new File(KYUUBI_ID_FILE);
        try {
            if (!serverIdFile.exists() || !serverIdFile.isFile()) {
                FileUtils.deleteQuietly(serverIdFile);
                KYUUBI_ID = UUID.randomUUID().toString();

                FileUtils.writeStringToFile(serverIdFile, KYUUBI_ID);

            } else {
                KYUUBI_ID = FileUtils.readFileToString(serverIdFile).trim();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error create KYUUBI Web Id file: " + serverIdFile.getAbsolutePath(), e);
        }
        return KYUUBI_ID;
    }

    /**
     * load current server ip
     *
     * @return Ip
     */
    public static synchronized String loadServerIp() {
        if (LOCAL_IP != null) {
            return LOCAL_IP;
        }
        try {
            LOCAL_IP = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException("Error get KYUUBI Web local ip address", e);
        }
        return LOCAL_IP;
    }

    public static synchronized String loadServerIp(String host) {
        if (host == null || host.trim().equals("0.0.0.0")) {
            return loadServerIp();
        } else {
            return host;
        }
    }

    /**
     * Get the index separating the user name from domain name (the user's name up
     * to the first '/' or '@').
     *
     * @param userName full user name.
     * @return index of domain match or -1 if not found
     */
    public static int indexOfDomainMatch(String userName) {
        if (userName == null) {
            return -1;
        }
        int idx = userName.indexOf('/');
        int idx2 = userName.indexOf('@');
        int endIdx = Math.min(idx, idx2); // Use the earlier match.
        // Unless at least one of '/' or '@' was not found, in
        // which case, user the latter match.
        if (endIdx == -1) {
            endIdx = Math.max(idx, idx2);
        }
        return endIdx;
    }

    public static String dateToString(Date date) {
        return Optional.ofNullable(date).map(it -> DateFormatUtils.format(it, "yyyy-MM-dd HH:mm:ss")).orElse("");
    }

    public static String durationToString(long duration) {
        return DurationFormatUtils.formatDuration(duration, "HH:mm:ss");
    }

    public static String durationToString(Date start, Date end) {
        if (start == null || end == null) {
            return "";
        }
        long duration = Math.abs(start.getTime() - end.getTime());
        return durationToString(duration);
    }
}
