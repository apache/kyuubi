package org.apache.kyuubi.web.utils;

/**
 * TODO
 *
 * @author lixk
 * @version 1.0
 * @date 2020/10/13 19:45
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class YarnUtils {

    public static final Logger LOGGER = LoggerFactory.getLogger(YarnUtils.class);

    public static final String YARN_HTTP_POLICY = "yarn.http.policy";
    public static final String RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.webapp.address";
    public static final String RESOURCE_MANAGER_HTTPS_ADDRESS = "yarn.resourcemanager.webapp.https.address";

    private static final String IS_RM_HA_ENABLED = "yarn.resourcemanager.ha.enabled";
    private static final String RESOURCE_MANAGER_IDS = "yarn.resourcemanager.ha.rm-ids";
    private static final String RM_NODE_STATE_URL = "%s://%s/ws/v1/cluster/info";
    public static final ObjectMapper objectMapper = new ObjectMapper();

    public static Configuration getYarnConf() {
        Configuration conf = new Configuration();
        Map<String, String> env = System.getenv();
        String yarn_conf_dir = env.get("YARN_CONF_DIR");
        String hadoop_conf_dir = env.get("HADOOP_CONF_DIR");

        String spark_home = env.get("SPARK_HOME");
        String yarn_on_spark = new File(spark_home, "yarn-conf").getAbsolutePath();
        addHadoopConfIfExists(conf, hadoop_conf_dir);
        addHadoopConfIfExists(conf, yarn_conf_dir);
        addHadoopConfIfExists(conf, yarn_on_spark);
        return conf;
    }


    public static void addHadoopConfIfExists(Configuration configuration, String path) {
        if (path != null && !path.isEmpty()) {
            File file = new File(path);
            if (file.exists()) {
                if (file.isFile()) {
                    configuration.addResource(file.getAbsolutePath());
                } else {
                    try {
                        configuration.addResource(new File(file, "core-site.xml").toURI().toURL());
                        configuration.addResource(new File(file, "hdfs-site.xml").toURI().toURL());
                        configuration.addResource(new File(file, "mapred-site.xml").toURI().toURL());
                        configuration.addResource(new File(file, "yarn-site.xml").toURI().toURL());
                    } catch (MalformedURLException e) {
                        LOGGER.warn("Error to addResource", e);
                    }
                }
            }
        }
    }

    public static boolean isHttpOnly(Configuration configuration) {
        return configuration.get(YARN_HTTP_POLICY, "HTTP_ONLY").equals("HTTP_ONLY");
    }

    /**
     * get the yarn web addredss like host:port
     *
     * @param configuration yarn configuration
     * @return host:port
     */
    public static String getActiveYarnWebAddress(Configuration configuration) {
        String _resourceManagerAddress = null;
        boolean httpOnly = isHttpOnly(configuration);
        String protocol = httpOnly ? "http" : "https";
        boolean haEnabled = configuration.getBoolean(IS_RM_HA_ENABLED, false);
        if (haEnabled) {
            String resourceManagers = configuration.get(RESOURCE_MANAGER_IDS);
            if (resourceManagers != null) {
                LOGGER.info("The list of RM IDs are " + resourceManagers);
                List<String> ids = Arrays.asList(resourceManagers.split(","));
                for (String id : ids) {
                    try {
                        String resourceManager = httpOnly ? configuration.get(RESOURCE_MANAGER_ADDRESS + "." + id) :
                                configuration.get(RESOURCE_MANAGER_HTTPS_ADDRESS + "." + id);
                        String resourceManagerURL = String.format(RM_NODE_STATE_URL, protocol, resourceManager);
                        LOGGER.info("Checking RM URL: " + resourceManagerURL);

                        JsonNode rootNode = objectMapper.readTree(new URL(resourceManagerURL));
                        String status = rootNode.path("clusterInfo").path("haState").textValue();
                        if (status.equals("ACTIVE")) {
                            LOGGER.info(resourceManager + " is ACTIVE");
                            _resourceManagerAddress = resourceManager;
                            break;
                        } else {
                            LOGGER.info(resourceManager + " is STANDBY");
                        }
                    } catch (IOException e) {
                        LOGGER.info("Error fetching Json for resource manager " + id + " status " + e.getMessage());
                    }
                }
            }
        } else {
            _resourceManagerAddress = configuration.get(RESOURCE_MANAGER_ADDRESS);
        }
        if (_resourceManagerAddress == null) {
            return null;
        }
        try {
            String[] hostPort = _resourceManagerAddress.split(":");
            String host = hostPort[0];
            String port = host.length() == 1 ? String.valueOf(YarnConfiguration.DEFAULT_RM_WEBAPP_PORT) : hostPort[1];
            InetAddress[] inetAddresses = InetAddress.getAllByName(host);
            for (InetAddress inetAddress : inetAddresses) {
                if (inetAddress.getHostAddress().startsWith("127.")) {
                    continue;
                }
                return inetAddress.getHostAddress() + ":" + port;
            }
        } catch (UnknownHostException e) {
            throw new RuntimeException("error to resolve Yarn Resource Manager hostname: " + _resourceManagerAddress);
        }
        return _resourceManagerAddress;
    }

    public static String resolveBaseYarnUrl() {
        Configuration configuration = getYarnConf();
        String activeYarnWebAddress = getActiveYarnWebAddress(configuration);
        if (isHttpOnly(configuration)) {
            return "http://" + activeYarnWebAddress;
        } else {
            return "https://" + activeYarnWebAddress;
        }
    }

    /**
     * get the application url on the yarn
     *
     * @param configuration
     * @param applicationId
     * @return
     */
    public static String resolveApplicationUrl(YarnConfiguration configuration, String applicationId) {
        String activeYarnWebAddress = getActiveYarnWebAddress(configuration);
        if (isHttpOnly(configuration)) {
            return "http://" + activeYarnWebAddress + "/cluster/app/" + applicationId;
        } else {
            return "https://" + activeYarnWebAddress + "/cluster/app/" + applicationId;
        }
    }

    /**
     * get  the application track url on the yarn
     *
     * @param configuration
     * @param applicationId
     * @return
     */
    public static String resolveSparkUrl(YarnConfiguration configuration, String applicationId) {
        String activeYarnWebAddress = getActiveYarnWebAddress(configuration);
        if (isHttpOnly(configuration)) {
            return "http://" + activeYarnWebAddress + "/proxy/" + applicationId;
        } else {
            return "https://" + activeYarnWebAddress + "/proxy/" + applicationId;
        }
    }

    /**
     * get  application status
     *
     * @param applicationId
     * @return
     */
    public static YarnApplicationState getApplicationStatus(String applicationId) throws IOException, YarnException {
        YarnClient yarn = YarnClient.createYarnClient();
        yarn.init(getYarnConf());
        yarn.start();
        YarnApplicationState state = yarn.getApplicationReport((ApplicationId.fromString(applicationId))).getYarnApplicationState();
        yarn.stop();
        return state;
    }

    public static boolean isRunningState(YarnApplicationState yarnApplicationState) {
        return yarnApplicationState == YarnApplicationState.ACCEPTED ||
                yarnApplicationState == YarnApplicationState.NEW ||
                yarnApplicationState == YarnApplicationState.NEW_SAVING ||
                yarnApplicationState == YarnApplicationState.RUNNING ||
                yarnApplicationState == YarnApplicationState.SUBMITTED;
    }

    public static boolean isFinishedState(YarnApplicationState yarnApplicationState) {
        return yarnApplicationState == YarnApplicationState.FAILED ||
                yarnApplicationState == YarnApplicationState.FINISHED ||
                yarnApplicationState == YarnApplicationState.KILLED;
    }

    public static YarnClient createYarnClient() {
        YarnClient client = YarnClient.createYarnClient();
        Configuration yarnConf = YarnUtils.getYarnConf();
        yarnConf.set("yarn.resourcemanager.connect.max-wait.ms", "30000");
        client.init(yarnConf);
        client.start();
        return client;
    }
}
