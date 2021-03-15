package org.apache.kyuubi.web.utils;

import org.apache.kyuubi.common.KyuubiConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KyuubiWebConf {
    private static final Logger LOGGER = LoggerFactory.getLogger(KyuubiWebConf.class);

    public static final String Kyuubi_HOME = System.getenv().getOrDefault("Kyuubi_HOME", ".");
    public static final String Kyuubi_CONF_DIR = System.getenv().getOrDefault("Kyuubi_CONF_DIR", Kyuubi_HOME + "/conf/");
    public static final String Kyuubi_CONF_FILE_NAME = "Kyuubi.conf";


    private static Map<String, String> loadKyuubiProperties() {
        Map<String, String> map = new HashMap<>();
        File confFile = new File(Kyuubi_CONF_DIR, Kyuubi_CONF_FILE_NAME);
        if (confFile.exists() && confFile.isFile()) {
            LOGGER.info("loading Kyuubi conf file: " + confFile.getAbsolutePath());
            Properties properties = new Properties();
            BufferedReader reader = null;
            try {
                reader = Files.newBufferedReader(confFile.toPath(), StandardCharsets.UTF_8);
                properties.load(reader);
                reader.close();
            } catch (IOException e) {
                throw new KyuubiWebException("Can not read Kyuubi web config file: " + confFile.getAbsolutePath(), e);
            }

            for (String stringPropertyName : properties.stringPropertyNames()) {
                map.put(stringPropertyName, properties.getProperty(stringPropertyName));
            }

        }
        return map;
    }

    private static KyuubiWebConf Kyuubi_WEB_CONF = null;

    public static synchronized KyuubiWebConf load() {
        if (Kyuubi_WEB_CONF == null) {
            Kyuubi_WEB_CONF = new KyuubiWebConf();
            Kyuubi_WEB_CONF.confs.putAll(KyuubiConfigs.defaults);
            Kyuubi_WEB_CONF.confs.putAll(loadKyuubiProperties());
        }
        return Kyuubi_WEB_CONF;
    }

    private Map<String, String> confs = new HashMap<>();

    public String get(String key) {
        return confs.get(key);
    }

    public String get(KyuubiConfigs configs) {
        return confs.getOrDefault(configs.getKey(), configs.getDefaultValue());
    }

    public int getInt(KyuubiConfigs configs) {
        String s = get(configs);
        return Integer.parseInt(s);
    }

    public boolean getBoolean(KyuubiConfigs configs) {
        String s = get(configs);
        return Boolean.parseBoolean(s);
    }

    public Map<String, String> getAll() {
        return new HashMap<>(confs);
    }

    public String get(String s, String aDefault) {
        return confs.getOrDefault(s, aDefault);
    }
}
