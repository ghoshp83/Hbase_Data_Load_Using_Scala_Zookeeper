package com.pralay.core.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Contains configuration parameters for Application Context.
 *
 *
 */
public class ExecutionConfiguration {

    private static Logger LOG = LoggerFactory.getLogger(ExecutionConfiguration.class);

    private String bindAddress;
    private String redisHostName;
    private int redisPort;
    private int databaseIndex = 5;
    private boolean redisCluster = true;
    private String appName;
    private List<String> redisClusterHosts = new ArrayList<String>();
    private List<ServerAddr> kafkaBrokerHosts = new ArrayList<ServerAddr>();
    private List<ServerAddr> zookeeperHosts = new ArrayList<ServerAddr>();
    private Map<String, String> customConfig = new HashMap<String, String>();
    private String traceHost;
    private String psqlHostName = "localhost";
    private int psqlPort =  5432;
    private String psqlDatabase;
    private String psqlUsername;
    private String psqlPassword;
    private boolean isExternal = false;

    public ExecutionConfiguration() {
        try {
            this.bindAddress = InetAddress.getLocalHost().getHostName();
            setFromEnvironment();
        } catch (UnknownHostException e) {
            LOG.error("local host name could not get: " + e.getMessage() + " setting up to 0.0.0.0");
            this.bindAddress = "0.0.0.0";
        }
    }

    private void setFromEnvironment() {
        Map<String, String> envs = System.getenv();

        if (envs.containsKey("REDIS_HOST")) {
            setRedisHostName(envs.get("REDIS_HOST"));
        }

        if (envs.containsKey("REDIS_PORT")) {
            int port = 7000;
            boolean succ = false;
            try {
                port = Integer.parseInt(envs.get("REDIS_PORT"));
                succ = true;
            } catch (NumberFormatException ex) {
                LOG.warn("invalid port number set to REDIS_PORT env");
            } finally {
                if (succ) {
                        setRedisPort(port);
                }
            }
        }

        if (envs.containsKey("APPNAME")) {
            setAppname(envs.get("APPNAME"));
        }

        if (envs.containsKey("TRACE_HOST")) {
            setTraceHost(envs.get("TRACE_HOST"));
        }

        if (envs.containsKey("SQL_HOST")) {
            setPSQLHost(envs.get("SQL_HOST"));
        }

        if (envs.containsKey("SQL_PORT")) {
            try {
                setPSQLPort(Integer.parseInt(envs.get("SQL_PORT")));
            }
            catch(NumberFormatException ex) {
                LOG.warn("No SQL_PORT set, defaulted to 5432.");
                setPSQLPort(5432);
            }
        }

        if (envs.containsKey("SQL_DATABASE")) {
            setPSQLDatabase(envs.get("SQL_DATABASE"));
        }

        if (envs.containsKey("SQL_USERNAME")) {
            setPSQLUsername(envs.get("SQL_USERNAME"));
        }

        if (envs.containsKey("SQL_PASSWORD")) {
            setPSQLPassword(envs.get("SQL_PASSWORD"));
        }

        if (envs.containsKey("ZOOKEEPER_HOSTS")) {
                        int port = 2181;
                        try {
                            port = Integer.parseInt(envs.get("ZOOKEEPER_PORT"));
                        } catch (NumberFormatException ex) {
                            LOG.warn("invalid port number for ZOOKEEPEER_PORT: " + envs.get("ZOOKEEPER_PORT") + ", use default: " + port);
                        }
            setZookeeperHosts(ServerAddr.fromMultiString(envs.get("ZOOKEEPER_HOSTS"), port));
        }

        // infer that the app runs externally based on missing env variables
        if (!envs.containsKey("SQL_PASSWORD")) { //TODO would be safer to have a special purpose variable, eg. ENV_INTERNAL=1
            LOG.info("Application configured as EXTERNAL to Execution Framework (WARN: temporarily using $SQL_PASSWORD env variable to make the decision - a proper variable needs to be added).");
            setAppAsExternal();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ExecutionConfiguration[");
        sb.append("redisHostName=" + this.getRedisHostName());
        sb.append(";redisPort=" + this.redisPort);
        return sb.append("]").toString();
    }

    /**
     * @return the redisPort
     */
    public int getRedisPort() {
        return redisPort;
    }

    /**
     * @param redisPort the redisPort to set
     */
    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    /**
     * @return the redisHostName
     */
    public String getRedisHostName() {
        return redisHostName;
    }

    /**
     * @param redisHostName the redisHostName to set
     */
    public void setRedisHostName(String redisHostName) {
        this.redisHostName = redisHostName;
    }

    /**
     * @return the redisCluster
     */
    public boolean isRedisCluster() {
        return redisCluster;
    }

    /**
     * @param redisCluster the redisCluster to set
     */
    public void setRedisCluster(boolean redisCluster) {
        this.redisCluster = redisCluster;
    }

    /**
     * @return the redisClusterHosts
     */
    public List<String> getRedisClusterHosts() {
        return redisClusterHosts;
    }

    /**
     * @param redisClusterHosts the redisClusterHosts to set
     */
    public void setRedisClusterHosts(List<String> redisClusterHosts) {
        this.redisClusterHosts = redisClusterHosts;
    }

    /**
     * @return the bindAddress
     */
    public String getBindAddress() {
        return bindAddress;
    }

    /**
     * @param bindAddress the bindAddress to set
     */
    public void setBindAddress(String bindAddress) {
        this.bindAddress = bindAddress;
    }

    public int getDatabaseIndex() {
        return databaseIndex;
    }

    public void setDatabaseIndex(int databaseIndex) {
        this.databaseIndex = databaseIndex;
    }

    /**
     * @return the Application container id
     */
    public String getContaierId() {
        String c = System.getenv("CONTAINER_ID");
        return (c != null ? c : "unkown-container-id");
    }

    /**
     * @return the application name
     */
    public String getAppname() {
        return appName;
    }

    /**
     * @return the application name
     */
    public void setAppname(String appName) {
        this.appName = appName;
    }

    public List<ServerAddr> getKafkaBrokerHosts() {
        return kafkaBrokerHosts;
    }

    public void setKafkaBrokerHosts(List<ServerAddr> kafkaBrokerHosts) {
        this.kafkaBrokerHosts = kafkaBrokerHosts;
    }

    public void addKafkaBroker(ServerAddr server) {
        kafkaBrokerHosts.add(server);
    }

    public List<ServerAddr> getZookeeperHosts() {
        return zookeeperHosts;
    }

    public void setZookeeperHosts(List<ServerAddr> zookeeperHosts) {
        this.zookeeperHosts = zookeeperHosts;
    }

    public void addZookeerHost(ServerAddr server) {
        zookeeperHosts.add(server);
    }

    public Map<String, String> getCustomConfig() {
        return customConfig;
    }

    public void setCustomConfig(Map<String, String> customConfig) {
        this.customConfig = customConfig;
    }

    public void setConfig(String key, String value) {
        customConfig.put(key, value);
    }

    public String getConfig(String key) {
        return customConfig.get(key);
    }

    public boolean isConfig(String key) {
        return customConfig.containsKey(key);
    }

    public void setConfigBoolean(String key, boolean value) {
        customConfig.put(key, Boolean.toString(value));
    }

    public boolean getConfigBoolean(String key) {
        return Boolean.parseBoolean(customConfig.get(key));
    }

    public void setConfigInt(String key, int value) {
        customConfig.put(key, Integer.toString(value));
    }

    public int getConfigInt(String key) {
        return Integer.parseInt(customConfig.get(key));
    }

    public void setConfigDouble(String key, double value) {
        customConfig.put(key, Double.toString(value));
    }

    public double getConfigDouble(String key) {
        return Double.parseDouble(customConfig.get(key));
    }

    public String getTraceHost() {
        return traceHost;
    }

    public boolean isTracingEnabled() {
        return traceHost != null && !traceHost.equals("");
    }

    public void setTraceHost(String traceHost) {
        this.traceHost = traceHost;
    }

    public void setPSQLHost(String psqlHost) {
        this.psqlHostName = psqlHost;
    }

    public String getPSQLHost() {
        return this.psqlHostName;
    }

    public void setPSQLPort(int psqlPort) {
        this.psqlPort = psqlPort;
    }

    public int getPSQLPort() {
        return this.psqlPort;
    }

    public void setPSQLDatabase(String psqlDatabase) {
        this.psqlDatabase = psqlDatabase;
    }

    public String getPSQLDatabase() {
        return this.psqlDatabase;
    }

    public void setPSQLUsername(String psqlUsername) {
        this.psqlUsername = psqlUsername;
    }

    public String getPSQLUsername() {
        return this.psqlUsername;
    }

    public void setPSQLPassword(String psqlPassword) {
        this.psqlPassword = psqlPassword;
    }

    public String getPSQLPassword() {
        return this.psqlPassword;
    }

    public void setAppAsExternal() {
        this.isExternal = true;
    }

    public boolean isExternalApp() {
        return isExternal;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((appName == null) ? 0 : appName.hashCode());
        result = prime * result
                + ((bindAddress == null) ? 0 : bindAddress.hashCode());
        result = prime * result
                + ((customConfig == null) ? 0 : customConfig.hashCode());
        result = prime * result + databaseIndex;
        result = prime * result + (isExternal ? 1231 : 1237);
        result = prime
                * result
                + ((kafkaBrokerHosts == null) ? 0 : kafkaBrokerHosts.hashCode());
        result = prime * result + (redisCluster ? 1231 : 1237);
        result = prime
                * result
                + ((redisClusterHosts == null) ? 0 : redisClusterHosts
                                .hashCode());
        result = prime * result
                + ((redisHostName == null) ? 0 : redisHostName.hashCode());
        result = prime * result + redisPort;
        result = prime * result
                + ((zookeeperHosts == null) ? 0 : zookeeperHosts.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ExecutionConfiguration other = (ExecutionConfiguration) obj;
        if (appName == null) {
            if (other.appName != null)
                return false;
        } else if (!appName.equals(other.appName))
            return false;
        if (bindAddress == null) {
            if (other.bindAddress != null)
                return false;
        } else if (!bindAddress.equals(other.bindAddress))
            return false;
        if (customConfig == null) {
            if (other.customConfig != null)
                return false;
        } else if (!customConfig.equals(other.customConfig))
            return false;
        if (databaseIndex != other.databaseIndex)
            return false;
        if (isExternal != other.isExternal)
            return false;
        if (kafkaBrokerHosts == null) {
            if (other.kafkaBrokerHosts != null)
                return false;
        } else if (!kafkaBrokerHosts.equals(other.kafkaBrokerHosts))
            return false;
        if (redisCluster != other.redisCluster)
            return false;
        if (redisClusterHosts == null) {
            if (other.redisClusterHosts != null)
                return false;
        } else if (!redisClusterHosts.equals(other.redisClusterHosts))
            return false;
        if (redisHostName == null) {
            if (other.redisHostName != null)
                return false;
        } else if (!redisHostName.equals(other.redisHostName))
            return false;
        if (redisPort != other.redisPort)
            return false;
        if (zookeeperHosts == null) {
            if (other.zookeeperHosts != null)
                return false;
        } else if (!zookeeperHosts.equals(other.zookeeperHosts))
            return false;
        return true;
    }
}
