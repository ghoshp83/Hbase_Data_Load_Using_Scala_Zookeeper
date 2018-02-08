package com.pralay.HbaseFullLoad;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.pralay.core.ExecutionContext;
import com.pralay.core.configuration.ServerAddr;

public final class ZookeeperManagerSingleton {
    private static final Logger LOGGER = Logger.getLogger(ZookeeperManagerSingleton.class);
    private ZooKeeper zooKeeper = null;
    private String zkMonitoringFolder;
    private AddedChildrenListener addedChildrenListener;
    private String connectString;

    public static String getZookeeperConnectionString() {
        String connectString = "localhost:5181"; // default
        try {
            ExecutionContext context = ExecutionContext.create();
            List<ServerAddr> zookeeperList = context.getConfiguration().getZookeeperHosts();
            if (zookeeperList != null && !zookeeperList.isEmpty()) {
                StringBuffer stringBuffer = new StringBuffer();
                for (ServerAddr serverAddr : zookeeperList) {
                    if (stringBuffer.length() > 0)
                        stringBuffer.append(",");
                    stringBuffer.append(serverAddr);
                }
                connectString = stringBuffer.toString();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to get Zookeeper connection info from application", e);
            throw new FatalException(e);
        }
        LOGGER.info("Zookeeper connection string:" + connectString);
        return connectString;
    }

    public ZookeeperManagerSingleton() {
    }

    private static class ZookeeperManagerSingletonHolder {
        private static final ZookeeperManagerSingleton INSTANCE = new ZookeeperManagerSingleton();
    }

    public static ZookeeperManagerSingleton getInstance() {
        ZookeeperManagerSingleton instance = ZookeeperManagerSingletonHolder.INSTANCE;
        return instance;
    }

    public void init() {
        Configuration configuration = Configuration.getInstance();
        ZookeeperManagerSingleton instance = getInstance();
        instance.connectString = ZookeeperManagerSingleton.getZookeeperConnectionString();
        instance.zkMonitoringFolder = configuration.getValue("zkMonitoringFolder") + "/"
                + configuration.getValue("executionMode");
        instance.addedChildrenListener = null;
        instance.createNewSession();
    }

    private void checkZooKeeper() {
        if (zooKeeper == null) {
            createNewSession();
        }
    }

    public interface DataMonitorListener {
        void closing(Throwable exc);

        void trigger(List<String> newValues);
    }

    private void createNewSession() {
        LOGGER.info("createNewSession() called...");
        try {
            if (zooKeeper != null) {
                if (zooKeeper.getState().isAlive()) {
                    LOGGER.info("Zookeeper is alive...");
                    if (addedChildrenListener != null) {
                        addedChildrenListener.startWatcher();
                    }
                    return;
                }
                LOGGER.info("Closing Zookeeper session...");
                zooKeeper.close();// no problem if called more than once
            }
            Watcher watcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    try {
                        if (event.getType() == Watcher.Event.EventType.None) {
                            // We are are being told that the state of the
                            // connection has changed
                            switch (event.getState()) {
                            case Expired:
                                LOGGER.info("Main watcher received 'Expired'...");
                                // this connection expired - create new
                                // connection to Zookeeper
                                // it is to be done here, because we are not
                                // sure,
                                // that the latest getChildren() successfully
                                // set the watcher in Zookeeper
                                createNewSession();
                                break;
                            case SyncConnected:
                                LOGGER.info("Main watcher received 'SyncConnected'..."); // restart
                                                                                         // watcher
                                                                                         // if
                                                                                         // connected/reconnected
                                                                                         // to
                                                                                         // Zookeeper
                                                                                         // server
                                // it is to be done at reconnection, because
                                // might be, that the latest normal start of the
                                // watcher was unsuccessful
                                // (no problem if the same watcher registered
                                // multiple times, it will be notified only once
                                // by the Zookeeper server)
                                if (addedChildrenListener != null) {
                                    addedChildrenListener.startWatcher();
                                }
                                break;
                            }
                        }
                    } catch (Throwable th) {
                        Executor.getInstance().closing(th);
                    }
                }
            };
            LOGGER.info("Creating new Zookeeper session...");
            zooKeeper = createZooKeeper(connectString, 3000, watcher);
        } catch (IOException | InterruptedException ex) {
            LOGGER.info("Problem while creating new Zookeeper session...");
            throw new RetryableException(ex);
        }
    }

    public static ZooKeeper createZooKeeper(String connectString, int sessionTimeout, Watcher watcher)
            throws IOException {
        return new ZooKeeper(connectString, sessionTimeout, watcher);
    }

    private boolean isRetryable(KeeperException ke) {
        return isRetryable(ke.code());
    }

    private boolean isRetryable(KeeperException.Code code) {
        return code == KeeperException.Code.SESSIONEXPIRED || code == KeeperException.Code.CONNECTIONLOSS
                || code == KeeperException.Code.NONODE || code == KeeperException.Code.OPERATIONTIMEOUT
                || code == KeeperException.Code.NODEEXISTS || code == KeeperException.Code.NOTEMPTY;
    }

    public Stat exists(String processedZnodePath) {
        Stat stat = null;
        int repeatCounter = Executor.ZOOKEPER_RETRY;
        while (true) {
            try {
                stat = zooKeeper.exists(processedZnodePath, false);
                return stat;
            } catch (KeeperException ke) {
                repeatCounter--;
                if (repeatCounter < 0) {
                    throw new RetryableException(ke);
                }
                if (ke.code() == KeeperException.Code.CONNECTIONLOSS) {
                    // try again
                } else if (ke.code() == KeeperException.Code.SESSIONEXPIRED) {
                    createNewSession();
                    // try again
                } else {
                    throw new RetryableException(ke);
                }
                LOGGER.info("Retrying zookeeper.exists " + processedZnodePath + " because of KeeperException "
                        + ke.code().name());
            } catch (InterruptedException ex) {
                throw new RetryableException(ex);
            }
        }
    }

    public void create(String processedZnodePath, byte[] bytes, ArrayList<ACL> openAclUnsafe, CreateMode persistent) {
        String codeName;
        int repeatCounter = Executor.ZOOKEPER_RETRY;
        while (true) {
            try {
                zooKeeper.create(processedZnodePath, bytes, openAclUnsafe, persistent);
                return;
            } catch (KeeperException ke) {
                repeatCounter--;
                if (repeatCounter < 0) {
                    throw new RetryableException(ke);
                }
                if (ke.code() == KeeperException.Code.CONNECTIONLOSS) {
                    // try again
                } else if (ke.code() == KeeperException.Code.SESSIONEXPIRED) {
                    createNewSession();
                    // try again
                } else {
                    throw new RetryableException(ke);
                }
                codeName = ke.code().name();
            } catch (InterruptedException ex) {
                throw new RetryableException(ex);
            }
            if (exists(processedZnodePath) != null) {
                return;
            } else {
                LOGGER.info(
                        "Retrying zookeeper.create " + processedZnodePath + " because of KeeperException " + codeName);
            }
        }
    }

    public byte[] getData(String znodeProcessedFolder, boolean b, Stat stat) {
        byte[] result = null;
        int repeatCounter = Executor.ZOOKEPER_RETRY;
        while (true) {
            try {
                result = zooKeeper.getData(znodeProcessedFolder, b, stat);
                return result;
            } catch (KeeperException ke) {
                repeatCounter--;
                if (repeatCounter < 0) {
                    throw new RetryableException(ke);
                }
                if (ke.code() == KeeperException.Code.CONNECTIONLOSS) {
                    // try again
                } else if (ke.code() == KeeperException.Code.SESSIONEXPIRED) {
                    createNewSession();
                    // try again
                } else {
                    throw new RetryableException(ke);
                }
                LOGGER.info("Retrying zookeeper.getData " + znodeProcessedFolder + " because of KeeperException "
                        + ke.code().name());
            } catch (InterruptedException ex) {
                throw new RetryableException(ex);
            }
        }
    }

    private void delete(String path, int i) {
        String codeName;
        int repeatCounter = Executor.ZOOKEPER_RETRY;
        while (true) {
            try {
                zooKeeper.delete(path, i);
                return;
            } catch (KeeperException ke) {
                repeatCounter--;
                if (repeatCounter < 0) {
                    throw new RetryableException(ke);
                }
                if (ke.code() == KeeperException.Code.CONNECTIONLOSS) {
                    // try again
                } else if (ke.code() == KeeperException.Code.SESSIONEXPIRED) {
                    createNewSession();
                    // try again
                } else {
                    throw new RetryableException(ke);
                }
                codeName = ke.code().name();
            } catch (InterruptedException ex) {
                throw new RetryableException(ex);
            }
            if (exists(path) == null) {
                return;
            } else {
                LOGGER.info("Retrying zookeeper.delete " + path + " because of KeeperException " + codeName);
            }
            repeatCounter--;
        }
    }

    /*
     * From Monitor class
     */
    public void writeMonitoringParam(String paramName, String paramValue) {
        String path = zkMonitoringFolder + "/" + paramName;
        // Check path existence and delete if exists
        if (exists(path) != null) {
            delete(path, -1);
        }
        // Create znode
        create(path, paramValue.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        LOGGER.info("MONITORING: " + path + ": " + paramValue);
    }

    private void getChildren(String watchedZNode, Watcher watcher,
            AsyncCallback.Children2Callback addedChildrenListener, Object o) {
        zooKeeper.getChildren(watchedZNode, watcher, addedChildrenListener, o);
    }

    List<String> getChildren(String nodePath, boolean watcher) {
        List<String> children = null;
        int repeatCounter = Executor.ZOOKEPER_RETRY;
        while (true) {
            try {
                children = zooKeeper.getChildren(nodePath, watcher);
                Collections.sort(children);
                return children;
            } catch (KeeperException ke) {
                repeatCounter--;
                if (repeatCounter < 0) {
                    throw new RetryableException(ke);
                }
                if (ke.code() == KeeperException.Code.CONNECTIONLOSS) {
                    // try again
                } else if (ke.code() == KeeperException.Code.SESSIONEXPIRED) {
                    createNewSession();
                    // try again
                } else {
                    throw new RetryableException(ke);
                }
                LOGGER.info("Retrying zookeeper.getChildren " + nodePath + " because of KeeperException "
                        + ke.code().name());
            } catch (InterruptedException ex) {
                throw new RetryableException(ex);
            }
        }
    }

    public void setAddedChildrenListener(String watchedZNode, DataMonitorListener listener, String processedZnode) {
        this.addedChildrenListener = new AddedChildrenListener(watchedZNode, listener, processedZnode);
        this.addedChildrenListener.startWatcher();
    }

    // Mostly for test purposes
    public AddedChildrenListener getAddedChildrenListener() {
        return addedChildrenListener;
    }

    public class AddedChildrenListener implements Watcher, AsyncCallback.Children2Callback {
        private final Logger LOGGER = Logger.getLogger(AddedChildrenListener.class);
        /*
         * From ZkListener class
         */
        private String watchedZNode;
        private DataMonitorListener listener;
        private List<String> currentChildren;
        private String processedZnode;

        public AddedChildrenListener(String watchedZNode, ZookeeperManagerSingleton.DataMonitorListener listener,
                String processedZnode) {
            this.watchedZNode = watchedZNode;
            this.listener = listener;
            LOGGER.info("ChildrenListener created...");
            this.currentChildren = new ArrayList<String>();
            this.processedZnode = processedZnode;
        }

        public void startWatcher() {
            LOGGER.info("children watcher set for " + watchedZNode);
            getInstance().getChildren(watchedZNode, this, this, null);
        }

        // Implement Watcher interface
        // Watcher callback for getChildren (children list changed)
        @Override
        public void process(WatchedEvent event) {
            LOGGER.info("children watcher fired");
            try {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    // set the watcher again; the children list will be handled
                    // in asynchronous callback -> processResult
                    startWatcher();
                }
            } catch (Throwable th) {
                Executor.getInstance().closing(th);
            }
        }

        // implement AsyncCallback.Children2Callback interface
        // Result callback function for getChildren (current children list)
        @Override
        public void processResult(int rootCode, String path, Object ctx, List<String> children, Stat stat) {
            try {
                processResult_withExceptions(rootCode, path, ctx, children, stat);
            } catch (Throwable exc) {
                listener.closing(exc);
            }
        }

        public synchronized void processResult_withExceptions(int rootCode, String path, Object ctx,
                List<String> children, Stat stat) {
            LOGGER.info("processResult(rootCode: " + rootCode + ", path: " + path + ")");
            KeeperException.Code code = KeeperException.Code.get(rootCode);
            switch (code) {
            case OK:
                LOGGER.info("processResult(rootCode: " + rootCode + ", path: " + path + ")");
                handleChildrenList(children);
                break;
            case CONNECTIONLOSS:
                // will be handled in Zookeeper objects watcher
                // // restart getting children as connection lost to one of
                // Zookeeper hosts
                // startWatcher();
                break;
            case SESSIONEXPIRED:
                // will be handled in Zookeeper objects watcher
                // createNewSession();
                // // restart getting children as session expired
                // startWatcher();
                break;
            default:
                if (code != KeeperException.Code.OK) {
                    KeeperException ke = KeeperException.create(code, watchedZNode);
                    throw new RetryableException(ke);
                }
                break;
            }
        }

        private void handleChildrenList(List<String> notProcessedChildren) {
            while (!notProcessedChildren.isEmpty()) {
                // List of folder paths of dirs to load into HBase
                List<String> newValues = new ArrayList<String>();
                // drop already processed children
                LOGGER.info("Getting children znodes of Processed, path: " + processedZnode);
                if (ZookeeperManagerSingleton.getInstance().exists(processedZnode) != null) {
                    List<String> processedChildren = getChildren(processedZnode, false);
                    if (processedChildren.size() > 0) {
                        notProcessedChildren.removeAll(processedChildren);
                        LOGGER.info("The following timestamps need to be processed:" + notProcessedChildren);
                        if (notProcessedChildren.isEmpty()) {
                            LOGGER.info("Not processed node list is empty - completed processing.");
                            break;
                        }
                        LOGGER.info("Already processed nodes removed from ready list");
                    } else {
                        LOGGER.info("Already Processed nodes not found");
                    }
                } else {
                    LOGGER.info("Processed parent znode not found");
                }
                // drop already processed children
                LOGGER.info("Getting children znodes of Processed");
                if (ZookeeperManagerSingleton.getInstance().exists(processedZnode) != null) {
                    List<String> processedChildren = getChildren(processedZnode, false);
                    if (processedChildren.size() > 0) {
                        notProcessedChildren.removeAll(processedChildren);
                        LOGGER.info("Already processed nodes removed from ready list");
                    } else {
                        LOGGER.info("Already Processed nodes not found");
                    }
                } else {
                    LOGGER.info("Processed parent znode not found");
                }
                // For each child of znode
                for (String child : notProcessedChildren) {
                    LOGGER.info("Start processing child: " + child);
                    // Get the value of the new child (folder path)
                    String znode = this.watchedZNode + "/" + child;
                    byte[] value = getInstance().getData(znode, false, null);
                    String valueStr = new String(value);
                    LOGGER.info("New unprocessed value of " + znode + ": " + valueStr);
                    // Put the new path into the list of new values
                    newValues.add(valueStr);
                }
                // If there are new values, pass it to the trigger
                if (!newValues.isEmpty()) {
                    // Sort the list
                    Collections.sort(newValues);
                    // Pass it
                    listener.trigger(newValues);
                }
                if (ZookeeperManagerSingleton.getInstance().exists(watchedZNode) != null) {
                    notProcessedChildren = getChildren(watchedZNode, false);
                } else {
                    LOGGER.error("Ready parent znode not found");
                    return;
                }
            }
        }
    }
    /*
     * From InitTasks
     */

    // Creates an Zookeeper path if it doesn't exist
    public void createZookeeperPath(String zkPath) {
        // Check whether path exists
        if (exists(zkPath) == null) {
            // Truncate startWatcher and end /
            if (zkPath.endsWith("/"))
                zkPath = zkPath.substring(0, zkPath.length() - 1);
            if (zkPath.startsWith("/"))
                zkPath = zkPath.substring(1, zkPath.length());
            // Split path by / and create an array from path members
            String pathArray[] = zkPath.split("/");
            String currentPath = "";
            // Iterate through the array and check whether the partial path
            // exists and if not, create it
            for (int i = 0; i < pathArray.length; i++) {
                currentPath = currentPath + "/" + pathArray[i];
                if (exists(currentPath) == null) {
                    // Create path if it doesn't exist
                    create(currentPath, toBytes(1), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }
        }
    }
}
