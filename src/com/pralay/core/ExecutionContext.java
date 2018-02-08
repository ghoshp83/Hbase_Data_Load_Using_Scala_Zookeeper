package com.pralay.core;

import com.pralay.core.configuration.ExecutionConfiguration;
import java.io.Closeable;

/**
 * Context for app core library and applications. The context holds the following shared state/objects:
 *
 *   - one ExecutionConfiguration
 *
 * Context is implemented as a singleton object defined by an ExecutionConfiguration object.
 * Context-creation attempts with differing configurations will throw an error.
 *
 */
public class ExecutionContext implements Closeable {

    // the singleton instance
    private static ExecutionContext instance;

    private volatile boolean isShutdown = false;

    private ExecutionConfiguration conf;
   
    /**
     * Ctor with custom configuration.
     * @param conf
     */
    private ExecutionContext(ExecutionConfiguration conf) {
        this.conf           = conf;
        
        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                instance.close();
            }
        });
    }

    /**
     * Static factory method for creating ExecutionContext with default configuration.
     * @return the global context object
     * @throws IllegalArgumentException if the context has already been initialized with a different configuration.
     */
    public static ExecutionContext create() {
        return ExecutionContext.create(new ExecutionConfiguration());
    }

    /**
     * Static factory method for creating ExecutionContext with custom configuration.
     *
     * @param conf custom ExecutionConfiguration
     * @return the global context object
     * @throws IllegalArgumentException if the context has already been initialized with a different configuration.
     */
    public static synchronized ExecutionContext create(ExecutionConfiguration conf) {
        if (ExecutionContext.instance == null) {
            ExecutionContext.instance = new ExecutionContext(conf);
        }
        else if (!ExecutionContext.instance.conf.equals(conf)) {
            // check if conf is consistent with already initialized context
            throw new IllegalArgumentException("ExecutionContext already exists with different configuration!");
        }
        return instance;
    }


    ///////////////////////////////////////////////////////////////
    //
    // GETTERS
    //
    ///////////////////////////////////////////////////////////////

    /**
     * Returns the shared ExecutionConfiguration object.
     * @return
     */
    public ExecutionConfiguration getConfiguration() {
        return conf;
    }

    ///////////////////////////////////////////////////////////////
    //
    // CLEANUP
    //
    ///////////////////////////////////////////////////////////////
    @Override
    public synchronized void close() {
        if (!isShutdown) {
           isShutdown = true;
        }
    }
}
