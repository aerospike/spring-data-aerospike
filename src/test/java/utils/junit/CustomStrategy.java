package utils.junit;

import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.support.hierarchical.ParallelExecutionConfiguration;
import org.junit.platform.engine.support.hierarchical.ParallelExecutionConfigurationStrategy;

public class CustomStrategy implements ParallelExecutionConfiguration, ParallelExecutionConfigurationStrategy {

    private int getParallelNumber() {
        String parallelNumber = "1";
        if (System.getProperty("junit.jupiter.execution.parallel.config.fixed.parallelism") != null)
            parallelNumber = System.getProperty("junit.jupiter.execution.parallel.config.fixed.parallelism");
        return Integer.parseInt(parallelNumber);
    }

    @Override
    public int getParallelism() {
        return getParallelNumber();
    }

    @Override
    public int getMinimumRunnable() {
        return getParallelNumber();
    }

    @Override
    public int getMaxPoolSize() {
        return getParallelNumber();
    }

    @Override
    public int getCorePoolSize() {
        return getParallelNumber();
    }

    @Override
    public int getKeepAliveSeconds() {
        return 30;
    }

    @Override
    public ParallelExecutionConfiguration createConfiguration(final ConfigurationParameters configurationParameters) {
        return this;
    }
}
