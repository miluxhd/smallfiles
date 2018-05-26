package ir.milux.smallfile.client;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.apache.log4j.Logger;

/**
 * Created by miluz on 5/19/18.
 */
public class FinalBatchStatement {
    static final Logger logger = Logger.getRootLogger();

    private int maxBatchSize;
    private BatchStatement batchStatement = null;

    private int dynamicBatchSize;

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    FinalBatchStatement(int maxBatchSize, int initialBatchSize) {
        this.maxBatchSize = maxBatchSize;
        this.dynamicBatchSize = initialBatchSize;
        this.batchStatement = new BatchStatement();
        this.batchStatement.setConsistencyLevel(ConsistencyLevel.ONE);
    }

    public synchronized void reset() {
        this.dynamicBatchSize=0;
        batchStatement = new BatchStatement();
    }

    public synchronized void add(Statement statement) {
        batchStatement.add(statement);
        this.dynamicBatchSize++;
    }

    public synchronized BatchStatement getStatement() {
        return batchStatement;
    }

    public int getBatchSize() {
        return this.dynamicBatchSize;
    }

    public void increaseBatchSize(int step) {
        this.dynamicBatchSize = this.dynamicBatchSize + step;
    }

    public void executeSequential(Session session) {
        for (Statement s : batchStatement.getStatements()
                ) {
            try {
                session.execute(s);
                logger.info("executing single statement");
            } catch (Exception ee) {
                ee.printStackTrace();
            }
        }
    }
}