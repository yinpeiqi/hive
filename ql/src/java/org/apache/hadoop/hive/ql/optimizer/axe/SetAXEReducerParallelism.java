package org.apache.hadoop.hive.ql.optimizer.axe;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkUtils;
import org.apache.hadoop.hive.ql.parse.spark.OptimizeSparkProcContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.Stack;

public class SetAXEReducerParallelism implements NodeProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(SetAXEReducerParallelism.class.getName());

  private final boolean useOpStats;

  public SetAXEReducerParallelism(HiveConf conf) {
    useOpStats = conf.getBoolVar(HiveConf.ConfVars.SPARK_USE_OP_STATS);
  }

  @Override
  public Object process(Node nd, Stack<Node> stack,
      NodeProcessorCtx processorContext, Object... nodeOutputs)
      throws SemanticException {

    OptimizeSparkProcContext context = (OptimizeSparkProcContext) processorContext;

    ReduceSinkOperator sink = (ReduceSinkOperator) nd;
    ReduceSinkDesc desc = sink.getConf();
    Set<ReduceSinkOperator> parentSinks = null;

    int maxReducers = context.getConf().getIntVar(HiveConf.ConfVars.MAXREDUCERS);
    int constantReducers = context.getConf().getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS);

    if (!useOpStats) {
      parentSinks = OperatorUtils.findOperatorsUpstream(sink, ReduceSinkOperator.class);
      parentSinks.remove(sink);
      if (!context.getVisitedReduceSinks().containsAll(parentSinks)) {
        // We haven't processed all the parent sinks, and we need
        // them to be done in order to compute the parallelism for this sink.
        // In this case, skip. We should visit this again from another path.
        LOG.debug("Skipping sink " + sink + " for now as we haven't seen all its parents.");
        return false;
      }
    }

    if (context.getVisitedReduceSinks().contains(sink)) {
      // skip walking the children
      LOG.debug("Already processed reduce sink: " + sink.getName());
      return true;
    }
    context.getVisitedReduceSinks().add(sink);

    if (needSetParallelism(sink, context.getConf())) {
      if (constantReducers > 0) {
        LOG.info("Parallelism for reduce sink " + sink + " set by user to " + constantReducers);
        desc.setNumReducers(constantReducers);
      } else {
        //If it's a FileSink to bucketed files, use the bucket count as the reducer number
        FileSinkOperator fso = GenSparkUtils.getChildOperator(sink, FileSinkOperator.class);
        if (fso != null) {
          String bucketCount = fso.getConf().getTableInfo().getProperties().getProperty(
              hive_metastoreConstants.BUCKET_COUNT);
          int numBuckets = bucketCount == null ? 0 : Integer.parseInt(bucketCount);
          if (numBuckets > 0) {
            LOG.info("Set parallelism for reduce sink " + sink + " to: " + numBuckets + " (buckets)");
            desc.setNumReducers(numBuckets);
            return false;
          }
        }

        long numberOfBytes = 0;

        if (useOpStats) {
          // we need to add up all the estimates from the siblings of this reduce sink
          for (Operator<? extends OperatorDesc> sibling
              : sink.getChildOperators().get(0).getParentOperators()) {
            if (sibling.getStatistics() != null) {
              numberOfBytes += sibling.getStatistics().getDataSize();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Sibling " + sibling + " has stats: " + sibling.getStatistics());
              }
            } else {
              LOG.warn("No stats available from: " + sibling);
            }
          }
        } else if (parentSinks.isEmpty()) {
          // Not using OP stats and this is the first sink in the path, meaning that
          // we should use TS stats to infer parallelism
          for (Operator<? extends OperatorDesc> sibling
              : sink.getChildOperators().get(0).getParentOperators()) {
            Set<TableScanOperator> sources =
                OperatorUtils.findOperatorsUpstream(sibling, TableScanOperator.class);
            for (TableScanOperator source : sources) {
              if (source.getStatistics() != null) {
                numberOfBytes += source.getStatistics().getDataSize();
                if (LOG.isDebugEnabled()) {
                  LOG.debug("Table source " + source + " has stats: " + source.getStatistics());
                }
              } else {
                LOG.warn("No stats available from table source: " + source);
              }
            }
          }
          LOG.debug("Gathered stats for sink " + sink + ". Total size is "
                        + numberOfBytes + " bytes.");
        } else {
          // Use the maximum parallelism from all parent reduce sinks
          int numberOfReducers = 0;
          for (ReduceSinkOperator parent : parentSinks) {
            numberOfReducers = Math.max(numberOfReducers, parent.getConf().getNumReducers());
          }
          desc.setNumReducers(numberOfReducers);
          LOG.debug("Set parallelism for sink " + sink + " to " + numberOfReducers
                        + " based on its parents");
          return false;
        }

        // Divide it by 2 so that we can have more reducers
        long bytesPerReducer = context.getConf().getLongVar(HiveConf.ConfVars.BYTESPERREDUCER) / 2;
        int numReducers = Utilities.estimateReducers(numberOfBytes, bytesPerReducer,
                                                     maxReducers, false);

        numReducers = Math.min(numReducers, maxReducers);
        LOG.info("Set parallelism for reduce sink " + sink + " to: " + numReducers + " (calculated)");
        desc.setNumReducers(numReducers);
      }
    } else {
      LOG.info("Number of reducers for sink " + sink + " was already determined to be: " + desc.getNumReducers());
    }

    return false;
  }

  // tests whether the RS needs automatic setting parallelism
  private boolean needSetParallelism(ReduceSinkOperator reduceSink, HiveConf hiveConf) {
    ReduceSinkDesc desc = reduceSink.getConf();
    if (desc.getNumReducers() <= 0) {
      return true;
    }
    if (desc.getNumReducers() == 1 && desc.hasOrderBy() &&
        hiveConf.getBoolVar(HiveConf.ConfVars.HIVESAMPLINGFORORDERBY) && !desc.isDeduplicated()) {
      List<Operator<? extends OperatorDesc>> children = reduceSink.getChildOperators();
      while (children != null && children.size() > 0) {
        if (children.size() != 1 || children.get(0) instanceof LimitOperator) {
          return false;
        }
        if (children.get(0) instanceof ReduceSinkOperator ||
            children.get(0) instanceof FileSinkOperator) {
          break;
        }
        children = children.get(0).getChildOperators();
      }
      return true;
    }
    return false;
  }

}
