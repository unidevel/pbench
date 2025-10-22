#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const readline = require('readline');

/**
 * Converts an event listener stats JSON file to a query stats JSON file
 *
 * Based on the schema defined in:
 * - el.schema (Event Listener schema)
 * - presto/query_json/query_info.go (QueryInfo structure)
 * - presto/query_json/stage_info.go (StageInfo structure)
 * - presto/query_json/operator_summary.go (OperatorSummary structure)
 */
function convertEventListenerToQueryInfo(elJson) {
  // Create the base QueryInfo structure
  const queryInfo = {
    queryId: elJson.queryCompletedEvent?.metadata?.queryId || '',
    self: elJson.queryCompletedEvent?.metadata?.uri || '',
    query: elJson.queryCompletedEvent?.metadata?.query || '',
    queryType: elJson.queryCompletedEvent?.queryType || '',
    state: elJson.queryCompletedEvent?.metadata?.queryState || '',
    failureInfo: elJson.queryCompletedEvent?.failureInfo || null,
    errorCode: createErrorCodeObject(elJson),
    warnings: elJson.queryCompletedEvent?.warnings || null,
    resourceGroupId: elJson.queryCompletedEvent?.context?.resourceGroupId || null,
    session: createSessionObject(elJson),
    queryStats: createQueryStatsObject(elJson),
    outputStage: createOutputStageObject(elJson)
  };

  return queryInfo;
}

/**
 * Creates an ErrorCode object from event listener data
 */
function createErrorCodeObject(elJson) {
  return elJson.queryCompletedEvent?.failureInfo?.errorCode
}

/**
 * Creates a Session object from event listener data
 */
function createSessionObject(elJson) {
  const context = elJson.queryCompletedEvent?.context || {};

  // Convert session properties to the expected format
  const systemProperties = {};
  const catalogProperties = {};

  if (context.sessionProperties) {
    Object.entries(context.sessionProperties).forEach(([key, value]) => {
      if (key.includes('.')) {
        // This is a catalog property (e.g., "hive.bucket_execution_enabled")
        const [catalog, propName] = key.split('.', 2);
        if (!catalogProperties[catalog]) {
          catalogProperties[catalog] = {};
        }
        catalogProperties[catalog][propName] = value.toString();
      } else {
        // This is a system property
        systemProperties[key] = value.toString();
      }
    });
  }

  return {
    transactionId: context.transactionId || null,
    schema: context.schema || null,
    catalog: context.catalog || null,
    systemProperties: systemProperties,
    catalogProperties: catalogProperties,
    user: context.user || null,
    principal: context.principal || null,
    remoteUserAddress: context.remoteClientAddress || null,
    source: context.source || null,
    resourceEstimates: context.resourceEstimates,
    userAgent: context.userAgent || null,
    clientTags: context.clientTags,
    // SessionPropertiesJson will be generated when needed
  };
}

/**
 * Converts a Unix timestamp in seconds to a JavaScript Date object
 * @param {number} timestamp - Unix timestamp in seconds
 * @return {Date|null} - JavaScript Date object or null if timestamp is falsy
 */
function convertTimestampToDate(timestamp) {
  return timestamp ? new Date(timestamp * 1000) : null;
}

/**
 * Creates a QueryStats object from event listener data
 */
function createQueryStatsObject(elJson) {
  const stats = elJson.queryCompletedEvent?.statistics || {};
  const timestamps = {
    createTime: convertTimestampToDate(elJson.queryCompletedEvent?.createTime),
    executionStartTime: convertTimestampToDate(elJson.queryCompletedEvent?.executionStartTime),
    endTime: convertTimestampToDate(elJson.queryCompletedEvent?.endTime)
  };

  // Calculate derived metrics
  let bytesPerCPUSec = 0;
  let rowsPerCPUSec = 0;
  let bytesPerSec = 0;

  const cpuTimeMs = stats.cpuTime || 0;
  if (cpuTimeMs > 0) {
    bytesPerCPUSec = Math.floor((stats.totalBytes || 0) / (cpuTimeMs / 1000));
    rowsPerCPUSec = Math.floor((stats.totalRows || 0) / (cpuTimeMs / 1000));
  }

  const executionTimeMs = stats.executionTime || 0;
  if (executionTimeMs > 0) {
    bytesPerSec = Math.floor((stats.totalBytes || 0) / (executionTimeMs / 1000));
  }

  return {
    createTime: timestamps.createTime,
    endTime: timestamps.endTime,
    executionStartTime: timestamps.executionStartTime,
    analysisTime: stats.analysisTime || 0,
    queuedTime: stats.queuedTime || 0,
    totalPlanningTime: stats.planningTime || 0,
    elapsedTime: stats.wallTime || 0,
    executionTime: stats.executionTime || 0,
    totalCpuTime: stats.cpuTime || 0,
    rawInputPositions: stats.totalRows || 0,
    rawInputDataSize: stats.totalBytes || 0,
    outputPositions: stats.outputRows || 0,
    outputDataSize: stats.outputBytes || 0,
    writtenOutputPositions: stats.writtenOutputRows || 0,
    writtenOutputDataSize: stats.writtenOutputBytes || 0,
    cumulativeUserMemory: stats.cumulativeMemory || 0,
    cumulativeTotalMemory: stats.cumulativeTotalMemory || 0,
    peakUserMemoryReservation: stats.peakUserMemoryBytes || 0,
    peakTotalMemoryReservation: stats.peakTotalNonRevocableMemoryBytes || 0,
    peakTaskUserMemory: stats.peakTaskUserMemory || 0,
    peakTaskTotalMemory: stats.peakTaskTotalMemory || 0,
    writtenIntermediatePhysicalDataSize: stats.writtenIntermediateBytes || 0,
    peakNodeTotalMemory: stats.peakNodeTotalMemory || 0,
    totalDrivers: stats.completedSplits || 0,
    stageGcStatistics: createStageGcStatistics(elJson),
    operatorSummaries: createOperatorSummaries(elJson),

    // Calculated fields
    bytesPerCPUSec: bytesPerCPUSec,
    rowsPerCPUSec: rowsPerCPUSec,
    bytesPerSec: bytesPerSec,
    stageCount: (elJson.queryCompletedEvent?.stageStatistics || []).length
  };
}

/**
 * Creates stage GC statistics from event listener data
 */
function createStageGcStatistics(elJson) {
  const stageStats = elJson.queryCompletedEvent?.stageStatistics || [];
  return stageStats.map(stage => {
    if (!stage.gcStatistics) return null;

    // Convert to RawMessage format
    return JSON.parse(JSON.stringify(stage.gcStatistics));
  }).filter(Boolean);
}

/**
 * Creates operator summaries from event listener data
 */
function createOperatorSummaries(elJson) {
  const operatorStats = elJson.queryCompletedEvent?.operatorStatistics || [];

  return operatorStats.map(op => {
    let stageId = op.stageId;

    return {
      stageId: stageId,
      stageExecutionId: op.stageExecutionId,
      pipelineId: op.pipelineId,
      operatorId: op.operatorId,
      planNodeId: op.planNodeId || '',
      operatorType: op.operatorType || '',
      totalDrivers: op.totalDrivers || 0,
      addInputCalls: op.addInputCalls || 0,
      addInputWall: op.addInputWall || 0,
      addInputCpu: op.addInputCpu || 0,
      addInputAllocation: op.addInputAllocation || 0,
      rawInputDataSize: op.rawInputDataSize || 0,
      rawInputPositions: op.rawInputPositions || 0,
      inputDataSize: op.inputDataSize || 0,
      inputPositions: op.inputPositions || 0,
      sumSquaredInputPositions: op.sumSquaredInputPositions || 0,
      getOutputCalls: op.getOutputCalls || 0,
      getOutputWall: op.getOutputWall || 0,
      getOutputCpu: op.getOutputCpu || 0,
      getOutputAllocation: op.getOutputAllocation || 0,
      outputDataSize: op.outputDataSize || 0,
      outputPositions: op.outputPositions || 0,
      physicalWrittenDataSize: op.physicalWrittenDataSize || 0,
      blockedWall: op.blockedWall || 0,
      finishCalls: op.finishCalls || 0,
      finishWall: op.finishWall || 0,
      finishCpu: op.finishCpu || 0,
      finishAllocation: op.finishAllocation || 0,
      userMemoryReservation: op.userMemoryReservation || 0,
      revocableMemoryReservation: op.revocableMemoryReservation || 0,
      systemMemoryReservation: op.systemMemoryReservation || 0,
      peakUserMemoryReservation: op.peakUserMemoryReservation || 0,
      peakSystemMemoryReservation: op.peakSystemMemoryReservation || 0,
      peakTotalMemoryReservation: op.peakTotalMemoryReservation || 0,
      spilledDataSize: op.spilledDataSize || 0,
      info: op.info,
      // info: op.info ? JSON.stringify(op.info) : null,
      runtimeStats: op.runtimeStats
    };
  });
}

/**
 * Creates an OutputStage object from event listener data
 */
function createOutputStageObject(elJson) {
  const stageGcStatistics = createStageGcStatistics(elJson);
  const gcInfoById = stageGcStatistics.reduce((acc, gcInfo) => {
    acc[gcInfo.stageId] = gcInfo;
    return acc;
  }, {});
  const stageStats = elJson.queryCompletedEvent?.stageStatistics || [];
  if (stageStats.length === 0) return null;

  // Build a map of stages by ID
  const stagesById = {};
  let plans = JSON.parse(elJson.queryCompletedEvent?.metadata?.jsonPlan || "{}");
  let rootStageId = null;
  stageStats.forEach((stage,index) => {
    let stageId =  stage.stageId?.toString();

    // Create StageGcInfo with stageExecutionId
    const gcInfo = {
      ...(gcInfoById[stageId] || {})
    };

    if (index === 0) {
      rootStageId = stageId;
    }

    stagesById[stageId] = {
      stageId: stageId,
      latestAttemptExecutionInfo: {
        state: "FINISHED", // Assuming completed stages
        stats: {
          totalTasks: stage.tasks || 0,
          totalScheduledTime: stage.totalScheduledTime || 0,
          totalCpuTime: stage.totalCpuTime || 0,
          retriedCpuTime: stage.retriedCpuTime || 0,
          totalBlockedTime: stage.totalBlockedTime || 0,
          rawInputDataSize: stage.rawInputDataSize || 0,
          processedInputDataSize: stage.processedInputDataSize || 0,
          physicalWrittenDataSize: stage.physicalWrittenDataSize || 0,
          gcInfo: gcInfo
        }
      },
      plan: {
        jsonRepresentation: JSON.stringify(plans[stageId]?.plan),
      },
      subStages: [],
      stageExecutionId: gcInfo.stageExecutionId,
    };
  });

  let rootStage = stagesById[rootStageId];
  if (rootStage) {
    Object.entries(stagesById).forEach(([stageId, stage]) => {
      if (stageId != rootStageId) {
        rootStage.subStages.push(stage);
      }
    });

    return rootStage
  }

  return null;
}

/**
 * Prepares the QueryInfo object for output, similar to PrepareForInsert in Go
 */
function prepareForOutput(queryInfo) {
  return queryInfo;
}

/**
 * Process a single event listener JSON object and convert it to query info format
 */
async function processJsonObject(elJson) {
  try {
    // Check if this is an event listener JSON
    if (!elJson.queryCompletedEvent?.metadata?.queryId) {
      return {
        success: false,
        error: 'Not an event listener JSON file (missing queryId)',
        isEventListener: false
      };
    }

    // Convert the data
    let queryInfo = convertEventListenerToQueryInfo(elJson);

    // Prepare for output (similar to PrepareForInsert in Go)
    queryInfo = prepareForOutput(queryInfo);

    return { success: true, queryInfo };
  } catch (error) {
    return { success: false, error: error.message, isEventListener: true };
  }
}

/**
 * Try to read the first line of a file to check if it's NDJSON
 */
async function tryReadFirstLine(filePath) {
  return new Promise((resolve) => {
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity
    });

    let firstLine = null;

    rl.on('line', (line) => {
      if (line.trim()) {
        firstLine = line;
        rl.close();
      }
    });

    rl.on('close', () => {
      resolve(firstLine);
    });

    rl.on('error', () => {
      resolve(null);
    });
  });
}

/**
 * Process a single file and save the result to the target directory
 */
async function processFile(inputFile, targetDir) {
  try {
    // First try to read the first line to check if it's NDJSON
    const firstLine = await tryReadFirstLine(inputFile);

    if (firstLine) {
      try {
        // Try to parse the first line as JSON
        const elJson = JSON.parse(firstLine);

        // Check if it's an event listener JSON
        const result = await processJsonObject(elJson);

        if (result.success) {
          // It's a valid event listener JSON, process as NDJSON
          const results = await processFileAsNdjson(inputFile, targetDir);
          if (results.length > 0) {
            console.log(`Successfully processed ${inputFile} as NDJSON with ${results.length} records`);
            return { success: true, file: inputFile, count: results.length };
          }
        } else if (!result.isEventListener) {
          console.warn(`Warning: ${inputFile} is not an event listener JSON file (missing queryId)`);
          return { success: false, file: inputFile, error: 'Not an event listener JSON file' };
        }
      } catch (ndjsonError) {
        // First line is not valid JSON, try as regular JSON
      }
    }

    // Try to process as regular JSON
    try {
      const inputData = fs.readFileSync(inputFile, 'utf8');
      const elJson = JSON.parse(inputData);

      // Check if it's an event listener JSON
      const result = await processJsonObject(elJson);

      if (!result.success) {
        if (!result.isEventListener) {
          console.warn(`Warning: ${inputFile} is not an event listener JSON file (missing queryId)`);
          return { success: false, file: inputFile, error: 'Not an event listener JSON file' };
        }
        throw new Error(result.error);
      }

      // Get queryId for the output filename
      const queryId = result.queryInfo.queryId;
      const outputFile = path.join(targetDir, `${queryId}.json`);

      // Create output stream and write the data
      const outputStream = fs.createWriteStream(outputFile);
      outputStream.write(JSON.stringify(result.queryInfo, null, 2));
      outputStream.end();

      // Wait for the stream to finish
      await new Promise((resolve) => {
        outputStream.on('finish', resolve);
      });

      console.log(`Successfully converted ${inputFile} to ${outputFile}`);
      return { success: true, file: inputFile, queryId };
    } catch (jsonError) {
      // Both NDJSON and JSON parsing failed
      const errorMsg = `Failed to process ${inputFile}: ${jsonError.message}`;
      console.error(errorMsg);
      process.stderr.write(`${errorMsg}\n`);
      return { success: false, file: inputFile, error: jsonError.message };
    }
  } catch (error) {
    const errorMsg = `Error processing ${inputFile}: ${error.message}`;
    console.error(errorMsg);
    process.stderr.write(`${errorMsg}\n`);
    return { success: false, file: inputFile, error: error.message };
  }
}

/**
 * Process a file as NDJSON and save each record to the target directory
 */
async function processFileAsNdjson(inputFile, targetDir) {
  return new Promise((resolve, reject) => {
    const rl = readline.createInterface({
      input: fs.createReadStream(inputFile),
      crlfDelay: Infinity
    });

    const results = [];
    let lineCount = 0;

    rl.on('line', async (line) => {
      if (!line.trim()) return; // Skip empty lines

      lineCount++;
      try {
        const elJson = JSON.parse(line);

        // Check if it's an event listener JSON
        const result = await processJsonObject(elJson);

        if (!result.success) {
          if (!result.isEventListener) {
            console.warn(`Warning: Line ${lineCount} in ${inputFile} is not an event listener JSON (missing queryId)`);
            return;
          }
          console.error(`Error processing line ${lineCount} from ${inputFile}: ${result.error}`);
          process.stderr.write(`Error processing line ${lineCount} from ${inputFile}: ${result.error}\n`);
          return;
        }

        // Get queryId for the output filename
        const queryId = result.queryInfo.queryId;
        const outputFile = path.join(targetDir, `${queryId}.json`);

        // Create output stream and write the data
        const outputStream = fs.createWriteStream(outputFile);
        outputStream.write(JSON.stringify(result.queryInfo, null, 2));
        outputStream.end();

        console.log(`Successfully converted line ${lineCount} from ${inputFile} to ${outputFile}`);
        results.push({ queryId, lineNumber: lineCount });
      } catch (error) {
        const errorMsg = `Error processing line ${lineCount} from ${inputFile}: ${error.message}`;
        console.error(errorMsg);
        process.stderr.write(`${errorMsg}\n`);
      }
    });

    rl.on('close', () => {
      resolve(results);
    });

    rl.on('error', (err) => {
      reject(err);
    });
  });
}

/**
 * Get all files in a directory recursively
 */
function getAllFiles(dirPath, arrayOfFiles = []) {
  const files = fs.readdirSync(dirPath);

  files.forEach(file => {
    const filePath = path.join(dirPath, file);
    if (fs.statSync(filePath).isDirectory()) {
      arrayOfFiles = getAllFiles(filePath, arrayOfFiles);
    } else {
      arrayOfFiles.push(filePath);
    }
  });

  return arrayOfFiles;
}

/**
 * Main function to process the conversion
 */
async function main() {
  // Parse command line arguments
  const args = process.argv.slice(2);
  let sourceDir = null;
  let targetDir = null;
  let showHelp = false;

  // Simple argument parsing
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--help' || args[i] === '-h') {
      showHelp = true;
    } else if (!sourceDir) {
      sourceDir = args[i];
    } else if (!targetDir) {
      targetDir = args[i];
    }
  }

  if (showHelp || !sourceDir || !targetDir) {
    console.error('Usage: node convert-el-to-query.js <source-directory> <target-directory>');
    console.error('');
    console.error('Options:');
    console.error('  --help, -h  Show this help message');
    process.exit(1);
  }

  try {
    // Check if source directory exists
    if (!fs.existsSync(sourceDir)) {
      console.error(`Source directory does not exist: ${sourceDir}`);
      process.exit(1);
    }

    // Create target directory if it doesn't exist
    if (!fs.existsSync(targetDir)) {
      fs.mkdirSync(targetDir, { recursive: true });
    }

    // Get all JSON files in the source directory recursively
    const files = getAllFiles(sourceDir)
      .filter(file => file.toLowerCase().endsWith('.json'));

    if (files.length === 0) {
      console.error(`No JSON files found in source directory: ${sourceDir}`);
      process.exit(1);
    }

    console.log(`Found ${files.length} JSON files to process`);

    // Process each file
    const results = [];
    for (const file of files) {
      const result = await processFile(file, targetDir);
      results.push(result);
    }

    // Summarize results
    const successful = results.filter(r => r.success);
    const failed = results.filter(r => !r.success);

    console.log(`\nConversion Summary:`);
    console.log(`- Successfully processed ${successful.length} files`);
    if (failed.length > 0) {
      console.error(`- Failed to process ${failed.length} files`);
      failed.forEach(f => console.error(`  - ${f.file}: ${f.error}`));
    }
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

// Run the main function
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});