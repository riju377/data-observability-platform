import { apiCache, getCacheKey } from '../utils/cache';
import * as api from './api';

/**
 * Cached datasets - TTL: 2 minutes
 */
export const getCachedDatasets = () => {
  const key = getCacheKey('datasets');
  return apiCache.getOrFetch(
    key,
    () => api.getDatasets().then(res => res.data),
    120000
  );
};

/**
 * Cached anomalies - TTL: 30 seconds (volatile data)
 */
export const getCachedAnomalies = (hours = 24, severity = null, limit = 100) => {
  const key = getCacheKey('anomalies', { hours, severity, limit });
  return apiCache.getOrFetch(
    key,
    () => api.getAnomalies(hours, severity, limit).then(res => res.data),
    30000
  );
};

/**
 * Cached alert history - TTL: 30 seconds
 */
export const getCachedAlertHistory = (hours = 24) => {
  const key = getCacheKey('alertHistory', { hours });
  return apiCache.getOrFetch(
    key,
    () => api.getAlertHistory(hours).then(res => res.data),
    30000
  );
};

/**
 * Cached alert rules - TTL: 2 minutes
 */
export const getCachedAlertRules = () => {
  const key = getCacheKey('alertRules');
  return apiCache.getOrFetch(
    key,
    () => api.getAlertRules().then(res => res.data),
    120000 // 2 minutes
  );
};

/**
 * Cached lineage graph - TTL: 5 minutes (relatively static)
 */
export const getCachedLineageGraph = (datasetName) => {
  const key = getCacheKey('lineageGraph', { datasetName });
  return apiCache.getOrFetch(
    key,
    () => api.getLineageGraph(datasetName).then(res => res.data),
    300000
  );
};

/**
 * Cached schema history - TTL: 5 minutes
 */
export const getCachedSchemaHistory = (datasetName, limit = 10) => {
  const key = getCacheKey('schemaHistory', { datasetName, limit });
  return apiCache.getOrFetch(
    key,
    () => api.getSchemaHistory(datasetName, limit).then(res => res.data),
    300000
  );
};

/**
 * Cached dataset columns - TTL: 5 minutes
 */
export const getCachedDatasetColumns = (datasetName) => {
  const key = getCacheKey('datasetColumns', { datasetName });
  return apiCache.getOrFetch(
    key,
    () => api.getDatasetColumns(datasetName).then(res => res.data),
    300000
  );
};

/**
 * Cached column lineage graph - TTL: 5 minutes
 */
export const getCachedColumnLineage = (datasetName, columnName) => {
  const key = getCacheKey('columnLineage', { datasetName, columnName });
  return apiCache.getOrFetch(
    key,
    () => api.getColumnLineageGraph(datasetName, columnName).then(res => res.data),
    300000
  );
};

/**
 * Cached column impact analysis - TTL: 5 minutes
 */
export const getCachedColumnImpact = (datasetName, columnName) => {
  const key = getCacheKey('columnImpact', { datasetName, columnName });
  return apiCache.getOrFetch(
    key,
    () => api.getColumnImpact(datasetName, columnName).then(res => res.data),
    300000
  );
};

/**
 * Invalidate all dataset-related caches
 */
export const invalidateDatasetCache = () => {
  apiCache.invalidate('datasets');
  apiCache.invalidate('lineageGraph');
  apiCache.invalidate('schemaHistory');
  apiCache.invalidate('datasetColumns');
  apiCache.invalidate('columnLineage');
  apiCache.invalidate('columnImpact');
};

/**
 * Invalidate monitoring caches
 */
export const invalidateMonitoringCache = () => {
  apiCache.invalidate('anomalies');
  apiCache.invalidate('alertHistory');
  apiCache.invalidate('alertRules');
};

/**
 * Cached jobs list - TTL: 1 minute (dynamic data)
 */
export const getCachedJobs = (status = null, name = null, hours = 24) => {
  const key = getCacheKey('jobs', { status, name, hours });
  return apiCache.getOrFetch(
    key,
    () => api.getJobs(status, name, hours).then(res => res.data),
    60000 // 1 minute
  );
};

/**
 * Cached jobs summary - TTL: 1 minute
 */
export const getCachedJobsSummary = (hours = 24) => {
  const key = getCacheKey('jobsSummary', { hours });
  return apiCache.getOrFetch(
    key,
    () => api.getJobsSummary(hours).then(res => res.data),
    60000 // 1 minute
  );
};

/**
 * Invalidate jobs caches
 */
export const invalidateJobsCache = () => {
  apiCache.invalidate('jobs');
  apiCache.invalidate('jobsSummary');
};

/**
 * Cached API keys - TTL: 2 minutes
 */
export const getCachedApiKeys = () => {
  const key = getCacheKey('apiKeys');
  return apiCache.getOrFetch(
    key,
    () => api.getApiKeys().then(res => res.data),
    120000 // 2 minutes
  );
};

/**
 * Invalidate API keys cache
 */
export const invalidateApiKeysCache = () => {
  apiCache.invalidate('apiKeys');
};
