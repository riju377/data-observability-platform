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
};
