import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL;

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add request interceptor to inject token
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Add response interceptor to handle expired/invalid tokens
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response && error.response.status === 401) {
      localStorage.removeItem('token');
      // Preserve current path for redirect after login
      const currentPath = window.location.pathname;
      window.location.href = `/login?redirect=${encodeURIComponent(currentPath)}`;
    }
    return Promise.reject(error);
  }
);

// Datasets
export const getDatasets = () => api.get('/datasets');
export const getDataset = (name) => api.get(`/datasets/${name}`);

// Lineage
export const getLineageGraph = (datasetName) => api.get(`/datasets/${datasetName}/lineage`);
export const getUpstreamDatasets = (datasetName, depth = 3) =>
  api.get(`/datasets/${datasetName}/upstream?depth=${depth}`);
export const getDownstreamDatasets = (datasetName, depth = 3) =>
  api.get(`/datasets/${datasetName}/downstream?depth=${depth}`);

// Anomalies
export const getAnomalies = (hours = 24, severity = null, limit = 100) => {
  let url = `/anomalies?hours=${hours}&limit=${limit}`;
  if (severity) url += `&severity=${severity}`;
  return api.get(url);
};
export const getDatasetAnomalies = (datasetName, hours = 24) =>
  api.get(`/datasets/${datasetName}/anomalies?hours=${hours}`);

// Alert Rules
export const getAlertRules = () => api.get('/alert-rules');
export const createAlertRule = (rule) => api.post('/alert-rules', rule);
export const updateAlertRule = (id, rule) => api.put(`/alert-rules/${id}`, rule);
export const deleteAlertRule = (id) => api.delete(`/alert-rules/${id}`);

// Alert History
export const getAlertHistory = (hours = 24) => api.get(`/alert-history?hours=${hours}`);

// Column Lineage
export const getDatasetColumns = (datasetName) => api.get(`/datasets/${datasetName}/column-lineage`);
export const getColumnUpstream = (datasetName, columnName, maxDepth = 10) =>
  api.get(`/datasets/${datasetName}/columns/${columnName}/upstream?max_depth=${maxDepth}`);
export const getColumnDownstream = (datasetName, columnName, maxDepth = 10) =>
  api.get(`/datasets/${datasetName}/columns/${columnName}/downstream?max_depth=${maxDepth}`);
export const getColumnLineageGraph = (datasetName, columnName, maxDepth = 10) =>
  api.get(`/datasets/${datasetName}/columns/${columnName}/lineage?max_depth=${maxDepth}`);

// Impact Analysis
export const getColumnImpact = (datasetName, columnName, maxDepth = 10) =>
  api.get(`/datasets/${datasetName}/columns/${columnName}/impact?max_depth=${maxDepth}`);

// Schema
export const getSchemaHistory = (datasetName, limit = 10) =>
  api.get(`/datasets/${datasetName}/schema/history?limit=${limit}`);
export const getCurrentSchema = (datasetName) =>
  api.get(`/datasets/${datasetName}/schema`);
export const getSchemaDiff = (datasetName, fromVersion, toVersion) =>
  api.get(`/datasets/${datasetName}/schema/diff?from_version=${fromVersion}&to_version=${toVersion}`);

// Jobs
export const getJobs = (status = null, jobName = null, hours = 24, limit = 100, offset = 0) => {
  let url = `/jobs?hours=${hours}&limit=${limit}&offset=${offset}`;
  if (status) url += `&status=${status}`;
  if (jobName) url += `&job_name=${encodeURIComponent(jobName)}`;
  return api.get(url);
};
export const getJob = (uuid) => api.get(`/jobs/${uuid}`);
export const getJobStages = (uuid) => api.get(`/jobs/${uuid}/stages`);
export const getJobStage = (uuid, stageId) => api.get(`/jobs/${uuid}/stages/${stageId}`);
export const getJobsSummary = (hours = 24) => api.get(`/jobs/stats/summary?hours=${hours}`);

// API Keys Management
export const getApiKeys = () => api.get('/api/v1/auth/api-keys');
export const createApiKey = (payload) => api.post('/api/v1/auth/api-keys', payload);
export const deleteApiKey = (keyId) => api.delete(`/api/v1/auth/api-keys/${keyId}`);

export default api;