// src/services/apiService.js
import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

const apiService = {
  // 辅助函数，构建 URL
  _buildUrl: (path, environmentId) => {
    let cleanedPath = path.startsWith('/') ? path : `/${path}`;

    // 如果 environmentId 是 undefined 或 null，则表示这是一个全局路径
    // 例如：/environments (list environments), /environments (create environment)
    if (environmentId === undefined || environmentId === null) {
        return `${API_BASE_URL}${cleanedPath}`;
    }
    // 否则，按环境路径拼接
    return `${API_BASE_URL}/environments/${environmentId}${cleanedPath}`;
  },

  get: async (path, environmentId = undefined, params = {}) => {
    const url = apiService._buildUrl(path, environmentId);
    try {
      const response = await axios.get(url, { params });
      return response.data;
    } catch (error) {
      console.error('API GET Error:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  post: async (path, environmentId = undefined, data = {}, config = {}) => {
    const url = apiService._buildUrl(path, environmentId);
    try {
      const response = await axios.post(url, data, config);
      return response.data;
    } catch (error) {
      console.error('API POST Error:', error.response?.data || error.message);
      // 传递更详细的后端错误信息
      const backendError = error.response?.data || { detail: error.message };
      throw backendError;
    }
  },

  put: async (path, environmentId = undefined, data = {}) => {
    const url = apiService._buildUrl(path, environmentId);
    try {
      const response = await axios.put(url, data);
      return response.data;
    } catch (error) {
      console.error('API PUT Error:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  delete: async (path, environmentId = undefined, config = {}) => {
    const url = apiService._buildUrl(path, environmentId);
    try {
      const response = await axios.delete(url, config);
      return response.data;
    } catch (error) {
      console.error('API DELETE Error:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  },

  uploadFile: async (path, environmentId, formData, config = {}) => {
    // 对于文件上传，通常它属于某个环境，所以 environmentId 应该总是存在的
    if (environmentId === undefined || environmentId === null) {
        console.error("Environment ID is required for file upload.");
        throw new Error("Environment ID is required for file upload.");
    }
    const url = apiService._buildUrl(path, environmentId);
    try {
      const response = await axios.post(url, formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
          ...config.headers,
        },
        ...config,
      });
      return response.data;
    } catch (error) {
      console.error('API File Upload Error:', error.response?.data || error.message);
      throw error.response?.data || error;
    }
  }
};

export default apiService;