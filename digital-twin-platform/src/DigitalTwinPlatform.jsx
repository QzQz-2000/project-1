import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Download,
  Building,
  Database,
  Settings,
  Plus,
  Edit,
  Trash2,
  Search,
  Network,
  Upload,
  Play,
  Eye,
  ChevronRight,
  ChevronDown,
  Activity,
  Cpu,
  GitBranch,
  X,
  Check,
  AlertCircle,
  FileText,
  Save,
  Link,
  Unlink,
  Target,
  Layers,
  Wifi,
  WifiOff,
  Bell,
  Zap,
  TrendingUp,
  Clock,
  BarChart3
} from 'lucide-react';
import * as d3 from 'd3';

// API base configuration
const API_BASE_URL = 'http://localhost:8000';
const WORKFLOW_API_BASE_URL = 'http://localhost:8001';
const WS_BASE_URL = 'ws://localhost:8001';

// Style definitions
const styles = {
  app: {
    minHeight: '100vh',
    backgroundColor: '#f8fafc',
    fontFamily: 'system-ui, -apple-system, sans-serif'
  },
  lastUpdatedBox: {
    display: 'flex',
    alignItems: 'center',
    padding: '12px',
    backgroundColor: '#f0f9ff',
    border: '1px solid #0ea5e9',
    borderRadius: '6px',
    margin: '16px 0'
  },
  lastUpdatedText: {
    fontSize: '14px',
    color: '#0369a1',
    marginLeft: '8px'
  },
  header: {
    backgroundColor: '#ffffff',
    borderBottom: '1px solid #e2e8f0',
    boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1)'
  },
  headerContainer: {
    maxWidth: '1200px',
    margin: '0 auto',
    padding: '0 24px'
  },
  headerContent: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    height: '64px'
  },
  headerLeft: {
    display: 'flex',
    alignItems: 'center'
  },
  headerRight: {
    display: 'flex',
    alignItems: 'center',
    gap: '16px'
  },
  logo: {
    width: '32px',
    height: '32px',
    color: '#2563eb',
    marginRight: '12px'
  },
  title: {
    fontSize: '20px',
    fontWeight: '600',
    color: '#1f2937',
    margin: 0
  },
  select: {
    border: '1px solid #d1d5db',
    borderRadius: '6px',
    padding: '8px 12px',
    fontSize: '14px',
    backgroundColor: '#ffffff',
    minWidth: '200px'
  },
  mainContainer: {
    maxWidth: '1200px',
    margin: '0 auto',
    padding: '32px 24px'
  },
  contentWrapper: {
    display: 'flex',
    gap: '24px'
  },
  sidebar: {
    width: '256px',
    backgroundColor: '#ffffff',
    borderRadius: '8px',
    boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1)',
    padding: '16px'
  },
  navButton: {
    width: '100%',
    display: 'flex',
    alignItems: 'center',
    padding: '8px 12px',
    marginBottom: '4px',
    fontSize: '14px',
    fontWeight: '500',
    border: 'none',
    borderRadius: '6px',
    cursor: 'pointer',
    transition: 'all 0.2s',
    backgroundColor: 'transparent',
    color: '#4b5563'
  },
  navButtonActive: {
    backgroundColor: '#dbeafe',
    color: '#1d4ed8'
  },
  navIcon: {
    width: '20px',
    height: '20px',
    marginRight: '12px'
  },
  mainContent: {
    flex: 1,
    backgroundColor: '#ffffff',
    borderRadius: '8px',
    boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1)',
    minHeight: '600px'
  },
  contentHeader: {
    padding: '24px',
    borderBottom: '1px solid #e2e8f0',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center'
  },
  contentTitle: {
    fontSize: '24px',
    fontWeight: '600',
    color: '#1f2937',
    margin: 0
  },
  primaryButton: {
    display: 'inline-flex',
    alignItems: 'center',
    padding: '8px 16px',
    border: 'none',
    fontSize: '14px',
    fontWeight: '500',
    borderRadius: '6px',
    boxShadow: '0 1px 2px 0 rgba(0, 0, 0, 0.05)',
    color: '#ffffff',
    backgroundColor: '#2563eb',
    cursor: 'pointer',
    transition: 'all 0.2s'
  },
  buttonIcon: {
    width: '16px',
    height: '16px',
    marginRight: '8px'
  },
  content: {
    padding: '24px'
  },
  errorMessage: {
    marginBottom: '16px',
    backgroundColor: '#fef2f2',
    border: '1px solid #fecaca',
    borderRadius: '6px',
    padding: '16px',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center'
  },
  errorText: {
    display: 'flex',
    alignItems: 'center',
    color: '#b91c1c'
  },
  loadingContainer: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '48px'
  },
  spinner: {
    width: '32px',
    height: '32px',
    border: '2px solid #e2e8f0',
    borderTop: '2px solid #2563eb',
    borderRadius: '50%',
    animation: 'spin 1s linear infinite'
  },
  emptyState: {
    textAlign: 'center',
    padding: '48px',
    color: '#6b7280'
  },
  emptyStateIcon: {
    width: '48px',
    height: '48px',
    color: '#9ca3af',
    margin: '0 auto 16px'
  },
  listItem: {
    border: '1px solid #e2e8f0',
    borderRadius: '8px',
    padding: '16px',
    marginBottom: '16px'
  },
  itemContent: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start'
  },
  itemInfo: {
    flex: 1
  },
  itemTitle: {
    fontSize: '18px',
    fontWeight: '500',
    color: '#1f2937',
    margin: '0 0 4px 0'
  },
  itemSubtitle: {
    fontSize: '14px',
    color: '#6b7280',
    margin: '0 0 8px 0'
  },
  itemDescription: {
    color: '#4b5563',
    margin: '8px 0'
  },
  itemMeta: {
    fontSize: '12px',
    color: '#6b7280',
    margin: '8px 0 0 0'
  },
  itemActions: {
    display: 'flex',
    gap: '8px'
  },
  actionButton: {
    padding: '8px',
    border: 'none',
    borderRadius: '6px',
    cursor: 'pointer',
    transition: 'all 0.2s'
  },
  actionButtonBlue: {
    color: '#2563eb',
    backgroundColor: 'transparent'
  },
  actionButtonGray: {
    color: '#6b7280',
    backgroundColor: 'transparent'
  },
  actionButtonRed: {
    color: '#dc2626',
    backgroundColor: 'transparent'
  },
  actionButtonGreen: {
    color: '#16a34a',
    backgroundColor: 'transparent'
  },
  actionIcon: {
    width: '16px',
    height: '16px'
  },
  modal: {
    position: 'fixed',
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
    backgroundColor: 'rgba(0, 0, 0, 0.5)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    zIndex: 50
  },
  modalContent: {
    backgroundColor: '#ffffff',
    borderRadius: '8px',
    padding: '24px',
    maxWidth: '600px',
    width: '100%',
    margin: '16px',
    maxHeight: '90vh',
    overflowY: 'auto'
  },
  modalContentLarge: {
    backgroundColor: '#ffffff',
    borderRadius: '8px',
    padding: '24px',
    maxWidth: '800px',
    width: '100%',
    margin: '16px',
    maxHeight: '90vh',
    overflowY: 'auto'
  },
  modalHeader: {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: '16px'
  },
  modalTitle: {
    fontSize: '20px',
    fontWeight: '600',
    margin: 0
  },
  closeButton: {
    background: 'none',
    border: 'none',
    color: '#6b7280',
    cursor: 'pointer',
    padding: '4px'
  },
  formGroup: {
    marginBottom: '16px'
  },
  formLabel: {
    display: 'block',
    fontSize: '14px',
    fontWeight: '500',
    color: '#374151',
    marginBottom: '4px'
  },
  formInput: {
    width: '100%',
    padding: '8px 12px',
    border: '1px solid #d1d5db',
    borderRadius: '6px',
    fontSize: '14px',
    boxSizing: 'border-box'
  },
  formTextarea: {
    width: '100%',
    padding: '8px 12px',
    border: '1px solid #d1d5db',
    borderRadius: '6px',
    fontSize: '14px',
    fontFamily: 'inherit',
    resize: 'vertical',
    boxSizing: 'border-box'
  },
  formActions: {
    display: 'flex',
    justifyContent: 'flex-end',
    gap: '12px',
    marginTop: '24px'
  },
  cancelButton: {
    padding: '8px 16px',
    border: '1px solid #d1d5db',
    borderRadius: '6px',
    fontSize: '14px',
    fontWeight: '500',
    color: '#374151',
    backgroundColor: '#ffffff',
    cursor: 'pointer'
  },
  jsonEditor: {
    width: '100%',
    height: '400px',
    padding: '12px',
    border: '1px solid #d1d5db',
    borderRadius: '6px',
    fontFamily: 'Monaco, Consolas, monospace',
    fontSize: '12px',
    boxSizing: 'border-box'
  },
  jsonError: {
    marginTop: '4px',
    fontSize: '12px',
    color: '#dc2626',
    display: 'flex',
    alignItems: 'center'
  },
  badge: {
    display: 'inline-flex',
    alignItems: 'center',
    padding: '2px 8px',
    borderRadius: '9999px',
    fontSize: '12px',
    fontWeight: '500'
  },
  badgeGreen: {
    backgroundColor: '#dcfce7',
    color: '#166534'
  },
  badgeGray: {
    backgroundColor: '#f3f4f6',
    color: '#374151'
  },
  badgeRed: {
    backgroundColor: '#fee2e2',
    color: '#991b1b'
  },
  badgeYellow: {
    backgroundColor: '#fef3c7',
    color: '#92400e'
  },
  badgeBlue: {
    backgroundColor: '#dbeafe',
    color: '#1e40af'
  },
  preCode: {
    backgroundColor: '#f8fafc',
    padding: '16px',
    borderRadius: '6px',
    fontSize: '12px',
    overflowX: 'auto',
    maxHeight: '384px',
    whiteSpace: 'pre-wrap',
    fontFamily: 'Monaco, Consolas, monospace'
  },
  grid2: {
    display: 'grid',
    gridTemplateColumns: '1fr 1fr',
    gap: '16px'
  },
  grid3: {
    display: 'grid',
    gridTemplateColumns: '1fr 1fr 1fr',
    gap: '16px'
  },
  warningMessage: {
    marginBottom: '16px',
    backgroundColor: '#fefce8',
    border: '1px solid #fde047',
    borderRadius: '6px',
    padding: '16px'
  },
  warningText: {
    color: '#a16207'
  }
};

// Add CSS animation
const spinKeyframes = `
@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
`;

// Insert CSS into head
if (typeof document !== 'undefined') {
  const style = document.createElement('style');
  style.textContent = spinKeyframes;
  document.head.appendChild(style);
}

// Date formatting helper function (Netherlands local time)
const formatDate = (dateString) => {
  if (!dateString) return 'Unknown';

  try {
    let cleanDateString = dateString;
    if (typeof dateString === 'string' && dateString.includes('+00:00Z')) {
      cleanDateString = dateString.replace('+00:00Z', '+00:00');
    }

    const date = new Date(cleanDateString);

    if (isNaN(date.getTime())) {
      console.error('Date parsing failed:', dateString);
      return 'Invalid Format';
    }

    return date.toLocaleString('nl-NL', {
      timeZone: 'Europe/Amsterdam', // 强制荷兰时间
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  } catch (error) {
    console.error('Date formatting error:', error);
    return 'Parse Error';
  }
};


const GrafanaDashboard = ({ selectedEnvironment }) => {
  const [dashboardUrl] = useState(
    'http://localhost:3000/d/a3640573-e6d0-4dea-9aff-5eae6808544a/device?orgId=1&refresh=5s&from=1755829753896&to=1755830053896&theme=current'
  );

  const openGrafanaExternal = () => {
    window.open('http://localhost:3000', '_blank');
  };

  const openDashboardExternal = () => {
    window.open(dashboardUrl, '_blank');
  };

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <BarChart3 style={styles.emptyStateIcon} />
        <p>Please select an environment first</p>
      </div>
    );
  }

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>Grafana Dashboard</h2>
        <div style={{ display: 'flex', gap: '12px' }}>
          <button onClick={openGrafanaExternal} style={styles.primaryButton}>
            Open Grafana
          </button>
          <button onClick={openDashboardExternal} style={styles.primaryButton}>
            Open Dashboard
          </button>
        </div>
      </div>
    </div>
  );
};

// CSV下载功能
const downloadCSV = (data, filename = 'workflow_result.csv') => {
  if (!data || data.length === 0) {
    alert('No data to download');
    return;
  }

  // 获取所有列名
  const headers = Object.keys(data[0]);
  
  // 构建CSV内容
  const csvContent = [
    // CSV头部
    headers.join(','),
    // CSV数据行
    ...data.map(row => 
      headers.map(header => {
        const value = row[header];
        // 处理包含逗号、引号或换行符的值
        if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
          return `"${value.replace(/"/g, '""')}"`;
        }
        return value;
      }).join(',')
    )
  ].join('\n');

  // 创建Blob并下载
  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');
  
  if (link.download !== undefined) {
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', filename);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }
};

// Workflow API service class
class WorkflowAPI {
  static async request(endpoint, options = {}) {
    try {
      const response = await fetch(`${WORKFLOW_API_BASE_URL}${endpoint}`, {
        headers: {
          'Content-Type': 'application/json',
          ...options.headers
        },
        ...options
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
      }

      return response.json();
    } catch (error) {
      console.error('Workflow API Request failed:', error);
      throw error;
    }
  }

  static async submitWorkflow(environmentId, workflowData) {
    return this.request(`/environments/${environmentId}/workflows/submit`, {
      method: 'POST',
      body: JSON.stringify(workflowData)
    });
  }

  static async getWorkflowStatus(environmentId, workflowId) {
    return this.request(`/environments/${environmentId}/workflows/${workflowId}/status`);
  }

  // static async getTaskResult(environmentId, workflowId, taskName) {
  //   return this.request(`/environments/${environmentId}/workflows/${workflowId}/tasks/${taskName}/result`);
  // }

  static async listWorkflows(environmentId) {
    return this.request(`/environments/${environmentId}/workflows`);
  }

  static async deleteWorkflow(environmentId, workflowId) {
    return this.request(`/environments/${environmentId}/workflows/${workflowId}`, {
      method: 'DELETE'
    });
  }

  static async getAvailableFunctions() {
    return this.request('/functions');
  }

  static async getHealth() {
    return this.request('/health');
  }

  static async getWorkflowFinalResult(environmentId, workflowId) {
    return this.request(`/environments/${environmentId}/workflows/${workflowId}/final-result`);
  }

  static async getTaskResult(environmentId, workflowId, taskName) {
    return this.request(`/environments/${environmentId}/workflows/${workflowId}/tasks/${taskName}/result`);
  }
}

// Enhanced API service class
class DigitalTwinAPI {
  static async request(endpoint, options = {}) {
    try {
      const response = await fetch(`${API_BASE_URL}${endpoint}`, {
        headers: {
          'Content-Type': 'application/json',
          ...options.headers
        },
        ...options
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
      }

      return response.json();
    } catch (error) {
      console.error('API Request failed:', error);
      throw error;
    }
  }

  // Environment management
  static async getEnvironments() {
    const response = await this.request('/environments');
    return response.items || response;
  }

  static async createEnvironment(data) {
    return this.request('/environments', {
      method: 'POST',
      body: JSON.stringify(data)
    });
  }

  static async updateEnvironment(envId, data) {
    return this.request(`/environments/${envId}`, {
      method: 'PUT',
      body: JSON.stringify(data)
    });
  }

  static async deleteEnvironment(envId) {
    return this.request(`/environments/${envId}`, {
      method: 'DELETE'
    });
  }

  // Model management
  static async getModels(envId) {
    const response = await this.request(`/environments/${envId}/models`);
    return response.items || response;
  }

  static async getModel(envId, modelId) {
    return this.request(`/environments/${envId}/models/${modelId}`);
  }

  static async createModel(envId, data) {
    return this.request(`/environments/${envId}/models`, {
      method: 'POST',
      body: JSON.stringify(data)
    });
  }

  static async updateModel(envId, modelId, data) {
    return this.request(`/environments/${envId}/models/${modelId}`, {
      method: 'PUT',
      body: JSON.stringify(data)
    });
  }

  static async deleteModel(envId, modelId) {
    return this.request(`/environments/${envId}/models/${modelId}`, {
      method: 'DELETE'
    });
  }

  // Digital twin management
  static async getTwins(envId) {
    const response = await this.request(`/environments/${envId}/twins`);
    return response.items || response;
  }

  static async getTwin(envId, twinId) {
    return this.request(`/environments/${envId}/twins/${twinId}`);
  }

  static async createTwin(envId, data) {
    return this.request(`/environments/${envId}/twins`, {
      method: 'POST',
      body: JSON.stringify(data)
    });
  }

  static async updateTwin(envId, twinId, data) {
    return this.request(`/environments/${envId}/twins/${twinId}`, {
      method: 'PUT',
      body: JSON.stringify(data)
    });
  }

  static async deleteTwin(envId, twinId) {
    return this.request(`/environments/${envId}/twins/${twinId}`, {
      method: 'DELETE'
    });
  }

  // Device management (updated - no properties)
  static async getDevices(envId) {
    const response = await this.request(`/environments/${envId}/devices`);
    return response.items || response;
  }

  static async getDevice(envId, deviceId) {
    return this.request(`/environments/${envId}/devices/${deviceId}`);
  }

  static async createDevice(envId, data) {
    return this.request(`/environments/${envId}/devices`, {
      method: 'POST',
      body: JSON.stringify(data)
    });
  }

  static async updateDevice(envId, deviceId, data) {
    return this.request(`/environments/${envId}/devices/${deviceId}`, {
      method: 'PUT',
      body: JSON.stringify(data)
    });
  }

  static async deleteDevice(envId, deviceId) {
    return this.request(`/environments/${envId}/devices/${deviceId}`, {
      method: 'DELETE'
    });
  }

  // Device-Twin Mapping management (NEW)
  static async getDeviceTwinMappings(envId) {
    const response = await this.request(`/environments/${envId}/mappings`);
    return response.items || response;
  }

  static async getDeviceMappings(envId, deviceId) {
    return this.request(`/environments/${envId}/mappings/device/${deviceId}`);
  }

  static async getTwinMappings(envId, twinId) {
    return this.request(`/environments/${envId}/mappings/twin/${twinId}`);
  }

  static async createDeviceTwinMapping(envId, data) {
    return this.request(`/environments/${envId}/mappings`, {
      method: 'POST',
      body: JSON.stringify(data)
    });
  }

  static async updateDeviceTwinMapping(envId, deviceId, twinId, data) {
    return this.request(`/environments/${envId}/mappings/${deviceId}/${twinId}`, {
      method: 'PUT',
      body: JSON.stringify(data)
    });
  }

  static async deleteDeviceTwinMapping(envId, deviceId, twinId) {
    return this.request(`/environments/${envId}/mappings/${deviceId}/${twinId}`, {
      method: 'DELETE'
    });
  }

  // Relationship management
  static async getRelationships(envId) {
    const response = await this.request(`/environments/${envId}/relationships`);
    return response.items || response;
  }

  static async createRelationship(envId, data) {
    return this.request(`/environments/${envId}/relationships`, {
      method: 'POST',
      body: JSON.stringify(data)
    });
  }

  static async updateRelationship(envId, sourceId, targetId, relationshipName, data) {
    return this.request(`/environments/${envId}/relationships/${sourceId}/${targetId}/${relationshipName}`, {
      method: 'PUT',
      body: JSON.stringify(data)
    });
  }

  static async deleteRelationship(envId, sourceId, targetId, relationshipName) {
    return this.request(`/environments/${envId}/relationships/${sourceId}/${targetId}/${relationshipName}`, {
      method: 'DELETE'
    });
  }

  // Tree Graph and Advanced features (UPDATED)
  static async getTreeGraph(envId, rootTwinId = null) {
    const url = rootTwinId ? `/environments/${envId}/tree-graph?root_twin_id=${rootTwinId}` : `/environments/${envId}/tree-graph`;
    return this.request(url);
  }

  static async getRelationshipStats(envId) {
    return this.request(`/environments/${envId}/relationships/stats`);
  }

  static async findPath(envId, sourceId, targetId, maxDepth = 5) {
    return this.request(`/environments/${envId}/relationships/path/${sourceId}/${targetId}?max_depth=${maxDepth}`);
  }
}

// Generic modal component
const Modal = ({ isOpen, onClose, title, children, size = 'medium' }) => {
  if (!isOpen) return null;

  const modalStyle = size === 'large' ? styles.modalContentLarge : styles.modalContent;

  return (
    <div style={styles.modal}>
      <div style={modalStyle}>
        <div style={styles.modalHeader}>
          <h2 style={styles.modalTitle}>{title}</h2>
          <button onClick={onClose} style={styles.closeButton}>
            <X style={{width: '24px', height: '24px'}} />
          </button>
        </div>
        {children}
      </div>
    </div>
  );
};

// JSON editor component
const JsonEditor = ({ value, onChange, placeholder = "Enter valid JSON..." }) => {
  const [isValid, setIsValid] = useState(true);
  const [error, setError] = useState('');

  const handleChange = (e) => {
    const newValue = e.target.value;
    onChange(newValue);

    if (!newValue.trim()) {
      setIsValid(true);
      setError('');
      return;
    }

    try {
      JSON.parse(newValue);
      setIsValid(true);
      setError('');
    } catch (err) {
      setIsValid(false);
      setError(err.message);
    }
  };

  return (
    <div>
      <textarea
        value={value}
        onChange={handleChange}
        style={{
          ...styles.jsonEditor,
          borderColor: isValid ? '#d1d5db' : '#dc2626'
        }}
        placeholder={placeholder}
      />
      {!isValid && (
        <div style={styles.jsonError}>
          <AlertCircle style={{ width: '16px', height: '16px', marginRight: '4px' }} />
          JSON format error: {error}
        </div>
      )}
    </div>
  );
};

// Simplified Workflow Management Component
const WorkflowManager = ({ selectedEnvironment }) => {
  const [workflows, setWorkflows] = useState([]);
  const [availableFunctions, setAvailableFunctions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);

  const [showWorkflowDetailModal, setShowWorkflowDetailModal] = useState(false);
  const [showFinalResultModal, setShowFinalResultModal] = useState(false);

  const [selectedWorkflow, setSelectedWorkflow] = useState(null);
  const [finalResultData, setFinalResultData] = useState(null);
  const [error, setError] = useState(null);
  const [loadingResult, setLoadingResult] = useState(false);

  const [showTaskResultModal, setShowTaskResultModal] = useState(false); // New modal for task results
  const [selectedTask, setSelectedTask] = useState(null); // New state for selected task
  const [taskResultData, setTaskResultData] = useState(null); // New state for task result data
  const [loadingTaskResult, setLoadingTaskResult] = useState(false); // New loading state for task results

  // 简化的表单数据 - 只需要一个JSON字符串
  const [workflowFormData, setWorkflowFormData] = useState({
    workflowJson: JSON.stringify({
      name: "",
      description: "",
      steps: []
    }, null, 2)
  });

  useEffect(() => {
    loadAvailableFunctions();
    let interval;
    if (selectedEnvironment) {
      interval = setInterval(() => loadWorkflows(), 5000);
    }
    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [selectedEnvironment]);

  useEffect(() => {
    if (selectedEnvironment) {
      loadWorkflows();
    }
  }, [selectedEnvironment]);

  const loadWorkflows = async () => {
    if (!selectedEnvironment) return;

    try {
      setLoading(true);
      const workflowsData = await WorkflowAPI.listWorkflows(selectedEnvironment.environment_id);
      setWorkflows(workflowsData.workflows || []);
    } catch (err) {
      console.error('Failed to load workflows:', err);
      setWorkflows([]);
    } finally {
      setLoading(false);
    }
  };

  const loadAvailableFunctions = async () => {
    try {
      const functionsData = await WorkflowAPI.getAvailableFunctions();
      setAvailableFunctions(functionsData.functions || []);
    } catch (err) {
      console.error('Failed to load available functions:', err);
      setAvailableFunctions([]);
    }
  };

  // 加载示例工作流的函数
  const loadExampleWorkflow = () => {
    const exampleWorkflow = {
      name: "Moving Average and Threshold Alarm Workflow",
      description: "Load data -> Calculate moving average -> Apply threshold alarm",
      steps: [
        {
          name: "load_data",
          task_type: "DATA",
          data_source_config: {
            source_type: "csv",
            query_config: {
              file_path: "/app/data/test_data.csv"
            }
          }
        },
        {
          name: "calculate_ma",
          task_type: "FUNCTION",
          function_name: "MovingAverage",
          config: {
            field: "value",
            window: 5
          },
          dependencies: ["load_data"]
        },
        {
          name: "threshold_alarm",
          task_type: "FUNCTION",
          function_name: "ThresholdAlarm",
          config: {
            field: "value_ma5",
            threshold: 30,
            mode: "greater"
          },
          dependencies: ["calculate_ma"]
        }
      ]
    };

    setWorkflowFormData({
      workflowJson: JSON.stringify(exampleWorkflow, null, 2)
    });
  };

  // 简化的提交处理函数
  const handleSubmitWorkflow = async () => {
    try {
      setSubmitting(true);

      // 解析JSON配置
      let workflowConfig;
      try {
        workflowConfig = JSON.parse(workflowFormData.workflowJson);
      } catch (e) {
        throw new Error(`Invalid JSON format: ${e.message}`);
      }

      // 验证必需字段
      if (!workflowConfig.name) {
        throw new Error('Workflow name is required');
      }
      if (!workflowConfig.steps || !Array.isArray(workflowConfig.steps)) {
        throw new Error('Workflow steps array is required');
      }

      // 验证每个步骤
      for (const step of workflowConfig.steps) {
        if (!step.name) {
          throw new Error('Step name is required for all steps');
        }
        if (!step.task_type) {
          throw new Error(`Task type is required for step: ${step.name}`);
        }
        if (step.task_type === 'FUNCTION' && !step.function_name) {
          throw new Error(`Function name is required for FUNCTION step: ${step.name}`);
        }
        if (step.task_type === 'DATA' && !step.data_source_config) {
          throw new Error(`Data source config is required for DATA step: ${step.name}`);
        }
      }

      const workflowData = {
        workflow: workflowConfig,
        submitted_by: 'user'
      };

      const result = await WorkflowAPI.submitWorkflow(selectedEnvironment.environment_id, workflowData);

      setShowCreateForm(false);
      setWorkflowFormData({
        workflowJson: JSON.stringify({
          name: "",
          description: "",
          steps: []
        }, null, 2)
      });

      alert(`Workflow submitted successfully! ID: ${result.workflow_id}`);

      await loadWorkflows();

    } catch (err) {
      alert('Failed to submit workflow: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleRefreshStatus = async (workflowId) => {
    try {
      const statusData = await WorkflowAPI.getWorkflowStatus(selectedEnvironment.environment_id, workflowId);

      const updatedWorkflows = workflows.map(w => {
        if (w.workflow_id === workflowId) {
          return {
            ...w,
            status: statusData.status,
            total_tasks: statusData.total_tasks,
            task_statistics: statusData.task_statistics,
            tasks: statusData.tasks
          };
        }
        return w;
      });

      setWorkflows(updatedWorkflows);
    } catch (err) {
      console.error('Failed to refresh workflow status:', err);
    }
  };

  const handleViewWorkflowDetails = async (workflow) => {
    try {
      setSelectedWorkflow(workflow);
      setShowWorkflowDetailModal(true);
    } catch (err) {
      console.error('Failed to view workflow details:', err);
    }
  };

  // 新增：查看最终结果
  const handleViewFinalResults = async (workflow) => {
    try {
      setLoadingResult(true);
      setSelectedWorkflow(workflow);
      setFinalResultData(null);
      setShowFinalResultModal(true);

      // 调用新的API端点获取最终结果
      const resultData = await WorkflowAPI.getWorkflowFinalResult(
        selectedEnvironment.environment_id,
        workflow.workflow_id
      );

      setFinalResultData(resultData);
    } catch (err) {
      console.error('Failed to load final results:', err);
      setFinalResultData({
        error: 'Failed to load final results: ' + err.message
      });
    } finally {
      setLoadingResult(false);
    }
  };
  // 新增：查看单个任务结果
  const handleViewTaskResult = async (task) => {
    try {
      setLoadingTaskResult(true);
      setSelectedTask(task);
      setTaskResultData(null);
      setShowTaskResultModal(true);

      // 调用API获取任务结果
      const resultData = await WorkflowAPI.getTaskResult(
        selectedEnvironment.environment_id,
        selectedWorkflow.workflow_id,
        task.name
      );

      setTaskResultData(resultData);
    } catch (err) {
      console.error('Failed to load task result:', err);
      setTaskResultData({
        error: 'Failed to load task result: ' + err.message
      });
    } finally {
      setLoadingTaskResult(false);
    }
  };

  const handleDeleteWorkflow = async (workflowId) => {
    if (window.confirm('Are you sure you want to delete this workflow?')) {
      try {
        await WorkflowAPI.deleteWorkflow(selectedEnvironment.environment_id, workflowId);
        await loadWorkflows();
      } catch (err) {
        console.error('Failed to delete workflow:', err);
        alert('Failed to delete workflow: ' + err.message);
      }
    }
  };

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <Activity style={styles.emptyStateIcon} />
        <p>Please select an environment first</p>
      </div>
    );
  }

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>Workflow Management</h2>
        <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
          <button
            onClick={() => setShowCreateForm(true)}
            style={styles.primaryButton}
            disabled={submitting}
          >
            <Plus style={styles.buttonIcon} />
            Create Workflow
          </button>
        </div>
      </div>

      <div style={styles.content}>
        {error && (
          <div style={styles.errorMessage}>
            <span style={styles.errorText}>{error}</span>
            <button onClick={() => setError(null)} style={{ ...styles.closeButton, color: '#dc2626' }}>
              <X style={{ width: '16px', height: '16px' }} />
            </button>
          </div>
        )}

        {loading ? (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
            <span style={{ marginLeft: '8px', color: '#6b7280' }}>Loading workflows...</span>
          </div>
        ) : workflows.length === 0 ? (
          <div style={styles.emptyState}>
            <Activity style={styles.emptyStateIcon} />
            <p>No workflows yet. Create your first workflow to start processing data.</p>
          </div>
        ) : (
          <div>
            {workflows.map(workflow => (
              <div key={workflow.workflow_id} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{workflow.name}</h3>
                    <p style={styles.itemSubtitle}>ID: {workflow.workflow_id}</p>
                    <div style={{ display: 'flex', gap: '8px', alignItems: 'center', margin: '8px 0' }}>
                      <span style={{
                        ...styles.badge,
                        ...(workflow.status === 'completed' ? styles.badgeGreen :
                          workflow.status === 'failed' ? styles.badgeRed :
                            workflow.status === 'running' ? styles.badgeBlue : styles.badgeGray)
                      }}>
                        {workflow.status || 'submitted'}
                      </span>
                      {workflow.total_tasks && (
                        <span style={{ ...styles.badge, ...styles.badgeGray }}>
                          {workflow.total_tasks} tasks
                        </span>
                      )}
                    </div>
                    {workflow.description && (
                      <p style={styles.itemDescription}>{workflow.description}</p>
                    )}
                    <p style={styles.itemMeta}>
                    Created: {(() => {
                      try {
                        const timeString = workflow.created_at;
                        const utcTimeString = timeString.endsWith('Z') ? timeString : timeString + 'Z';
                        const utcDate = new Date(utcTimeString);
                        return utcDate.toLocaleString('en-US', {
                          timeZone: 'Europe/Amsterdam',
                          month: '2-digit',
                          day: '2-digit',
                          hour: 'numeric',
                          minute: '2-digit',
                          hour12: true
                        });
                      } catch (error) {
                        console.error('Date formatting error:', error);
                        return 'Invalid Date';
                      }
                    })()}
                  </p>
                    {workflow.task_statistics && (
                      <div style={{ marginTop: '8px' }}>
                        <span style={{ fontSize: '12px', color: '#6b7280' }}>
                          Progress: {Object.entries(workflow.task_statistics).map(([status, count]) =>
                            `${count} ${status}`
                          ).join(', ')}
                        </span>
                      </div>
                    )}
                  </div>

                  {/* 修改：更新按钮区域 */}
                  <div style={styles.itemActions}>
                    <button
                      onClick={() => handleRefreshStatus(workflow.workflow_id)}
                      style={{ ...styles.actionButton, ...styles.actionButtonBlue }}
                      title="Refresh Status"
                    >
                      <Activity style={styles.actionIcon} />
                    </button>

                    {/* 修改：眼睛图标现在查看工作流详情 */}
                    <button
                      onClick={() => handleViewWorkflowDetails(workflow)}
                      style={{ ...styles.actionButton, ...styles.actionButtonGray }}
                      title="View Workflow Details"
                    >
                      <Eye style={styles.actionIcon} />
                    </button>

                    {/* 新增：图表图标查看最终结果 */}
                    <button
                      onClick={() => handleViewFinalResults(workflow)}
                      style={{
                        ...styles.actionButton,
                        ...styles.actionButtonGreen,
                        // 只有完成的工作流才能查看结果
                        opacity: workflow.status === 'completed' ? 1 : 0.5
                      }}
                      title="View Final Results"
                      disabled={workflow.status !== 'completed'}
                    >
                      <BarChart3 style={styles.actionIcon} />
                    </button>

                    <button
                      onClick={() => handleDeleteWorkflow(workflow.workflow_id)}
                      style={{ ...styles.actionButton, ...styles.actionButtonRed }}
                      title="Delete Workflow"
                    >
                      <Trash2 style={styles.actionIcon} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* 创建工作流模态框 */}
      <Modal
        isOpen={showCreateForm}
        onClose={() => setShowCreateForm(false)}
        title="Create New Workflow"
        size="large"
      >
        <div>
          <div style={styles.formGroup}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
              <label style={styles.formLabel}>Workflow Configuration (JSON) *</label>
              <button
                type="button"
                onClick={loadExampleWorkflow}
                style={{
                  ...styles.actionButton,
                  ...styles.actionButtonBlue,
                  fontSize: '12px',
                  padding: '4px 8px'
                }}
              >
                Load Example
              </button>
            </div>

            <JsonEditor
              value={workflowFormData.workflowJson}
              onChange={(value) => setWorkflowFormData({ workflowJson: value })}
              placeholder={`{
  "name": "My Workflow",
  "description": "Description of what this workflow does",
  "steps": [
    {
      "name": "load_data",
      "task_type": "DATA",
      "data_source_config": {
        "source_type": "csv",
        "query_config": {
          "file_path": "/app/data/your_data.csv"
        }
      }
    },
    {
      "name": "process_data",
      "task_type": "FUNCTION", 
      "function_name": "YourFunction",
      "config": {
        "field": "value",
        "parameter": 10
      },
      "dependencies": ["load_data"]
    }
  ]
}`}
            />
          </div>

          <div style={{
            backgroundColor: '#f0f9ff',
            border: '1px solid #0ea5e9',
            borderRadius: '6px',
            padding: '12px',
            marginTop: '16px',
            fontSize: '12px'
          }}>
            <h4 style={{ margin: '0 0 8px 0', color: '#0369a1' }}>Available Functions:</h4>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
              {availableFunctions.map(func => (
                <span key={func.name} style={{
                  backgroundColor: '#ffffff',
                  padding: '2px 6px',
                  borderRadius: '4px',
                  fontSize: '11px',
                  color: '#0369a1',
                  border: '1px solid #0ea5e9'
                }}>
                  {func.name}
                </span>
              ))}
            </div>
          </div>

          <div style={styles.formActions}>
            <button
              type="button"
              onClick={() => setShowCreateForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSubmitWorkflow}
              style={{
                ...styles.primaryButton,
                opacity: submitting ? 0.5 : 1
              }}
              disabled={submitting}
            >
              {submitting ? 'Submitting...' : 'Submit Workflow'}
            </button>
          </div>
        </div>
      </Modal>

      {/* 修改：工作流详情模态框 */}
      <Modal
        isOpen={showWorkflowDetailModal}
        onClose={() => setShowWorkflowDetailModal(false)}
        title={`Workflow Details: ${selectedWorkflow?.name}`}
        size="large"
      >
        {selectedWorkflow && (
          <div>
            {/* 基础信息 */}
            <div style={styles.grid2}>
              <div>
                <label style={styles.formLabel}>Workflow ID</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px', fontWeight: '500' }}>
                  {selectedWorkflow.workflow_id}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Status</label>
                <span style={{
                  ...styles.badge,
                  ...(selectedWorkflow.status === 'completed' ? styles.badgeGreen :
                    selectedWorkflow.status === 'failed' ? styles.badgeRed :
                      selectedWorkflow.status === 'running' ? styles.badgeBlue : styles.badgeGray)
                }}>
                  {selectedWorkflow.status || 'submitted'}
                </span>
              </div>
              <div>
                <label style={styles.formLabel}>Created</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {formatDate(selectedWorkflow.created_at)}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Total Tasks</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {selectedWorkflow.total_tasks || 0}
                </p>
              </div>
            </div>

            {/* 描述 */}
            {selectedWorkflow.description && (
              <div style={{ marginTop: '16px' }}>
                <label style={styles.formLabel}>Description</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {selectedWorkflow.description}
                </p>
              </div>
            )}

            {/* 任务进度
            {selectedWorkflow.tasks && (
              <div style={{ marginTop: '24px' }}>
                <label style={{ ...styles.formLabel, marginBottom: '8px' }}>
                  Task Progress ({selectedWorkflow.tasks.length} tasks)
                </label>
                <div style={{ maxHeight: '300px', overflow: 'auto' }}>
                  {selectedWorkflow.tasks.map((task, index) => (
                    <div key={index} style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      padding: '12px',
                      marginBottom: '8px',
                      backgroundColor: '#f8fafc',
                      borderRadius: '6px',
                      fontSize: '14px',
                      border: '1px solid #e2e8f0'
                    }}>
                      <div>
                        <div style={{ fontWeight: '500', marginBottom: '4px' }}>{task.name}</div>
                        <div style={{ fontSize: '12px', color: '#6b7280' }}>
                          Type: {task.task_type}
                          {task.function_name && ` • Function: ${task.function_name}`}
                        </div>
                        {task.dependencies && task.dependencies.length > 0 && (
                          <div style={{ fontSize: '12px', color: '#6b7280', marginTop: '4px' }}>
                            Dependencies: {task.dependencies.map(dep => dep.split('#').pop()).join(', ')}
                          </div>
                        )}
                      </div>
                      <span style={{
                        ...styles.badge,
                        ...(task.status === 'completed' ? styles.badgeGreen :
                          task.status === 'failed' ? styles.badgeRed :
                            task.status === 'running' ? styles.badgeBlue : styles.badgeGray)
                      }}>
                        {task.status}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )} */}
            
            {/* 任务进度 - 增强版，包含查看结果按钮 */}
            {selectedWorkflow.tasks && (
              <div style={{ marginTop: '24px' }}>
                <label style={{ ...styles.formLabel, marginBottom: '8px' }}>
                  Task Progress ({selectedWorkflow.tasks.length} tasks)
                </label>
                <div style={{ maxHeight: '400px', overflow: 'auto' }}>
                  {selectedWorkflow.tasks.map((task, index) => (
                    <div key={index} style={{
                      display: 'flex',
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      padding: '12px',
                      marginBottom: '8px',
                      backgroundColor: '#f8fafc',
                      borderRadius: '6px',
                      fontSize: '14px',
                      border: '1px solid #e2e8f0'
                    }}>
                      <div style={{ flex: 1 }}>
                        <div style={{ fontWeight: '500', marginBottom: '4px' }}>{task.name}</div>
                        <div style={{ fontSize: '12px', color: '#6b7280' }}>
                          Type: {task.task_type}
                          {task.function_name && ` • Function: ${task.function_name}`}
                        </div>
                        {task.dependencies && task.dependencies.length > 0 && (
                          <div style={{ fontSize: '12px', color: '#6b7280', marginTop: '4px' }}>
                            Dependencies: {task.dependencies.map(dep => dep.split('#').pop()).join(', ')}
                          </div>
                        )}
                      </div>
                      
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        {/* 查看任务结果按钮 */}
                        <button
                          onClick={() => handleViewTaskResult(task)}
                          style={{
                            ...styles.actionButton,
                            ...styles.actionButtonBlue,
                            padding: '4px 8px',
                            fontSize: '11px',
                            opacity: task.status === 'completed' ? 1 : 0.5
                          }}
                          title={task.status === 'completed' ? "View Task Result" : "Task not completed"}
                          disabled={task.status !== 'completed'}
                        >
                          <FileText style={{ width: '12px', height: '12px' }} />
                          View Result
                        </button>
                        
                        <span style={{
                          ...styles.badge,
                          ...(task.status === 'completed' ? styles.badgeGreen :
                            task.status === 'failed' ? styles.badgeRed :
                              task.status === 'running' ? styles.badgeBlue : styles.badgeGray)
                        }}>
                          {task.status}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* 任务统计 */}
            {selectedWorkflow.task_statistics && (
              <div style={{ marginTop: '24px' }}>
                <label style={styles.formLabel}>Task Statistics</label>
                <div style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(100px, 1fr))',
                  gap: '12px',
                  marginTop: '8px'
                }}>
                  {Object.entries(selectedWorkflow.task_statistics).map(([status, count]) => (
                    <div key={status} style={{
                      textAlign: 'center',
                      padding: '12px',
                      backgroundColor: '#f8fafc',
                      borderRadius: '6px',
                      border: '1px solid #e2e8f0'
                    }}>
                      <div style={{ fontSize: '24px', fontWeight: '600', color: '#1f2937' }}>{count}</div>
                      <div style={{ fontSize: '12px', color: '#6b7280', textTransform: 'capitalize' }}>{status}</div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </Modal>

      {/* 新增：任务结果模态框 */}
      <Modal
        isOpen={showTaskResultModal}
        onClose={() => setShowTaskResultModal(false)}
        title={`Task Result: ${selectedTask?.name}`}
        size="large"
      >
        {selectedTask && (
          <div>
            {/* 任务基础信息 */}
            <div style={styles.grid3}>
              <div>
                <label style={styles.formLabel}>Task Name</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px', fontWeight: '500' }}>
                  {selectedTask.name}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Task Type</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {selectedTask.task_type}
                  {selectedTask.function_name && ` (${selectedTask.function_name})`}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Status</label>
                <span style={{
                  ...styles.badge,
                  ...(selectedTask.status === 'completed' ? styles.badgeGreen :
                    selectedTask.status === 'failed' ? styles.badgeRed :
                      selectedTask.status === 'running' ? styles.badgeBlue : styles.badgeGray)
                }}>
                  {selectedTask.status}
                </span>
              </div>
            </div>

            {/* 依赖关系 */}
            {selectedTask.dependencies && selectedTask.dependencies.length > 0 && (
              <div style={{ marginTop: '16px' }}>
                <label style={styles.formLabel}>Dependencies</label>
                <div style={{ display: 'flex', gap: '8px', marginTop: '4px', flexWrap: 'wrap' }}>
                  {selectedTask.dependencies.map((dep, index) => (
                    <span key={index} style={{ ...styles.badge, ...styles.badgeGray }}>
                      {dep.split('#').pop()}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* 任务结果数据 */}
            <div style={{ marginTop: '24px' }}>
              {loadingTaskResult ? (
                <div style={{ ...styles.loadingContainer, margin: '40px 0' }}>
                  <div style={styles.spinner}></div>
                  <span style={{ marginLeft: '8px', color: '#6b7280' }}>Loading task result...</span>
                </div>
              ) : taskResultData?.error ? (
                <div style={{
                  ...styles.errorMessage,
                  margin: '20px 0'
                }}>
                  <span style={styles.errorText}>{taskResultData.error}</span>
                </div>
              ) : taskResultData && Array.isArray(taskResultData) ? (
                <div>
                  {/* 结果摘要和下载按钮 */}
                  <div style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'center',
                    marginBottom: '16px'
                  }}>
                    <label style={styles.formLabel}>Task Result Data</label>
                    <button
                      onClick={() => downloadCSV(
                        taskResultData,
                        `${selectedWorkflow.name.replace(/[^a-zA-Z0-9]/g, '_')}_${selectedTask.name}_result.csv`
                      )}
                      style={{
                        ...styles.primaryButton,
                        backgroundColor: '#16a34a',
                        fontSize: '12px',
                        padding: '6px 12px'
                      }}
                    >
                      <Download style={{ width: '14px', height: '14px', marginRight: '4px' }} />
                      Download CSV
                    </button>
                  </div>

                  <div style={{
                    backgroundColor: '#f0f9ff',
                    border: '1px solid #0ea5e9',
                    borderRadius: '6px',
                    padding: '12px',
                    fontSize: '14px',
                    marginBottom: '16px'
                  }}>
                    <div style={{ display: 'flex', gap: '24px', flexWrap: 'wrap' }}>
                      <span><strong>Rows:</strong> {taskResultData.length}</span>
                      <span><strong>Columns:</strong> {taskResultData.length > 0 ? Object.keys(taskResultData[0]).length : 0}</span>
                    </div>
                    {taskResultData.length > 0 && (
                      <div style={{ marginTop: '8px' }}>
                        <strong>Columns:</strong> {Object.keys(taskResultData[0]).join(', ')}
                      </div>
                    )}
                  </div>

                  {/* 数据表格 */}
                  <div style={{
                    maxHeight: '400px',
                    overflow: 'auto',
                    border: '1px solid #e2e8f0',
                    borderRadius: '6px'
                  }}>
                    <table style={{ width: '100%', fontSize: '12px', borderCollapse: 'collapse' }}>
                      <thead style={{ backgroundColor: '#f8fafc', position: 'sticky', top: 0 }}>
                        <tr>
                          {taskResultData.length > 0 && Object.keys(taskResultData[0]).map(col => (
                            <th key={col} style={{
                              padding: '8px',
                              textAlign: 'left',
                              borderBottom: '1px solid #e2e8f0',
                              fontWeight: '600'
                            }}>
                              {col}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {taskResultData.slice(0, 100).map((row, index) => (
                          <tr key={index} style={{ borderBottom: '1px solid #f3f4f6' }}>
                            {Object.values(row).map((value, colIndex) => (
                              <td key={colIndex} style={{ padding: '6px 8px' }}>
                                {typeof value === 'boolean' ? (value ? 'true' : 'false') :
                                  typeof value === 'object' && value !== null ? JSON.stringify(value) :
                                    String(value)}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {taskResultData.length > 100 && (
                      <div style={{ padding: '8px', textAlign: 'center', fontSize: '12px', color: '#6b7280' }}>
                        Showing first 100 rows of {taskResultData.length} total rows
                      </div>
                    )}
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        )}
      </Modal>

      {/* 新增：最终结果模态框（带下载CSV功能）*/}
      <Modal
        isOpen={showFinalResultModal}
        onClose={() => setShowFinalResultModal(false)}
        title={`Final Results: ${selectedWorkflow?.name}`}
        size="large"
      >
        {selectedWorkflow && (
          <div>
            {/* 结果摘要信息 */}
            <div style={styles.grid3}>
              <div>
                <label style={styles.formLabel}>Workflow ID</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px', fontWeight: '500' }}>
                  {selectedWorkflow.workflow_id}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Status</label>
                <span style={{
                  ...styles.badge,
                  ...(selectedWorkflow.status === 'completed' ? styles.badgeGreen :
                    selectedWorkflow.status === 'failed' ? styles.badgeRed : styles.badgeGray)
                }}>
                  {selectedWorkflow.status}
                </span>
              </div>
              <div>
                <label style={styles.formLabel}>Completed</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {formatDate(selectedWorkflow.completed_at) || 'In progress'}
                </p>
              </div>
            </div>

            {/* 加载状态或结果内容 */}
            {loadingResult ? (
              <div style={{ ...styles.loadingContainer, margin: '40px 0' }}>
                <div style={styles.spinner}></div>
                <span style={{ marginLeft: '8px', color: '#6b7280' }}>Loading final results...</span>
              </div>
            ) : finalResultData?.error ? (
              <div style={{
                ...styles.errorMessage,
                margin: '20px 0'
              }}>
                <span style={styles.errorText}>{finalResultData.error}</span>
              </div>
            ) : finalResultData?.message ? (
              <div style={{
                backgroundColor: '#fef3c7',
                border: '1px solid #f59e0b',
                borderRadius: '6px',
                padding: '16px',
                margin: '20px 0'
              }}>
                <div style={{ color: '#92400e', fontSize: '14px' }}>
                  {finalResultData.message}
                </div>
                {finalResultData.final_task_name && (
                  <div style={{ color: '#6b7280', fontSize: '12px', marginTop: '4px' }}>
                    Final task: {finalResultData.final_task_name}
                  </div>
                )}
              </div>
            ) : finalResultData?.data ? (
              <div style={{ marginTop: '24px' }}>
                {/* 结果摘要和下载按钮 */}
                <div style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  marginBottom: '16px'
                }}>
                  <label style={styles.formLabel}>Result Summary</label>
                  <button
                    onClick={() => downloadCSV(
                      finalResultData.data,
                      `${selectedWorkflow.name.replace(/[^a-zA-Z0-9]/g, '_')}_${finalResultData.final_task_name}_results.csv`
                    )}
                    style={{
                      ...styles.primaryButton,
                      backgroundColor: '#16a34a',
                      fontSize: '12px',
                      padding: '6px 12px'
                    }}
                  >
                    <Download style={{ width: '14px', height: '14px', marginRight: '4px' }} />
                    Download CSV
                  </button>
                </div>

                <div style={{
                  backgroundColor: '#f0f9ff',
                  border: '1px solid #0ea5e9',
                  borderRadius: '6px',
                  padding: '12px',
                  fontSize: '14px',
                  marginBottom: '16px'
                }}>
                  <div style={{ display: 'flex', gap: '24px', flexWrap: 'wrap' }}>
                    <span><strong>Final Task:</strong> {finalResultData.final_task_name}</span>
                    <span><strong>Rows:</strong> {finalResultData.result_summary?.total_rows || 0}</span>
                    <span><strong>Columns:</strong> {finalResultData.result_summary?.total_columns || 0}</span>
                  </div>
                  {finalResultData.result_summary?.columns && (
                    <div style={{ marginTop: '8px' }}>
                      <strong>Columns:</strong> {finalResultData.result_summary.columns.join(', ')}
                    </div>
                  )}
                </div>

                {/* 数据表格 */}
                <label style={{ ...styles.formLabel, marginBottom: '8px' }}>
                  Final Result Data
                </label>
                <div style={{
                  maxHeight: '400px',
                  overflow: 'auto',
                  border: '1px solid #e2e8f0',
                  borderRadius: '6px'
                }}>
                  <table style={{ width: '100%', fontSize: '12px', borderCollapse: 'collapse' }}>
                    <thead style={{ backgroundColor: '#f8fafc', position: 'sticky', top: 0 }}>
                      <tr>
                        {finalResultData.data.length > 0 && Object.keys(finalResultData.data[0]).map(col => (
                          <th key={col} style={{
                            padding: '8px',
                            textAlign: 'left',
                            borderBottom: '1px solid #e2e8f0',
                            fontWeight: '600'
                          }}>
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {finalResultData.data.slice(0, 100).map((row, index) => (
                        <tr key={index} style={{ borderBottom: '1px solid #f3f4f6' }}>
                          {Object.values(row).map((value, colIndex) => (
                            <td key={colIndex} style={{ padding: '6px 8px' }}>
                              {typeof value === 'boolean' ? (value ? 'true' : 'false') :
                                typeof value === 'object' && value !== null ? JSON.stringify(value) :
                                  String(value)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {finalResultData.data.length > 100 && (
                    <div style={{ padding: '8px', textAlign: 'center', fontSize: '12px', color: '#6b7280' }}>
                      Showing first 100 rows of {finalResultData.data.length} total rows
                    </div>
                  )}
                </div>
              </div>
            ) : null}
          </div>
        )}
      </Modal>
    </div>
  );
};


// Enhanced Network Graph Visualizer - ADT Explorer style
const TwinGraphVisualizer = ({ selectedEnvironment }) => {
  const [graphData, setGraphData] = useState({ nodes: [], relationships: [] });
  const [twins, setTwins] = useState([]);
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);
  const [stats, setStats] = useState(null);

  // UI state
  const [selectedNode, setSelectedNode] = useState(null);
  const [selectedRelationship, setSelectedRelationship] = useState(null);
  const [showCreateTwinForm, setShowCreateTwinForm] = useState(false);
  const [showCreateRelForm, setShowCreateRelForm] = useState(false);
  const [showDetailModal, setShowDetailModal] = useState(false);

  // Form data
  const [twinFormData, setTwinFormData] = useState({
    twin_id: '',
    model_id: '',
    display_name: '',
    properties: '{}'
  });

  const [relFormData, setRelFormData] = useState({
    source_twin_id: '',
    target_twin_id: '',
    relationship_name: '',
    properties: '{}'
  });

  // Refs for D3
  const svgRef = useRef(null);
  const containerRef = useRef(null);
  const simulationRef = useRef(null);

  useEffect(() => {
    if (selectedEnvironment) {
      loadData();
      loadStats();
    }
  }, [selectedEnvironment]);

  useEffect(() => {
    if (containerRef.current) {
      renderNetworkGraph();
    }
  }, [graphData, selectedNode, selectedRelationship]);

  const loadData = async () => {
    try {
      setLoading(true);
      setError(null);

      const [graphResponse, twinsData, modelsData] = await Promise.all([
        DigitalTwinAPI.getTreeGraph(selectedEnvironment.environment_id),
        DigitalTwinAPI.getTwins(selectedEnvironment.environment_id),
        DigitalTwinAPI.getModels(selectedEnvironment.environment_id)
      ]);

      // Convert tree response to network graph format
      const networkData = convertToNetworkGraph(graphResponse || { nodes: [], relationships: [] });
      setGraphData(networkData);
      setTwins(Array.isArray(twinsData) ? twinsData : []);
      setModels(Array.isArray(modelsData) ? modelsData : []);
    } catch (err) {
      setError('Failed to load data: ' + err.message);
    } finally {
      setLoading(false);
    }
  };

  const loadStats = async () => {
    try {
      const statsData = await DigitalTwinAPI.getRelationshipStats(selectedEnvironment.environment_id);
      setStats(statsData);
    } catch (err) {
      console.error('Failed to load statistics:', err);
    }
  };

  const convertToNetworkGraph = (treeData) => {
    // Convert tree nodes to flat network nodes
    const flattenNodes = (nodes) => {
      const result = [];
      const processNode = (node) => {
        result.push({
          id: node.id,
          label: node.label,
          type: node.type,
          metadata: node.metadata
        });
        if (node.children) {
          node.children.forEach(processNode);
        }
      };
      nodes.forEach(processNode);
      return result;
    };

    return {
      nodes: flattenNodes(treeData.nodes),
      relationships: treeData.relationships || []
    };
  };

  const renderNetworkGraph = () => {
    if (!containerRef.current || !graphData.nodes.length) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const containerRect = containerRef.current.getBoundingClientRect();
    const width = containerRect.width;
    const height = containerRect.height;

    svg.attr("width", width).attr("height", height);

    // Create main container with zoom support
    const container = svg.append("g");

    // Create zoom behavior
    const zoom = d3.zoom()
      .scaleExtent([0.1, 4])
      .on("zoom", (event) => {
        container.attr("transform", event.transform);
      });

    svg.call(zoom);

    // Prepare data for D3 force simulation
    const nodes = graphData.nodes.map(d => ({
      ...d,
      x: Math.random() * width,
      y: Math.random() * height
    }));

    const links = graphData.relationships.map(rel => ({
      source: rel.source_twin_id,
      target: rel.target_twin_id,
      relationship: rel
    }));

    // Create force simulation
    const simulation = d3.forceSimulation(nodes)
      .force("link", d3.forceLink(links)
        .id(d => d.id)
        .distance(150)
        .strength(0.5))
      .force("charge", d3.forceManyBody().strength(-800))
      .force("center", d3.forceCenter(width / 2, height / 2))
      .force("collision", d3.forceCollide().radius(35));

    simulationRef.current = simulation;

    // Draw relationship lines
    const linkGroups = container.selectAll(".link")
      .data(links)
      .enter().append("g")
      .attr("class", "link");

    const linkLines = linkGroups.append("line")
      .attr("stroke", d => selectedRelationship?.source_twin_id === d.relationship.source_twin_id && 
                           selectedRelationship?.target_twin_id === d.relationship.target_twin_id &&
                           selectedRelationship?.relationship_name === d.relationship.relationship_name ? "#2563eb" : "#999")
      .attr("stroke-width", d => selectedRelationship?.source_twin_id === d.relationship.source_twin_id && 
                                 selectedRelationship?.target_twin_id === d.relationship.target_twin_id &&
                                 selectedRelationship?.relationship_name === d.relationship.relationship_name ? 3 : 2)
      .attr("marker-end", "url(#arrowhead)")
      .style("cursor", "pointer")
      .on("click", (event, d) => {
        event.stopPropagation();
        setSelectedRelationship(d.relationship);
        setSelectedNode(null);
      });

    // Add arrowhead marker
    svg.append("defs").append("marker")
      .attr("id", "arrowhead")
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 25)
      .attr("refY", 0)
      .attr("markerWidth", 6)
      .attr("markerHeight", 6)
      .attr("orient", "auto")
      .append("path")
      .attr("d", "M0,-5L10,0L0,5")
      .attr("fill", "#999");

    // Add relationship labels
    const linkLabels = linkGroups.append("text")
      .attr("text-anchor", "middle")
      .attr("font-size", "10px")
      .attr("fill", "#666")
      .attr("dy", -5)
      .text(d => d.relationship.relationship_name);

    // Draw twin nodes
    const nodeGroups = container.selectAll(".node")
      .data(nodes)
      .enter().append("g")
      .attr("class", "node")
      .style("cursor", "pointer")
      .call(d3.drag()
        .on("start", (event, d) => {
          if (!event.active) simulation.alphaTarget(0.3).restart();
          d.fx = d.x;
          d.fy = d.y;
        })
        .on("drag", (event, d) => {
          d.fx = event.x;
          d.fy = event.y;
        })
        .on("end", (event, d) => {
          if (!event.active) simulation.alphaTarget(0);
          d.fx = null;
          d.fy = null;
        }))
      .on("click", (event, d) => {
        event.stopPropagation();
        setSelectedNode(d);
        setSelectedRelationship(null);
      });

    // Node circles
    nodeGroups.append("circle")
      .attr("r", 20)
      .attr("fill", d => getNodeColor(d.metadata?.model_id))
      .attr("stroke", d => selectedNode?.id === d.id ? "#2563eb" : "#fff")
      .attr("stroke-width", d => selectedNode?.id === d.id ? 3 : 2);

    // Node labels
    nodeGroups.append("text")
      .attr("dy", 35)
      .attr("text-anchor", "middle")
      .attr("font-size", "12px")
      .attr("font-weight", "500")
      .attr("fill", "#333")
      .text(d => d.label || d.id);

    // Update positions on simulation tick
    simulation.on("tick", () => {
      linkLines
        .attr("x1", d => d.source.x)
        .attr("y1", d => d.source.y)
        .attr("x2", d => d.target.x)
        .attr("y2", d => d.target.y);

      linkLabels
        .attr("x", d => (d.source.x + d.target.x) / 2)
        .attr("y", d => (d.source.y + d.target.y) / 2);

      nodeGroups
        .attr("transform", d => `translate(${d.x},${d.y})`);
    });

    // Clear selection when clicking on empty space
    svg.on("click", () => {
      setSelectedNode(null);
      setSelectedRelationship(null);
    });
  };

  const getNodeColor = (modelId) => {
    const colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4'];
    const modelList = [...new Set(twins.map(t => t.model_id))];
    const index = modelList.indexOf(modelId) % colors.length;
    return colors[index];
  };

  const handleCreateTwin = async () => {
    try {
      setSubmitting(true);

      let properties = {};
      if (twinFormData.properties.trim()) {
        try {
          properties = JSON.parse(twinFormData.properties);
        } catch (err) {
          throw new Error('Property JSON format error: ' + err.message);
        }
      }

      const twinData = {
        twin_id: twinFormData.twin_id,
        model_id: twinFormData.model_id,
        display_name: twinFormData.display_name || twinFormData.twin_id,
        properties: properties
      };

      await DigitalTwinAPI.createTwin(selectedEnvironment.environment_id, twinData);
      setShowCreateTwinForm(false);
      setTwinFormData({ twin_id: '', model_id: '', display_name: '', properties: '{}' });
      await loadData();
      await loadStats();
    } catch (err) {
      alert('Failed to create digital twin: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleCreateRelationship = async () => {
    try {
      // Check if relationship already exists (client-side validation)
      const existingRelationship = graphData.relationships.find(rel => 
        (rel.source_twin_id === relFormData.source_twin_id && rel.target_twin_id === relFormData.target_twin_id) ||
        (rel.source_twin_id === relFormData.target_twin_id && rel.target_twin_id === relFormData.source_twin_id)
      );

      if (existingRelationship) {
        alert(`A relationship already exists between these twins: ${existingRelationship.relationship_name}`);
        return;
      }
      setSubmitting(true);

      let properties = {};
      if (relFormData.properties.trim()) {
        try {
          properties = JSON.parse(relFormData.properties);
        } catch (err) {
          throw new Error('Property JSON format error: ' + err.message);
        }
      }

      const relationshipData = {
        source_twin_id: relFormData.source_twin_id,
        target_twin_id: relFormData.target_twin_id,
        relationship_name: relFormData.relationship_name,
        properties: properties
      };

      await DigitalTwinAPI.createRelationship(selectedEnvironment.environment_id, relationshipData);
      setShowCreateRelForm(false);
      setRelFormData({ source_twin_id: '', target_twin_id: '', relationship_name: '', properties: '{}' });
      await loadData();
      await loadStats();
    } catch (err) {
      alert('Failed to create relationship: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDeleteTwin = async (twinId) => {
    if (window.confirm(`Are you sure you want to delete twin '${twinId}'? This will also delete all its relationships.`)) {
      try {
        await DigitalTwinAPI.deleteTwin(selectedEnvironment.environment_id, twinId);
        setSelectedNode(null);
        await loadData();
        await loadStats();
      } catch (err) {
        alert('Failed to delete twin: ' + err.message);
      }
    }
  };

  const handleDeleteRelationship = async (relationship) => {
    if (window.confirm(`Are you sure you want to delete the relationship '${relationship.relationship_name}' between '${relationship.source_twin_id}' and '${relationship.target_twin_id}'?`)) {
      try {
        await DigitalTwinAPI.deleteRelationship(
          selectedEnvironment.environment_id,
          relationship.source_twin_id,
          relationship.target_twin_id,
          relationship.relationship_name
        );
        setSelectedRelationship(null);
        await loadData();
        await loadStats();
      } catch (err) {
        alert('Failed to delete relationship: ' + err.message);
      }
    }
  };

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <Network style={styles.emptyStateIcon} />
        <p>Please select an environment first</p>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', height: 'calc(100vh - 160px)', minHeight: '600px' }}>
      {/* Left sidebar */}
      <div style={{
        width: '320px',
        minWidth: '320px',
        backgroundColor: '#f8fafc',
        borderRight: '1px solid #e2e8f0',
        display: 'flex',
        flexDirection: 'column'
      }}>
        {/* Header section */}
        <div style={{
          padding: '16px',
          borderBottom: '1px solid #e2e8f0',
          backgroundColor: '#ffffff'
        }}>
          <h2 style={{ margin: '0 0 16px 0', fontSize: '18px', fontWeight: '600' }}>
            Digital Twin Explorer
          </h2>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            <button
              onClick={() => setShowCreateTwinForm(true)}
              style={{
                ...styles.primaryButton,
                width: '100%',
                fontSize: '12px',
                padding: '8px 12px',
                opacity: models.length === 0 ? 0.5 : 1
              }}
              disabled={models.length === 0}
            >
              <Plus style={{ width: '14px', height: '14px', marginRight: '6px' }} />
              Create Twin
            </button>

            <button
              onClick={() => setShowCreateRelForm(true)}
              style={{
                ...styles.primaryButton,
                width: '100%',
                fontSize: '12px',
                padding: '8px 12px',
                backgroundColor: '#16a34a',
                opacity: twins.length < 2 ? 0.5 : 1
              }}
              disabled={twins.length < 2}
            >
              <GitBranch style={{ width: '14px', height: '14px', marginRight: '6px' }} />
              Add Relationship
            </button>
          </div>
        </div>

        {/* Statistics section */}
        {stats && (
          <div style={{
            padding: '16px',
            borderBottom: '1px solid #e2e8f0',
            backgroundColor: '#ffffff'
          }}>
            <h3 style={{ margin: '0 0 12px 0', fontSize: '14px', fontWeight: '600' }}>
              Graph Statistics
            </h3>
            <div style={{ fontSize: '12px', color: '#6b7280' }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                <span>Digital Twins:</span>
                <span style={{ fontWeight: '500', color: '#1f2937' }}>{twins.length}</span>
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                <span>Relationships:</span>
                <span style={{ fontWeight: '500', color: '#1f2937' }}>{stats.total_relationships}</span>
              </div>
              {/* <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                <span>Twins :</span>
                <span style={{ fontWeight: '500', color: '#1f2937' }}>{stats.unique_twins_count}</span>
              </div> */}
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span>Relation Types:</span>
                <span style={{ fontWeight: '500', color: '#1f2937' }}>{stats.relationship_types_count}</span>
              </div>
            </div>
          </div>
        )}

        {/* Selection details panel */}
        <div style={{ flex: 1, overflow: 'auto', padding: '16px' }}>
          {selectedNode ? (
            <div>
              <h3 style={{ margin: '0 0 12px 0', fontSize: '16px' }}>Selected Twin</h3>
              <div style={{
                backgroundColor: '#ffffff',
                padding: '12px',
                borderRadius: '6px',
                boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                border: '2px solid #2563eb'
              }}>
                <p style={{ margin: '0 0 8px 0', fontWeight: '600', color: '#2563eb' }}>{selectedNode.label || selectedNode.id}</p>
                <p style={{ margin: '0 0 8px 0', fontSize: '12px', color: '#6b7280' }}>
                  Model: {selectedNode.metadata?.model_id || 'Unknown'}
                </p>
                <p style={{ margin: '0 0 12px 0', fontSize: '12px', color: '#6b7280' }}>
                  Type: {selectedNode.type}
                </p>

                <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                  <button
                    onClick={() => setShowDetailModal(true)}
                    style={{
                      ...styles.actionButton,
                      ...styles.actionButtonBlue,
                      fontSize: '12px',
                      padding: '6px 8px'
                    }}
                  >
                    <Eye style={{ width: '12px', height: '12px', marginRight: '4px' }} />
                    Details
                  </button>
                  <button
                    onClick={() => handleDeleteTwin(selectedNode.id)}
                    style={{
                      ...styles.actionButton,
                      ...styles.actionButtonRed,
                      fontSize: '12px',
                      padding: '6px 8px'
                    }}
                  >
                    <Trash2 style={{ width: '12px', height: '12px', marginRight: '4px' }} />
                    Delete
                  </button>
                </div>
              </div>
            </div>
          ) : selectedRelationship ? (
            <div>
              <h3 style={{ margin: '0 0 12px 0', fontSize: '16px' }}>Selected Relationship</h3>
              <div style={{
                backgroundColor: '#ffffff',
                padding: '12px',
                borderRadius: '6px',
                boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                border: '2px solid #16a34a'
              }}>
                <p style={{ margin: '0 0 8px 0', fontWeight: '600', color: '#16a34a' }}>
                  {selectedRelationship.relationship_name}
                </p>
                <p style={{ margin: '0 0 8px 0', fontSize: '12px', color: '#6b7280' }}>
                  From: {selectedRelationship.source_twin_id}
                </p>
                <p style={{ margin: '0 0 12px 0', fontSize: '12px', color: '#6b7280' }}>
                  To: {selectedRelationship.target_twin_id}
                </p>

                <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                  <button
                    onClick={() => handleDeleteRelationship(selectedRelationship)}
                    style={{
                      ...styles.actionButton,
                      ...styles.actionButtonRed,
                      fontSize: '12px',
                      padding: '6px 8px'
                    }}
                  >
                    <Trash2 style={{ width: '12px', height: '12px', marginRight: '4px' }} />
                    Delete
                  </button>
                </div>
              </div>
            </div>
          ) : (
            <div style={{ textAlign: 'center', color: '#6b7280', fontSize: '14px' }}>
              <Network style={{ width: '32px', height: '32px', margin: '0 auto 8px' }} />
              <p>Click a twin or relationship to view details</p>
            </div>
          )}

          {/* Network structure display */}
          <div style={{ marginTop: '24px' }}>
            <h3 style={{ margin: '0 0 12px 0', fontSize: '16px' }}>
              Network Elements
            </h3>
            {graphData.nodes.length === 0 ? (
              <p style={{ fontSize: '12px', color: '#6b7280', textAlign: 'center' }}>
                No twins yet. Create twins and relationships to build the network.
              </p>
            ) : (
              <div style={{ maxHeight: '300px', overflow: 'auto' }}>
                <h4 style={{ margin: '0 0 8px 0', fontSize: '14px', color: '#4b5563' }}>
                  Twins ({graphData.nodes.length})
                </h4>
                {graphData.nodes.map((node, index) => (
                  <div key={index} style={{
                    padding: '8px 12px',
                    marginBottom: '4px',
                    borderRadius: '4px',
                    fontSize: '12px',
                    cursor: 'pointer',
                    backgroundColor: selectedNode?.id === node.id ? '#dbeafe' : '#f8fafc',
                    borderColor: selectedNode?.id === node.id ? '#2563eb' : '#e2e8f0',
                    border: '1px solid'
                  }}
                    onClick={() => setSelectedNode(node)}
                  >
                    <div style={{ fontWeight: '500' }}>{node.label || node.id}</div>
                    <div style={{ fontSize: '11px', color: '#6b7280' }}>
                      {node.type} • {node.metadata?.model_id || 'No model'}
                    </div>
                  </div>
                ))}
                
                {graphData.relationships.length > 0 && (
                  <>
                    <h4 style={{ margin: '16px 0 8px 0', fontSize: '14px', color: '#4b5563' }}>
                      Relationships ({graphData.relationships.length})
                    </h4>
                    {graphData.relationships.map((rel, index) => (
                      <div key={index} style={{
                        padding: '8px 12px',
                        marginBottom: '4px',
                        borderRadius: '4px',
                        fontSize: '12px',
                        cursor: 'pointer',
                        backgroundColor: selectedRelationship?.source_twin_id === rel.source_twin_id && 
                                         selectedRelationship?.target_twin_id === rel.target_twin_id &&
                                         selectedRelationship?.relationship_name === rel.relationship_name ? '#dcfce7' : '#f8fafc',
                        borderColor: selectedRelationship?.source_twin_id === rel.source_twin_id && 
                                    selectedRelationship?.target_twin_id === rel.target_twin_id &&
                                    selectedRelationship?.relationship_name === rel.relationship_name ? '#16a34a' : '#e2e8f0',
                        border: '1px solid'
                      }}
                        onClick={() => setSelectedRelationship(rel)}
                      >
                        <div style={{ fontWeight: '500' }}>{rel.relationship_name}</div>
                        <div style={{ fontSize: '11px', color: '#6b7280' }}>
                          {rel.source_twin_id} → {rel.target_twin_id}
                        </div>
                      </div>
                    ))}
                  </>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Main graph area */}
      <div ref={containerRef} style={{ flex: 1, position: 'relative', overflow: 'hidden' }}>
        {error && (
          <div style={{
            ...styles.errorMessage,
            position: 'absolute',
            top: '16px',
            left: '16px',
            right: '16px',
            zIndex: 10
          }}>
            <span style={styles.errorText}>{error}</span>
            <button onClick={() => setError(null)} style={{ ...styles.closeButton, color: '#dc2626' }}>
              <X style={{ width: '16px', height: '16px' }} />
            </button>
          </div>
        )}

        {loading ? (
          <div style={{
            ...styles.loadingContainer,
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)'
          }}>
            <div style={styles.spinner}></div>
            <span style={{ marginLeft: '8px', color: '#6b7280' }}>Loading network...</span>
          </div>
        ) : graphData.nodes.length === 0 ? (
          <div style={{
            ...styles.emptyState,
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)'
          }}>
            <Network style={styles.emptyStateIcon} />
            <p>No digital twins yet. Create your first twin to start building the network.</p>
            {models.length === 0 && (
              <p style={{ color: '#f59e0b', fontSize: '14px', marginTop: '8px' }}>
                Create models first before adding twins
              </p>
            )}
          </div>
        ) : (
          <div style={{ width: '100%', height: '100%', position: 'relative' }}>
            <div style={{
              position: 'absolute',
              top: '10px',
              left: '10px',
              backgroundColor: 'rgba(255, 255, 255, 0.9)',
              padding: '8px 12px',
              borderRadius: '6px',
              fontSize: '12px',
              color: '#6b7280',
              border: '1px solid #e2e8f0'
            }}>
              Drag nodes to move • Click nodes/relationships to select • Use mouse wheel to zoom
            </div>
            <svg ref={svgRef} style={{ width: '100%', height: '100%', display: 'block' }} />
          </div>
        )}
      </div>

      {/* Create twin modal */}
      <Modal
        isOpen={showCreateTwinForm}
        onClose={() => setShowCreateTwinForm(false)}
        title="Create New Digital Twin"
        size="large"
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Twin ID *</label>
            <input
              type="text"
              value={twinFormData.twin_id}
              onChange={(e) => setTwinFormData({ ...twinFormData, twin_id: e.target.value })}
              style={styles.formInput}
              placeholder="e.g. sensor_001, controller_main"
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Display Name</label>
            <input
              type="text"
              value={twinFormData.display_name}
              onChange={(e) => setTwinFormData({ ...twinFormData, display_name: e.target.value })}
              style={styles.formInput}
              placeholder="Human-readable name (optional)"
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Select Model *</label>
            <select
              value={twinFormData.model_id}
              onChange={(e) => setTwinFormData({ ...twinFormData, model_id: e.target.value })}
              style={styles.formInput}
              required
            >
              <option value="">Please select a model</option>
              {models.map(model => (
                <option key={model.model_id} value={model.model_id}>
                  {model.display_name} ({model.model_id})
                </option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Initial Property Values (JSON Format)</label>
            <JsonEditor
              value={twinFormData.properties}
              onChange={(value) => setTwinFormData({ ...twinFormData, properties: value })}
              placeholder={`{
  "temperature": 25.5,
  "status": "online",
  "location": {
    "x": 100,
    "y": 200
  }
}`}
            />
          </div>
          <div style={styles.formActions}>
            <button
              type="button"
              onClick={() => setShowCreateTwinForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleCreateTwin}
              style={{
                ...styles.primaryButton,
                opacity: (submitting || !twinFormData.twin_id || !twinFormData.model_id) ? 0.5 : 1
              }}
              disabled={submitting || !twinFormData.twin_id || !twinFormData.model_id}
            >
              {submitting ? 'Creating...' : 'Create Twin'}
            </button>
          </div>
        </div>
      </Modal>

      {/* Create relationship modal */}
      <Modal
        isOpen={showCreateRelForm}
        onClose={() => setShowCreateRelForm(false)}
        title="Create New Relationship"
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Source Twin *</label>
            <select
              value={relFormData.source_twin_id}
              onChange={(e) => setRelFormData({ ...relFormData, source_twin_id: e.target.value })}
              style={styles.formInput}
              required
            >
              <option value="">Select source twin</option>
              {twins.map(twin => (
                <option key={twin.twin_id} value={twin.twin_id}>
                  {twin.twin_id}
                </option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Target Twin *</label>
            <select
              value={relFormData.target_twin_id}
              onChange={(e) => setRelFormData({ ...relFormData, target_twin_id: e.target.value })}
              style={styles.formInput}
              required
            >
              <option value="">Select target twin</option>
              {twins.filter(t => t.twin_id !== relFormData.source_twin_id).map(twin => (
                <option key={twin.twin_id} value={twin.twin_id}>
                  {twin.twin_id}
                </option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Relationship Name *</label>
            <input
              type="text"
              value={relFormData.relationship_name}
              onChange={(e) => setRelFormData({ ...relFormData, relationship_name: e.target.value })}
              style={styles.formInput}
              placeholder="e.g.: connects_to, contains, controls"
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Relationship Properties (JSON Format)</label>
            <JsonEditor
              value={relFormData.properties}
              onChange={(value) => setRelFormData({ ...relFormData, properties: value })}
              placeholder={`{
  "strength": 0.8,
  "type": "physical",
  "description": "Direct connection"
}`}
            />
          </div>
          <div style={styles.formActions}>
            <button
              type="button"
              onClick={() => setShowCreateRelForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleCreateRelationship}
              style={{
                ...styles.primaryButton,
                opacity: (submitting || !relFormData.source_twin_id || !relFormData.target_twin_id || !relFormData.relationship_name) ? 0.5 : 1
              }}
              disabled={submitting || !relFormData.source_twin_id || !relFormData.target_twin_id || !relFormData.relationship_name}
            >
              {submitting ? 'Creating...' : 'Create Relationship'}
            </button>
          </div>
        </div>
      </Modal>

      {/* Twin details modal */}
      <Modal
        isOpen={showDetailModal}
        onClose={() => setShowDetailModal(false)}
        title="Digital Twin Details"
        size="large"
      >
        {selectedNode && (
          <div>
            <div style={styles.grid2}>
              <div>
                <label style={styles.formLabel}>Twin ID</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px', fontWeight: '500' }}>{selectedNode.id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Display Name</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{selectedNode.label || selectedNode.id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Type</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{selectedNode.type}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Model ID</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{selectedNode.metadata?.model_id || 'Unknown'}</p>
              </div>
            </div>

            {/* Last Updated Time
            {selectedNode.metadata?.telemetry_last_updated && (
              <div style={styles.lastUpdatedBox}>
                <Clock style={{ width: '16px', height: '16px', color: '#0369a1' }} />
                <span style={styles.lastUpdatedText}>
                  Last Updated: {selectedNode.metadata.telemetry_last_updated}
                </span>
              </div>
            )} */}
            {/* Last Updated Time */}
            {selectedNode.metadata?.telemetry_last_updated && (
              <div style={styles.lastUpdatedBox}>
                <Clock style={{ width: '16px', height: '16px', color: '#0369a1' }} />
                <span style={styles.lastUpdatedText}>
                  Last Updated: {(() => {
                    try {
                      const timeString = selectedNode.metadata.telemetry_last_updated;
                      const utcTimeString = timeString.endsWith('Z') ? timeString : timeString + 'Z';
                      const utcDate = new Date(utcTimeString);
                      return utcDate.toLocaleString('en-US', {
                        timeZone: 'Europe/Amsterdam',
                        month: '2-digit',
                        day: '2-digit',
                        hour: 'numeric',
                        minute: '2-digit',
                        hour12: true
                      });
                    } catch (error) {
                      return selectedNode.metadata.telemetry_last_updated;
                    }
                  })()}
                </span>
              </div>
            )}

            {/* Properties */}
            <div style={{ marginTop: '24px' }}>
              <label style={{ ...styles.formLabel, marginBottom: '8px' }}>Properties</label>
              <pre style={styles.preCode}>
                {JSON.stringify(selectedNode.metadata?.properties || {}, null, 2)}
              </pre>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};

// Device-Twin Mapping Manager Component (NEW)
const DeviceTwinMappingManager = ({ selectedEnvironment }) => {
  const [mappings, setMappings] = useState([]);
  const [devices, setDevices] = useState([]);
  const [twins, setTwins] = useState([]);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [error, setError] = useState(null);

  const [formData, setFormData] = useState({
    device_id: '',
    twin_id: '',
    mapping_type: 'direct',
    description: ''
  });

  useEffect(() => {
    if (selectedEnvironment) {
      loadData();
    }
  }, [selectedEnvironment]);

  const loadData = async () => {
    try {
      setLoading(true);
      setError(null);

      const [mappingsData, devicesData, twinsData] = await Promise.all([
        DigitalTwinAPI.getDeviceTwinMappings(selectedEnvironment.environment_id),
        DigitalTwinAPI.getDevices(selectedEnvironment.environment_id),
        DigitalTwinAPI.getTwins(selectedEnvironment.environment_id)
      ]);

      setMappings(Array.isArray(mappingsData) ? mappingsData : []);
      setDevices(Array.isArray(devicesData) ? devicesData : []);
      setTwins(Array.isArray(twinsData) ? twinsData : []);
    } catch (err) {
      setError('Failed to load data: ' + err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = async () => {
    try {
      setSubmitting(true);

      await DigitalTwinAPI.createDeviceTwinMapping(selectedEnvironment.environment_id, formData);
      setShowCreateForm(false);
      setFormData({ device_id: '', twin_id: '', mapping_type: 'direct', description: '' });
      await loadData();
    } catch (err) {
      alert('Failed to create mapping: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (deviceId, twinId) => {
    if (window.confirm('Are you sure you want to delete this mapping?')) {
      try {
        await DigitalTwinAPI.deleteDeviceTwinMapping(selectedEnvironment.environment_id, deviceId, twinId);
        await loadData();
      } catch (err) {
        alert('Failed to delete mapping: ' + err.message);
      }
    }
  };

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <Link style={styles.emptyStateIcon} />
        <p>Please select an environment first</p>
      </div>
    );
  }

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>Device-Twin Mappings</h2>
        <button
          style={styles.primaryButton}
          onClick={() => setShowCreateForm(true)}
          disabled={submitting || devices.length === 0 || twins.length === 0}
        >
          <Link style={styles.buttonIcon} />
          Create Mapping
        </button>
      </div>

      <div style={styles.content}>
        {error && (
          <div style={styles.errorMessage}>
            <span style={styles.errorText}>{error}</span>
            <button onClick={() => setError(null)} style={{ ...styles.closeButton, color: '#dc2626' }}>
              <X style={{ width: '16px', height: '16px' }} />
            </button>
          </div>
        )}

        {(devices.length === 0 || twins.length === 0) && (
          <div style={styles.warningMessage}>
            <div style={styles.warningText}>
              ⚠️ You need both devices and digital twins to create mappings.
              {devices.length === 0 && ' Create devices first.'}
              {twins.length === 0 && ' Create digital twins first.'}
            </div>
          </div>
        )}

        {loading ? (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
            <span style={{ marginLeft: '8px', color: '#6b7280' }}>Loading...</span>
          </div>
        ) : mappings.length === 0 ? (
          <div style={styles.emptyState}>
            <Link style={styles.emptyStateIcon} />
            <p>No device-twin mappings yet. Create mappings to connect physical devices to digital twins.</p>
          </div>
        ) : (
          <div>
            {mappings.map(mapping => (
              <div key={`${mapping.device_id}-${mapping.twin_id}`} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>
                      {mapping.device_id} ↔ {mapping.twin_id}
                    </h3>
                    <div style={{ display: 'flex', gap: '8px', alignItems: 'center', margin: '8px 0' }}>
                      <span style={{ ...styles.badge, ...styles.badgeBlue }}>
                        {mapping.mapping_type}
                      </span>
                    </div>
                    {mapping.description && (
                      <p style={styles.itemDescription}>{mapping.description}</p>
                    )}
                    <p style={styles.itemMeta}>
                      Created: {formatDate(mapping.created_at)}
                    </p>
                  </div>
                  <div style={styles.itemActions}>
                    <button
                      onClick={() => handleDelete(mapping.device_id, mapping.twin_id)}
                      style={{ ...styles.actionButton, ...styles.actionButtonRed }}
                      title="Delete Mapping"
                    >
                      <Unlink style={styles.actionIcon} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Create mapping modal */}
      <Modal
        isOpen={showCreateForm}
        onClose={() => setShowCreateForm(false)}
        title="Create Device-Twin Mapping"
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Device *</label>
            <select
              value={formData.device_id}
              onChange={(e) => setFormData({ ...formData, device_id: e.target.value })}
              style={styles.formInput}
              required
            >
              <option value="">Select a device</option>
              {devices.map(device => (
                <option key={device.device_id} value={device.device_id}>
                  {device.display_name} ({device.device_id})
                </option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Digital Twin *</label>
            <select
              value={formData.twin_id}
              onChange={(e) => setFormData({ ...formData, twin_id: e.target.value })}
              style={styles.formInput}
              required
            >
              <option value="">Select a digital twin</option>
              {twins.map(twin => (
                <option key={twin.twin_id} value={twin.twin_id}>
                  {twin.display_name || twin.twin_id} ({twin.model_id})
                </option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Mapping Type</label>
            <select
              value={formData.mapping_type}
              onChange={(e) => setFormData({ ...formData, mapping_type: e.target.value })}
              style={styles.formInput}
            >
              <option value="direct">Direct</option>
              <option value="indirect">Indirect</option>
              <option value="virtual">Virtual</option>
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Description</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              style={styles.formTextarea}
              rows="3"
              placeholder="Describe the relationship between the device and twin"
            />
          </div>
          <div style={styles.formActions}>
            <button
              type="button"
              onClick={() => setShowCreateForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSubmit}
              style={{
                ...styles.primaryButton,
                opacity: (submitting || !formData.device_id || !formData.twin_id) ? 0.5 : 1
              }}
              disabled={submitting || !formData.device_id || !formData.twin_id}
            >
              {submitting ? 'Creating...' : 'Create Mapping'}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
};

// Main interface component (updated)
const DigitalTwinPlatform = () => {
  // 从localStorage恢复状态，如果没有则使用默认值
  const [activeTab, setActiveTab] = useState(() => {
    return localStorage.getItem('digitalTwin_activeTab') || 'environments';
  });
  const [selectedEnvironment, setSelectedEnvironment] = useState(() => {
    const saved = localStorage.getItem('digitalTwin_selectedEnvironment');
    return saved ? JSON.parse(saved) : null;
  });
  const [environments, setEnvironments] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // 持久化当前选项卡
  useEffect(() => {
    localStorage.setItem('digitalTwin_activeTab', activeTab);
  }, [activeTab]);

  // 持久化选中的环境
  useEffect(() => {
    if (selectedEnvironment) {
      localStorage.setItem('digitalTwin_selectedEnvironment', JSON.stringify(selectedEnvironment));
    } else {
      localStorage.removeItem('digitalTwin_selectedEnvironment');
    }
  }, [selectedEnvironment]);

  useEffect(() => {
    loadEnvironments();
  }, []);

  const loadEnvironments = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await DigitalTwinAPI.getEnvironments();
      const environmentsArray = Array.isArray(data) ? data : [];
      setEnvironments(environmentsArray);
      
      // 如果当前选中的环境不存在了，清除选择
      if (selectedEnvironment && !environmentsArray.find(env => env.environment_id === selectedEnvironment.environment_id)) {
        setSelectedEnvironment(null);
      }
    } catch (err) {
      setError('Failed to load environments: ' + err.message);
      setEnvironments([]);
    } finally {
      setLoading(false);
    }
  };

  // 处理环境选择（从Environment管理页面）
  const handleEnvironmentSelect = (environment) => {
    setSelectedEnvironment(environment);
    // 选择环境后自动跳转到models页面
    setActiveTab('models');
  };

  const tabs = [
    { id: 'environments', label: 'Environments', icon: Building },
    { id: 'models', label: 'Models', icon: Database },
    { id: 'devices', label: 'Devices', icon: Cpu },
    { id: 'mappings', label: 'Device Mappings', icon: Link },
    { id: 'workflows', label: 'Workflows', icon: Activity },
    { id: 'graph', label: 'Tree Explorer', icon: Network },
    { id: 'dashboard', label: 'Analytics', icon: BarChart3 },
  ];

  return (
    <div style={styles.app}>
      {/* Top navigation */}
      <header style={styles.header}>
        <div style={styles.headerContainer}>
          <div style={styles.headerContent}>
            <div style={styles.headerLeft}>
              <Network style={styles.logo} />
              <h1 style={styles.title}>Digital Twin Platform</h1>
            </div>
            <div style={styles.headerRight}>
              {/* 显示当前选中的环境，只有在environments页面或没有选中环境时才显示选择器 */}
              {activeTab === 'environments' || !selectedEnvironment ? (
                <select
                  style={styles.select}
                  value={selectedEnvironment?.environment_id || ''}
                  onChange={(e) => {
                    const env = environments.find(env => env.environment_id === e.target.value);
                    setSelectedEnvironment(env);
                  }}
                >
                  <option value="">Select Environment</option>
                  {environments.map(env => (
                    <option key={env.environment_id} value={env.environment_id}>
                      {env.display_name}
                    </option>
                  ))}
                </select>
              ) : (
                <div style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '12px',
                  fontSize: '14px',
                  color: '#6b7280'
                }}>
                  <span>Environment:</span>
                  <span style={{
                    fontSize: '14px',
                    fontWeight: '600',
                    color: '#2563eb',
                    padding: '6px 12px',
                    backgroundColor: '#dbeafe',
                    borderRadius: '6px'
                  }}>
                    {selectedEnvironment.display_name}
                  </span>
                  <button
                    onClick={() => setSelectedEnvironment(null)}
                    style={{
                      background: 'none',
                      border: 'none',
                      color: '#6b7280',
                      cursor: 'pointer',
                      padding: '4px'
                    }}
                    title="Change Environment"
                  >
                    <Settings style={{width: '16px', height: '16px'}} />
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      </header>

      <div style={styles.mainContainer}>
        {/* Error message */}
        {error && (
          <div style={styles.errorMessage}>
            <div style={styles.errorText}>
              <AlertCircle style={{ width: '20px', height: '20px', marginRight: '8px' }} />
              <span>{error}</span>
            </div>
            <button
              onClick={() => setError(null)}
              style={{ ...styles.closeButton, color: '#dc2626' }}
            >
              <X style={{ width: '20px', height: '20px' }} />
            </button>
          </div>
        )}

        <div style={styles.contentWrapper}>
          {/* Left sidebar */}
          <nav style={styles.sidebar}>
            {tabs.map(tab => {
              const Icon = tab.icon;
              const isActive = activeTab === tab.id;
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  style={{
                    ...styles.navButton,
                    ...(isActive ? styles.navButtonActive : {}),
                  }}
                  onMouseEnter={(e) => {
                    if (!isActive) e.target.style.backgroundColor = '#f3f4f6';
                  }}
                  onMouseLeave={(e) => {
                    if (!isActive) e.target.style.backgroundColor = 'transparent';
                  }}
                >
                  <Icon style={styles.navIcon} />
                  {tab.label}
                </button>
              );
            })}
          </nav>

          {/* Main content area */}
          <main style={styles.mainContent}>
            {activeTab === 'environments' && (
              <EnvironmentManager
                environments={environments}
                onEnvironmentChange={loadEnvironments}
                onEnvironmentSelect={handleEnvironmentSelect}
                loading={loading}
              />
            )}
            {activeTab === 'models' && (
              <ModelManager
                selectedEnvironment={selectedEnvironment}
              />
            )}
            {activeTab === 'devices' && (
              <DeviceManager
                selectedEnvironment={selectedEnvironment}
              />
            )}
            {activeTab === 'mappings' && (
              <DeviceTwinMappingManager
                selectedEnvironment={selectedEnvironment}
              />
            )}
            {activeTab === 'workflows' && (
              <WorkflowManager
                selectedEnvironment={selectedEnvironment}
              />
            )}
            {activeTab === 'graph' && (
              <TwinGraphVisualizer
                selectedEnvironment={selectedEnvironment}
              />
            )}
            {activeTab === 'dashboard' && (  // 添加这个条件
              <GrafanaDashboard
                selectedEnvironment={selectedEnvironment}
              />
            )}
          </main>
        </div>
      </div>
    </div>
  );
};

// Environment management component
const EnvironmentManager = ({ environments, onEnvironmentChange, onEnvironmentSelect, loading }) => {
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [showDetailModal, setShowDetailModal] = useState(false);
  const [selectedEnvironment, setSelectedEnvironment] = useState(null);
  const [editingEnvironment, setEditingEnvironment] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [formData, setFormData] = useState({
    environment_id: '',
    display_name: '',
    description: ''
  });

  const handleSubmit = async () => {
    try {
      setSubmitting(true);
      
      if (editingEnvironment) {
        // 更新环境
        await DigitalTwinAPI.updateEnvironment(editingEnvironment.environment_id, {
          display_name: formData.display_name,
          description: formData.description
        });
      } else {
        // 创建新环境
        await DigitalTwinAPI.createEnvironment(formData);
      }
      
      setShowCreateForm(false);
      setEditingEnvironment(null);
      setFormData({ environment_id: '', display_name: '', description: '' });
      onEnvironmentChange();
    } catch (err) {
      alert('Failed to ' + (editingEnvironment ? 'update' : 'create') + ' environment: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleEdit = (environment, event) => {
    event.stopPropagation(); // 防止触发环境选择
    setEditingEnvironment(environment);
    setFormData({
      environment_id: environment.environment_id,
      display_name: environment.display_name,
      description: environment.description || ''
    });
    setShowCreateForm(true);
  };

  const handleViewDetails = (environment, event) => {
    event.stopPropagation(); // 防止触发环境选择
    setSelectedEnvironment(environment);
    setShowDetailModal(true);
  };

  const handleDelete = async (envId, event) => {
    event.stopPropagation(); // 防止触发环境选择
    if (window.confirm('Are you sure you want to delete this environment? This will delete all related data.')) {
      try {
        await DigitalTwinAPI.deleteEnvironment(envId);
        onEnvironmentChange();
      } catch (err) {
        alert('Failed to delete environment: ' + err.message);
      }
    }
  };

  const handleEnvironmentClick = (environment) => {
    if (onEnvironmentSelect) {
      onEnvironmentSelect(environment);
    }
  };

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>Environment Management</h2>
        <button
          onClick={() => {
            setEditingEnvironment(null);
            setFormData({ environment_id: '', display_name: '', description: '' });
            setShowCreateForm(true);
          }}
          style={styles.primaryButton}
          disabled={submitting}
        >
          <Plus style={styles.buttonIcon} />
          Create Environment
        </button>
      </div>

      <div style={styles.content}>
        {loading ? (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
            <span style={{ marginLeft: '8px', color: '#6b7280' }}>Loading...</span>
          </div>
        ) : environments.length === 0 ? (
          <div style={styles.emptyState}>
            <Building style={styles.emptyStateIcon} />
            <p>No environments yet. Click the button above to create your first environment.</p>
          </div>
        ) : (
          <div>
            {environments.map(env => (
              <div 
                key={env.environment_id} 
                style={{
                  ...styles.listItem,
                  cursor: 'pointer',
                  transition: 'all 0.2s'
                }}
                onClick={() => handleEnvironmentClick(env)}
                onMouseEnter={(e) => {
                  e.currentTarget.style.backgroundColor = '#f8fafc';
                  e.currentTarget.style.borderColor = '#2563eb';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.backgroundColor = '#ffffff';
                  e.currentTarget.style.borderColor = '#e2e8f0';
                }}
              >
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{env.display_name}</h3>
                    <p style={styles.itemSubtitle}>ID: {env.environment_id}</p>
                    {env.description && (
                      <p style={styles.itemDescription}>{env.description}</p>
                    )}
                    <p style={styles.itemMeta}>
                      Created: {formatDate(env.created_at)}
                    </p>
                    <p style={{ ...styles.itemMeta, color: '#2563eb', fontSize: '12px' }}>
                      Click to select and continue →
                    </p>
                  </div>
                  <div style={styles.itemActions}>
                    <button
                      onClick={(event) => handleViewDetails(env, event)}
                      style={{ ...styles.actionButton, ...styles.actionButtonBlue }}
                      title="View Details"
                    >
                      <Eye style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={(event) => handleEdit(env, event)}
                      style={{ ...styles.actionButton, ...styles.actionButtonGray }}
                      title="Edit"
                    >
                      <Edit style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={(event) => handleDelete(env.environment_id, event)}
                      style={{ ...styles.actionButton, ...styles.actionButtonRed }}
                      title="Delete Environment"
                    >
                      <Trash2 style={styles.actionIcon} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Create/Edit environment modal */}
      <Modal
        isOpen={showCreateForm}
        onClose={() => setShowCreateForm(false)}
        title={editingEnvironment ? "Edit Environment" : "Create New Environment"}
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Environment ID *</label>
            <input
              type="text"
              value={formData.environment_id}
              onChange={(e) => setFormData({ ...formData, environment_id: e.target.value })}
              style={styles.formInput}
              placeholder="Only letters, numbers, hyphens and underscores allowed"
              disabled={editingEnvironment}
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Display Name *</label>
            <input
              type="text"
              value={formData.display_name}
              onChange={(e) => setFormData({ ...formData, display_name: e.target.value })}
              style={styles.formInput}
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Description</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              style={styles.formTextarea}
              rows="3"
              placeholder="Detailed description of the environment (optional)"
            />
          </div>
          <div style={styles.formActions}>
            <button
              type="button"
              onClick={() => setShowCreateForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSubmit}
              style={{
                ...styles.primaryButton,
                opacity: (submitting || !formData.environment_id || !formData.display_name) ? 0.5 : 1
              }}
              disabled={submitting || !formData.environment_id || !formData.display_name}
            >
              {submitting ? (editingEnvironment ? 'Updating...' : 'Creating...') : (editingEnvironment ? 'Update' : 'Create')}
            </button>
          </div>
        </div>
      </Modal>

      {/* Environment details modal */}
      <Modal
        isOpen={showDetailModal}
        onClose={() => setShowDetailModal(false)}
        title="Environment Details"
        size="large"
      >
        {selectedEnvironment && (
          <div>
            <div style={styles.grid2}>
              <div>
                <label style={styles.formLabel}>Environment ID</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px', fontWeight: '500' }}>
                  {selectedEnvironment.environment_id}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Display Name</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {selectedEnvironment.display_name}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Created</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {formatDate(selectedEnvironment.created_at)}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Updated</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {formatDate(selectedEnvironment.updated_at)}
                </p>
              </div>
            </div>

            {selectedEnvironment.description && (
              <div style={{ margin: '24px 0' }}>
                <label style={styles.formLabel}>Description</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {selectedEnvironment.description}
                </p>
              </div>
            )}
          </div>
        )}
      </Modal>
    </div>
  );
};

// Model management component
const ModelManager = ({ selectedEnvironment }) => {
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [showDetailModal, setShowDetailModal] = useState(false);
  const [selectedModel, setSelectedModel] = useState(null);
  const [editingModel, setEditingModel] = useState(null);
  const [error, setError] = useState(null);

  const [formData, setFormData] = useState({
    model_id: '',
    display_name: '',
    description: '',
    properties: '{}'
  });

  useEffect(() => {
    if (selectedEnvironment) {
      loadModels();
    }
  }, [selectedEnvironment]);

  const loadModels = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await DigitalTwinAPI.getModels(selectedEnvironment.environment_id);
      setModels(Array.isArray(data) ? data : []);
    } catch (err) {
      setError('Failed to load models: ' + err.message);
      setModels([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = async () => {
    try {
      setSubmitting(true);

      if (editingModel) {
        // 更新模型时，不包含properties字段
        await DigitalTwinAPI.updateModel(selectedEnvironment.environment_id, editingModel.model_id, {
          display_name: formData.display_name,
          description: formData.description
        });
      } else {
        // 创建新模型时，需要解析properties
        let properties = {};
        if (formData.properties.trim()) {
          try {
            properties = JSON.parse(formData.properties);
          } catch (err) {
            throw new Error('Property definition JSON format error: ' + err.message);
          }
        }

        const modelData = {
          model_id: formData.model_id,
          display_name: formData.display_name,
          description: formData.description,
          properties: properties
        };

        await DigitalTwinAPI.createModel(selectedEnvironment.environment_id, modelData);
      }

      setShowCreateForm(false);
      setEditingModel(null);
      setFormData({ model_id: '', display_name: '', description: '', properties: '{}' });
      loadModels();
    } catch (err) {
      alert('Failed to ' + (editingModel ? 'update' : 'create') + ' model: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleEdit = (model) => {
    setEditingModel(model);
    setFormData({
      model_id: model.model_id,
      display_name: model.display_name,
      description: model.description || '',
      properties: JSON.stringify(model.properties || {}, null, 2)
    });
    setShowCreateForm(true);
  };

  const handleDelete = async (modelId) => {
    if (window.confirm('Are you sure you want to delete this model?')) {
      try {
        await DigitalTwinAPI.deleteModel(selectedEnvironment.environment_id, modelId);
        loadModels();
      } catch (err) {
        alert('Failed to delete model: ' + err.message);
      }
    }
  };

  const handleViewDetails = async (model) => {
    try {
      const fullModel = await DigitalTwinAPI.getModel(selectedEnvironment.environment_id, model.model_id);
      setSelectedModel(fullModel);
      setShowDetailModal(true);
    } catch (err) {
      alert('Failed to get model details: ' + err.message);
    }
  };

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <Database style={styles.emptyStateIcon} />
        <p>Please select an environment first</p>
      </div>
    );
  }

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>Model Management</h2>
        <button
          style={styles.primaryButton}
          onClick={() => {
            setEditingModel(null);
            setFormData({ model_id: '', display_name: '', description: '', properties: '{}' });
            setShowCreateForm(true);
          }}
          disabled={submitting}
        >
          <Plus style={styles.buttonIcon} />
          Create Model
        </button>
      </div>

      <div style={styles.content}>
        {error && (
          <div style={styles.errorMessage}>
            <span style={styles.errorText}>{error}</span>
            <button onClick={() => setError(null)} style={{ ...styles.closeButton, color: '#dc2626' }}>
              <X style={{ width: '16px', height: '16px' }} />
            </button>
          </div>
        )}

        {loading ? (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
            <span style={{ marginLeft: '8px', color: '#6b7280' }}>Loading...</span>
          </div>
        ) : models.length === 0 ? (
          <div style={styles.emptyState}>
            <Database style={styles.emptyStateIcon} />
            <p>No models yet. Click the button above to create your first model.</p>
          </div>
        ) : (
          <div>
            {models.map(model => (
              <div key={model.model_id} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{model.display_name}</h3>
                    <p style={styles.itemSubtitle}>ID: {model.model_id}</p>
                    {model.description && (
                      <p style={styles.itemDescription}>{model.description}</p>
                    )}
                    <p style={styles.itemMeta}>
                      Created: {formatDate(model.created_at)}
                    </p>
                    <p style={styles.itemMeta}>
                      Properties: {Object.keys(model.properties || {}).length}
                    </p>
                  </div>
                  <div style={styles.itemActions}>
                    <button
                      onClick={() => handleViewDetails(model)}
                      style={{ ...styles.actionButton, ...styles.actionButtonBlue }}
                      title="View Details"
                    >
                      <Eye style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleEdit(model)}
                      style={{ ...styles.actionButton, ...styles.actionButtonGray }}
                      title="Edit"
                    >
                      <Edit style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleDelete(model.model_id)}
                      style={{ ...styles.actionButton, ...styles.actionButtonRed }}
                      title="Delete"
                    >
                      <Trash2 style={styles.actionIcon} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Create/Edit model modal */}
      <Modal
        isOpen={showCreateForm}
        onClose={() => setShowCreateForm(false)}
        title={editingModel ? "Edit Model" : "Create New Model"}
        size="large"
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Model ID *</label>
            <input
              type="text"
              value={formData.model_id}
              onChange={(e) => setFormData({ ...formData, model_id: e.target.value })}
              style={styles.formInput}
              disabled={editingModel}
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Display Name *</label>
            <input
              type="text"
              value={formData.display_name}
              onChange={(e) => setFormData({ ...formData, display_name: e.target.value })}
              style={styles.formInput}
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Description</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              style={styles.formTextarea}
              rows="3"
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>
              Property Definition (JSON Format)
              {editingModel && <span style={{ color: '#6b7280', fontSize: '12px', marginLeft: '8px' }}>- Read Only</span>}
            </label>
            {editingModel ? (
              // 编辑模式下显示只读的预览
              <pre style={{
                ...styles.preCode,
                backgroundColor: '#f9fafb',
                border: '1px solid #e5e7eb',
                color: '#6b7280'
              }}>
                {formData.properties}
              </pre>
            ) : (
              // 创建模式下显示可编辑的JsonEditor
              <JsonEditor
                value={formData.properties}
                onChange={(value) => setFormData({ ...formData, properties: value })}
                placeholder={`{
  "temperature": {
    "type": "number",
    "unit": "°C",
    "description": "Temperature sensor reading",
    "is_required": true,
    "default_value": 20.0
  },
  "status": {
    "type": "string",
    "description": "Device status",
    "is_required": false,
    "default_value": "unknown"
  }
}`}
              />
            )}
            {editingModel && (
              <p style={{ fontSize: '12px', color: '#f59e0b', marginTop: '8px' }}>
                Properties cannot be modified after model creation. Create a new model if you need different properties.
              </p>
            )}
          </div>
          <div style={styles.formActions}>
            <button
              type="button"
              onClick={() => setShowCreateForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSubmit}
              style={{
                ...styles.primaryButton,
                opacity: (submitting || !formData.model_id || !formData.display_name) ? 0.5 : 1
              }}
              disabled={submitting || !formData.model_id || !formData.display_name}
            >
              {submitting ? (editingModel ? 'Updating...' : 'Creating...') : (editingModel ? 'Update' : 'Create')}
            </button>
          </div>
        </div>
      </Modal>

      {/* Model details modal */}
      <Modal
        isOpen={showDetailModal}
        onClose={() => setShowDetailModal(false)}
        title="Model Details"
        size="large"
      >
        {selectedModel && (
          <div>
            <div style={styles.grid2}>
              <div>
                <label style={styles.formLabel}>Model ID</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{selectedModel.model_id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Display Name</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{selectedModel.display_name}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Created</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{formatDate(selectedModel.created_at)}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Updated</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{formatDate(selectedModel.updated_at)}</p>
              </div>
            </div>

            {selectedModel.description && (
              <div style={{ margin: '24px 0' }}>
                <label style={styles.formLabel}>Description</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{selectedModel.description}</p>
              </div>
            )}

            <div>
              <label style={{ ...styles.formLabel, marginBottom: '8px' }}>Property Definition</label>
              <pre style={styles.preCode}>
                {JSON.stringify(selectedModel.properties || {}, null, 2)}
              </pre>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};

// Device management component (updated - simplified, no properties)
const DeviceManager = ({ selectedEnvironment }) => {
  const [devices, setDevices] = useState([]);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [showDetailModal, setShowDetailModal] = useState(false);
  const [selectedDevice, setSelectedDevice] = useState(null);
  const [editingDevice, setEditingDevice] = useState(null);
  const [error, setError] = useState(null);

  const [formData, setFormData] = useState({
    device_id: '',
    display_name: '',
    description: '',
    device_type: '',
    location: '',
    manufacturer: '',
    model_number: ''
  });

  useEffect(() => {
    if (selectedEnvironment) {
      loadDevices();
    }
  }, [selectedEnvironment]);

  const loadDevices = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await DigitalTwinAPI.getDevices(selectedEnvironment.environment_id);
      setDevices(Array.isArray(data) ? data : []);
    } catch (err) {
      setError('Failed to load devices: ' + err.message);
      setDevices([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = async () => {
    try {
      setSubmitting(true);

      if (editingDevice) {
        await DigitalTwinAPI.updateDevice(selectedEnvironment.environment_id, editingDevice.device_id, {
          display_name: formData.display_name,
          description: formData.description,
          device_type: formData.device_type,
          location: formData.location,
          manufacturer: formData.manufacturer,
          model_number: formData.model_number
        });
      } else {
        await DigitalTwinAPI.createDevice(selectedEnvironment.environment_id, formData);
      }

      setShowCreateForm(false);
      setEditingDevice(null);
      setFormData({ device_id: '', display_name: '', description: '', device_type: '', location: '', manufacturer: '', model_number: '' });
      loadDevices();
    } catch (err) {
      alert('Failed to ' + (editingDevice ? 'update' : 'register') + ' device: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleEdit = (device) => {
    setEditingDevice(device);
    setFormData({
      device_id: device.device_id,
      display_name: device.display_name,
      description: device.description || '',
      device_type: device.device_type || '',
      location: device.location || '',
      manufacturer: device.manufacturer || '',
      model_number: device.model_number || ''
    });
    setShowCreateForm(true);
  };

  const handleDelete = async (deviceId) => {
    if (window.confirm('Are you sure you want to delete this device?')) {
      try {
        await DigitalTwinAPI.deleteDevice(selectedEnvironment.environment_id, deviceId);
        loadDevices();
      } catch (err) {
        alert('Failed to delete device: ' + err.message);
      }
    }
  };

  const handleViewDetails = async (device) => {
    try {
      const fullDevice = await DigitalTwinAPI.getDevice(selectedEnvironment.environment_id, device.device_id);
      setSelectedDevice(fullDevice);
      setShowDetailModal(true);
    } catch (err) {
      alert('Failed to get device details: ' + err.message);
    }
  };

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <Cpu style={styles.emptyStateIcon} />
        <p>Please select an environment first</p>
      </div>
    );
  }

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>Device Management</h2>
        <button
          style={styles.primaryButton}
          onClick={() => {
            setEditingDevice(null);
            setFormData({ device_id: '', display_name: '', description: '', device_type: '', location: '', manufacturer: '', model_number: '' });
            setShowCreateForm(true);
          }}
          disabled={submitting}
        >
          <Plus style={styles.buttonIcon} />
          Register Device
        </button>
      </div>

      <div style={styles.content}>
        {error && (
          <div style={styles.errorMessage}>
            <span style={styles.errorText}>{error}</span>
            <button onClick={() => setError(null)} style={{ ...styles.closeButton, color: '#dc2626' }}>
              <X style={{ width: '16px', height: '16px' }} />
            </button>
          </div>
        )}

        {loading ? (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
            <span style={{ marginLeft: '8px', color: '#6b7280' }}>Loading...</span>
          </div>
        ) : devices.length === 0 ? (
          <div style={styles.emptyState}>
            <Cpu style={styles.emptyStateIcon} />
            <p>No devices yet. Click the button above to register your first device.</p>
          </div>
        ) : (
          <div>
            {devices.map(device => (
              <div key={device.device_id} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{device.display_name}</h3>
                    <p style={styles.itemSubtitle}>ID: {device.device_id}</p>
                    <div style={{ display: 'flex', gap: '8px', alignItems: 'center', margin: '8px 0' }}>
                      {device.device_type && (
                        <span style={{ ...styles.badge, ...styles.badgeGray }}>
                          {device.device_type}
                        </span>
                      )}
                      {device.location && (
                        <span style={{ ...styles.badge, ...styles.badgeBlue }}>
                          📍 {device.location}
                        </span>
                      )}
                    </div>
                    {device.description && (
                      <p style={styles.itemDescription}>{device.description}</p>
                    )}
                    <p style={styles.itemMeta}>
                      Created: {formatDate(device.created_at)}
                    </p>
                    {device.manufacturer && (
                      <p style={styles.itemMeta}>
                        Manufacturer: {device.manufacturer}
                        {device.model_number && ` (${device.model_number})`}
                      </p>
                    )}
                  </div>
                  <div style={styles.itemActions}>
                    <button
                      onClick={() => handleViewDetails(device)}
                      style={{ ...styles.actionButton, ...styles.actionButtonBlue }}
                      title="View Details"
                    >
                      <Eye style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleEdit(device)}
                      style={{ ...styles.actionButton, ...styles.actionButtonGray }}
                      title="Edit"
                    >
                      <Edit style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleDelete(device.device_id)}
                      style={{ ...styles.actionButton, ...styles.actionButtonRed }}
                      title="Delete"
                    >
                      <Trash2 style={styles.actionIcon} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Create/Edit device modal */}
      <Modal
        isOpen={showCreateForm}
        onClose={() => setShowCreateForm(false)}
        title={editingDevice ? "Edit Device" : "Register New Device"}
        size="large"
      >
        <div>
          <div style={styles.grid2}>
            <div style={styles.formGroup}>
              <label style={styles.formLabel}>Device ID *</label>
              <input
                type="text"
                value={formData.device_id}
                onChange={(e) => setFormData({ ...formData, device_id: e.target.value })}
                style={styles.formInput}
                placeholder="e.g. sensor_001, pump_main"
                disabled={editingDevice}
                required
              />
            </div>
            <div style={styles.formGroup}>
              <label style={styles.formLabel}>Device Name *</label>
              <input
                type="text"
                value={formData.display_name}
                onChange={(e) => setFormData({ ...formData, display_name: e.target.value })}
                style={styles.formInput}
                placeholder="Human-readable device name"
                required
              />
            </div>
          </div>

          <div style={styles.grid2}>
            <div style={styles.formGroup}>
              <label style={styles.formLabel}>Device Type</label>
              <select
                value={formData.device_type}
                onChange={(e) => setFormData({ ...formData, device_type: e.target.value })}
                style={styles.formInput}
              >
                <option value="">Select type</option>
                <option value="sensor">Sensor</option>
                <option value="actuator">Actuator</option>
                <option value="controller">Controller</option>
                <option value="gateway">Gateway</option>
                <option value="camera">Camera</option>
                <option value="other">Other</option>
              </select>
            </div>
            <div style={styles.formGroup}>
              <label style={styles.formLabel}>Location</label>
              <input
                type="text"
                value={formData.location}
                onChange={(e) => setFormData({ ...formData, location: e.target.value })}
                style={styles.formInput}
                placeholder="e.g. Building A, Floor 2, Room 201"
              />
            </div>
          </div>

          <div style={styles.grid2}>
            <div style={styles.formGroup}>
              <label style={styles.formLabel}>Manufacturer</label>
              <input
                type="text"
                value={formData.manufacturer}
                onChange={(e) => setFormData({ ...formData, manufacturer: e.target.value })}
                style={styles.formInput}
                placeholder="e.g. Siemens, Honeywell"
              />
            </div>
          </div>

          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Description</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({ ...formData, description: e.target.value })}
              style={styles.formTextarea}
              rows="3"
              placeholder="Device description, specifications, notes (optional)"
            />
          </div>

          <div style={styles.formActions}>
            <button
              type="button"
              onClick={() => setShowCreateForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSubmit}
              style={{
                ...styles.primaryButton,
                opacity: (submitting || !formData.device_id || !formData.display_name) ? 0.5 : 1
              }}
              disabled={submitting || !formData.device_id || !formData.display_name}
            >
              {submitting ? (editingDevice ? 'Updating...' : 'Registering...') : (editingDevice ? 'Update' : 'Register')}
            </button>
          </div>
        </div>
      </Modal>

      {/* Device details modal */}
      <Modal
        isOpen={showDetailModal}
        onClose={() => setShowDetailModal(false)}
        title="Device Details"
        size="large"
      >
        {selectedDevice && (
          <div>
            <div style={styles.grid3}>
              <div>
                <label style={styles.formLabel}>Device ID</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px', fontWeight: '500' }}>{selectedDevice.device_id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Device Name</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{selectedDevice.display_name}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Device Type</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {selectedDevice.device_type || 'Not specified'}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Location</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {selectedDevice.location || 'Not specified'}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Manufacturer</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {selectedDevice.manufacturer || 'Not specified'}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Created</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{formatDate(selectedDevice.created_at)}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Updated</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{formatDate(selectedDevice.updated_at)}</p>
              </div>
              {/* <div>
                <label style={styles.formLabel}>Last Seen</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                  {selectedDevice.last_seen ? formatDate(selectedDevice.last_seen) : 'Never'}
                </p>
              </div> */}
            </div>

            {selectedDevice.description && (
              <div style={{ margin: '24px 0' }}>
                <label style={styles.formLabel}>Description</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{selectedDevice.description}</p>
              </div>
            )}

            {/* <div style={{ marginTop: '24px' }}>
              <label style={styles.formLabel}>Telemetry Statistics</label>
              <div style={{
                backgroundColor: '#f8fafc',
                padding: '12px',
                borderRadius: '6px',
                fontSize: '14px'
              }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                  <span>Total Data Points:</span>
                  <span style={{ fontWeight: '500' }}>{selectedDevice.telemetry_points_count || 0}</span>
                </div>
              </div>
            </div> */}
          </div>
        )}
      </Modal>
    </div>
  );
};

export default DigitalTwinPlatform;