import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
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
  BarChart3,
  RefreshCw
} from 'lucide-react';
import * as d3 from 'd3';

// API base configuration
const API_BASE_URL = 'http://localhost:8000';
const WS_BASE_URL = 'ws://localhost:8001';

// Style definitions
const styles = {
  app: {
    minHeight: '100vh',
    backgroundColor: '#f8fafc',
    fontFamily: 'system-ui, -apple-system, sans-serif'
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
    height: '160px',
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
  },
  mappingCard: {
    backgroundColor: '#f0f9ff',
    border: '1px solid #0ea5e9',
    borderRadius: '6px',
    padding: '12px',
    margin: '8px 0'
  },
  treeNodeItem: {
    backgroundColor: '#ffffff',
    border: '1px solid #e2e8f0',
    borderRadius: '4px',
    padding: '8px 12px',
    margin: '4px 0',
    fontSize: '12px',
    cursor: 'pointer'
  },
  lastUpdatedBox: {
    backgroundColor: '#f0f9ff',
    border: '1px solid #0ea5e9',
    borderRadius: '6px',
    padding: '12px',
    marginTop: '16px',
    display: 'flex',
    alignItems: 'center',
    gap: '8px'
  },
  lastUpdatedText: {
    fontSize: '14px',
    color: '#0369a1',
    fontWeight: '500'
  },
  // New styles for the dashboard and stats
  statsGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
    gap: '16px',
    marginBottom: '24px'
  },
  statCard: {
    backgroundColor: '#f8fafc',
    border: '1px solid #e2e8f0',
    borderRadius: '8px',
    padding: '16px'
  },
  statLabel: {
    fontSize: '14px',
    color: '#6b7280',
    marginBottom: '4px'
  },
  statValue: {
    fontSize: '24px',
    fontWeight: '600',
    color: '#1f2937'
  },
  statSubtext: {
    fontSize: '12px',
    color: '#9ca3af'
  },
  // Styles for the visualizer
  visualizerContainer: {
    height: '800px',
    width: '100%',
    border: '1px solid #e2e8f0',
    borderRadius: '8px',
    position: 'relative',
    overflow: 'hidden'
  },
  visualizerControls: {
    position: 'absolute',
    top: '16px',
    right: '16px',
    backgroundColor: 'rgba(255,255,255,0.8)',
    borderRadius: '8px',
    padding: '8px',
    boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
  },
  // Styles for relationship stats
  relationshipStatsGrid: {
    display: 'grid',
    gridTemplateColumns: '1fr 1fr',
    gap: '16px'
  },
  relationshipStatsCard: {
    backgroundColor: '#f8fafc',
    border: '1px solid #e2e8f0',
    borderRadius: '8px',
    padding: '16px'
  },
  relationshipStatsList: {
    listStyleType: 'none',
    padding: 0,
    margin: 0
  },
  relationshipStatsItem: {
    display: 'flex',
    justifyContent: 'space-between',
    padding: '4px 0',
    borderBottom: '1px solid #e2e8f0'
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

// Date formatting helper function
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

    return date.toLocaleString('en-US', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    });
  } catch (error) {
    console.error('Date formatting error:', error);
    return 'Parse Error';
  }
};

// Workflow API service class
class WorkflowAPI {
  static async request(endpoint, options = {}) {
    try {
      const response = await fetch(`${WS_BASE_URL}${endpoint}`, {
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

  static async getTaskResult(environmentId, workflowId, taskName) {
    return this.request(`/environments/${environmentId}/workflows/${workflowId}/tasks/${taskName}/result`);
  }

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
            <X style={{ width: '24px', height: '24px' }} />
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
      <textarea value={value} onChange={handleChange} style={{ ...styles.jsonEditor, borderColor: isValid ? '#d1d5db' : '#dc2626' }} placeholder={placeholder} />
      {!isValid && (
        <div style={styles.jsonError}>
          <AlertCircle style={{ width: '16px', height: '16px', marginRight: '4px' }} /> JSON format error: {error}
        </div>
      )}
    </div>
  );
};

// Dashboard component
const Dashboard = ({ selectedEnvironment }) => {
  const [stats, setStats] = useState({});
  const [relationships, setRelationships] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (selectedEnvironment) {
      fetchStats();
      fetchRelationships();
    }
  }, [selectedEnvironment]);

  const fetchStats = async () => {
    setLoading(true);
    setError(null);
    try {
      const models = await DigitalTwinAPI.getModels(selectedEnvironment.environment_id);
      const twins = await DigitalTwinAPI.getTwins(selectedEnvironment.environment_id);
      const devices = await DigitalTwinAPI.getDevices(selectedEnvironment.environment_id);
      const mappings = await DigitalTwinAPI.getDeviceTwinMappings(selectedEnvironment.environment_id);

      setStats({
        models: models.length,
        twins: twins.length,
        devices: devices.length,
        mappings: mappings.length
      });
    } catch (err) {
      setError('Failed to fetch stats: ' + err.message);
    } finally {
      setLoading(false);
    }
  };

  const fetchRelationships = async () => {
    try {
      const rels = await DigitalTwinAPI.getRelationships(selectedEnvironment.environment_id);
      setRelationships(rels);
    } catch (err) {
      console.error('Failed to fetch relationships:', err);
    }
  };

  const renderContent = () => {
    if (loading) {
      return (
        <div style={styles.loadingContainer}>
          <div style={styles.spinner}></div>
        </div>
      );
    }

    if (error) {
      return (
        <div style={styles.errorMessage}>
          <div style={styles.errorText}>
            <AlertCircle style={{ marginRight: '8px' }} />
            {error}
          </div>
        </div>
      );
    }

    return (
      <>
        <div style={styles.statsGrid}>
          <div style={styles.statCard}>
            <div style={styles.statLabel}>Models</div>
            <div style={styles.statValue}>{stats.models}</div>
          </div>
          <div style={styles.statCard}>
            <div style={styles.statLabel}>Digital Twins</div>
            <div style={styles.statValue}>{stats.twins}</div>
          </div>
          <div style={styles.statCard}>
            <div style={styles.statLabel}>Devices</div>
            <div style={styles.statValue}>{stats.devices}</div>
          </div>
          <div style={styles.statCard}>
            <div style={styles.statLabel}>Mappings</div>
            <div style={styles.statValue}>{stats.mappings}</div>
          </div>
        </div>
        <div style={styles.content}>
          <h3 style={{ fontSize: '18px', fontWeight: '600', marginBottom: '16px' }}>Recent Relationships</h3>
          {relationships.length > 0 ? (
            relationships.slice(0, 5).map((rel, index) => (
              <div key={index} style={styles.listItem}>
                <p style={styles.itemTitle}>{rel.relationship_name}</p>
                <p style={styles.itemSubtitle}>
                  From: {rel.source_twin_id} &rarr; To: {rel.target_twin_id}
                </p>
                <p style={styles.itemMeta}>Created: {formatDate(rel.created_at)}</p>
              </div>
            ))
          ) : (
            <div style={styles.emptyState}>No relationships found.</div>
          )}
        </div>
      </>
    );
  };

  return (
    <div style={styles.mainContent}>
      <div style={styles.contentHeader}>
        <h1 style={styles.contentTitle}>
          <BarChart3 style={{ marginRight: '12px' }} />
          Dashboard
        </h1>
      </div>
      <div style={styles.content}>
        {renderContent()}
      </div>
    </div>
  );
};

// Environment Management component
const EnvironmentManagement = ({ selectedEnvironment, setSelectedEnvironment, environments, fetchEnvironments }) => {
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [formData, setFormData] = useState({
    environment_id: '',
    display_name: ''
  });
  const [submitting, setSubmitting] = useState(false);

  const handleCreateEnvironment = async () => {
    try {
      setSubmitting(true);
      await DigitalTwinAPI.createEnvironment(formData);
      setIsModalOpen(false);
      await fetchEnvironments();
    } catch (error) {
      alert(`Failed to create environment: ${error.message}`);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDeleteEnvironment = async (envId) => {
    if (window.confirm('Are you sure you want to delete this environment and all its contents?')) {
      try {
        await DigitalTwinAPI.deleteEnvironment(envId);
        await fetchEnvironments();
        if (selectedEnvironment && selectedEnvironment.environment_id === envId) {
          setSelectedEnvironment(null);
        }
      } catch (error) {
        alert(`Failed to delete environment: ${error.message}`);
      }
    }
  };

  return (
    <div style={styles.mainContent}>
      <div style={styles.contentHeader}>
        <h1 style={styles.contentTitle}>
          <Building style={{ marginRight: '12px' }} />
          Environment Management
        </h1>
        <button style={styles.primaryButton} onClick={() => { setFormData({ environment_id: '', display_name: '' }); setIsModalOpen(true); }}>
          <Plus style={styles.buttonIcon} />
          Create Environment
        </button>
      </div>
      <div style={styles.content}>
        {environments.length === 0 ? (
          <div style={styles.emptyState}>
            <Building style={styles.emptyStateIcon} />
            <p>No environments found. Start by creating a new one.</p>
          </div>
        ) : (
          <div>
            {environments.map(env => (
              <div key={env.environment_id} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{env.display_name}</h3>
                    <p style={styles.itemSubtitle}>ID: {env.environment_id}</p>
                    <p style={styles.itemMeta}>Created: {formatDate(env.created_at)}</p>
                  </div>
                  <div style={styles.itemActions}>
                    <button onClick={() => handleDeleteEnvironment(env.environment_id)} style={{ ...styles.actionButton, ...styles.actionButtonRed }}>
                      <Trash2 style={styles.actionIcon} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
      <Modal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} title="Create New Environment">
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Environment ID *</label>
            <input type="text" value={formData.environment_id} onChange={(e) => setFormData({ ...formData, environment_id: e.target.value })} style={styles.formInput} required />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Display Name</label>
            <input type="text" value={formData.display_name} onChange={(e) => setFormData({ ...formData, display_name: e.target.value })} style={styles.formInput} />
          </div>
          <div style={styles.formActions}>
            <button onClick={() => setIsModalOpen(false)} style={styles.cancelButton}>Cancel</button>
            <button onClick={handleCreateEnvironment} style={styles.primaryButton} disabled={submitting || !formData.environment_id}>
              {submitting ? 'Creating...' : 'Create Environment'}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
};

// Model Management component
const ModelManagement = ({ selectedEnvironment }) => {
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [formData, setFormData] = useState({ model_id: '', properties: '{}', description: '' });
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (selectedEnvironment) {
      fetchModels();
    }
  }, [selectedEnvironment]);

  const fetchModels = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await DigitalTwinAPI.getModels(selectedEnvironment.environment_id);
      setModels(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleCreate = async () => {
    try {
      setSubmitting(true);
      await DigitalTwinAPI.createModel(selectedEnvironment.environment_id, {
        ...formData,
        properties: JSON.parse(formData.properties)
      });
      setIsModalOpen(false);
      fetchModels();
    } catch (err) {
      alert(`Failed to create model: ${err.message}`);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (modelId) => {
    if (window.confirm('Are you sure you want to delete this model?')) {
      try {
        await DigitalTwinAPI.deleteModel(selectedEnvironment.environment_id, modelId);
        fetchModels();
      } catch (err) {
        alert(`Failed to delete model: ${err.message}`);
      }
    }
  };

  return (
    <div style={styles.mainContent}>
      <div style={styles.contentHeader}>
        <h1 style={styles.contentTitle}>
          <Layers style={{ marginRight: '12px' }} />
          Model Management
        </h1>
        <button style={styles.primaryButton} onClick={() => { setFormData({ model_id: '', properties: '{}', description: '' }); setIsModalOpen(true); }}>
          <Plus style={styles.buttonIcon} />
          Create Model
        </button>
      </div>
      <div style={styles.content}>
        {loading && (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
          </div>
        )}

        {error && (
          <div style={styles.errorMessage}>
            <div style={styles.errorText}>
              <AlertCircle style={{ marginRight: '8px' }} />
              {error}
            </div>
            <button onClick={fetchModels} style={styles.cancelButton}>
              Retry
            </button>
          </div>
        )}

        {!loading && !error && models.length === 0 && (
          <div style={styles.emptyState}>
            <Layers style={styles.emptyStateIcon} />
            <p>No models found for this environment.</p>
          </div>
        )}

        {!loading && !error && models.length > 0 && (
          <div>
            {models.map(model => (
              <div key={model.model_id} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{model.model_id}</h3>
                    {model.description && <p style={styles.itemDescription}>{model.description}</p>}
                    <div style={{ marginTop: '8px' }}>
                      <span style={{ ...styles.badge, ...styles.badgeGray }}>Properties: {Object.keys(model.properties || {}).length}</span>
                    </div>
                  </div>
                  <div style={styles.itemActions}>
                    <button onClick={() => handleDelete(model.model_id)} style={{ ...styles.actionButton, ...styles.actionButtonRed }}>
                      <Trash2 style={styles.actionIcon} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
      <Modal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} title="Create New Model">
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Model ID *</label>
            <input type="text" value={formData.model_id} onChange={(e) => setFormData({ ...formData, model_id: e.target.value })} style={styles.formInput} required />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Properties (JSON)</label>
            <JsonEditor value={formData.properties} onChange={(val) => setFormData({ ...formData, properties: val })} />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Description</label>
            <textarea value={formData.description} onChange={(e) => setFormData({ ...formData, description: e.target.value })} style={styles.formTextarea} rows="3" />
          </div>
          <div style={styles.formActions}>
            <button onClick={() => setIsModalOpen(false)} style={styles.cancelButton}>Cancel</button>
            <button onClick={handleCreate} style={styles.primaryButton} disabled={submitting || !formData.model_id}>
              {submitting ? 'Creating...' : 'Create Model'}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
};

// Twin Management component
const TwinManagement = ({ selectedEnvironment }) => {
  const [twins, setTwins] = useState([]);
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [formData, setFormData] = useState({ twin_id: '', model_id: '', properties: '{}', description: '' });
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (selectedEnvironment) {
      fetchData();
    }
  }, [selectedEnvironment]);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const twinsData = await DigitalTwinAPI.getTwins(selectedEnvironment.environment_id);
      const modelsData = await DigitalTwinAPI.getModels(selectedEnvironment.environment_id);
      setTwins(twinsData);
      setModels(modelsData);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleCreate = async () => {
    try {
      setSubmitting(true);
      await DigitalTwinAPI.createTwin(selectedEnvironment.environment_id, {
        ...formData,
        properties: JSON.parse(formData.properties)
      });
      setIsModalOpen(false);
      fetchData();
    } catch (err) {
      alert(`Failed to create twin: ${err.message}`);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (twinId) => {
    if (window.confirm('Are you sure you want to delete this digital twin?')) {
      try {
        await DigitalTwinAPI.deleteTwin(selectedEnvironment.environment_id, twinId);
        fetchData();
      } catch (err) {
        alert(`Failed to delete twin: ${err.message}`);
      }
    }
  };

  return (
    <div style={styles.mainContent}>
      <div style={styles.contentHeader}>
        <h1 style={styles.contentTitle}>
          <Cpu style={{ marginRight: '12px' }} />
          Digital Twins
        </h1>
        <button style={styles.primaryButton} onClick={() => { setFormData({ twin_id: '', model_id: '', properties: '{}', description: '' }); setIsModalOpen(true); }}>
          <Plus style={styles.buttonIcon} />
          Create Twin
        </button>
      </div>
      <div style={styles.content}>
        {loading && (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
          </div>
        )}

        {error && (
          <div style={styles.errorMessage}>
            <div style={styles.errorText}>
              <AlertCircle style={{ marginRight: '8px' }} />
              {error}
            </div>
            <button onClick={fetchData} style={styles.cancelButton}>
              Retry
            </button>
          </div>
        )}

        {!loading && !error && twins.length === 0 && (
          <div style={styles.emptyState}>
            <Cpu style={styles.emptyStateIcon} />
            <p>No digital twins found for this environment.</p>
          </div>
        )}

        {!loading && !error && twins.length > 0 && (
          <div>
            {twins.map(twin => (
              <div key={twin.twin_id} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{twin.twin_id}</h3>
                    <p style={styles.itemSubtitle}>Model: {twin.model_id}</p>
                    {twin.description && <p style={styles.itemDescription}>{twin.description}</p>}
                    <p style={styles.itemMeta}>Created: {formatDate(twin.created_at)}</p>
                  </div>
                  <div style={styles.itemActions}>
                    <button onClick={() => handleDelete(twin.twin_id)} style={{ ...styles.actionButton, ...styles.actionButtonRed }}>
                      <Trash2 style={styles.actionIcon} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
      <Modal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} title="Create New Digital Twin">
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Twin ID *</label>
            <input type="text" value={formData.twin_id} onChange={(e) => setFormData({ ...formData, twin_id: e.target.value })} style={styles.formInput} required />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Model ID *</label>
            <select value={formData.model_id} onChange={(e) => setFormData({ ...formData, model_id: e.target.value })} style={styles.formInput} required>
              <option value="">Select a Model</option>
              {models.map(model => (
                <option key={model.model_id} value={model.model_id}>{model.model_id}</option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Properties (JSON)</label>
            <JsonEditor value={formData.properties} onChange={(val) => setFormData({ ...formData, properties: val })} />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Description</label>
            <textarea value={formData.description} onChange={(e) => setFormData({ ...formData, description: e.target.value })} style={styles.formTextarea} rows="3" />
          </div>
          <div style={styles.formActions}>
            <button onClick={() => setIsModalOpen(false)} style={styles.cancelButton}>Cancel</button>
            <button onClick={handleCreate} style={styles.primaryButton} disabled={submitting || !formData.twin_id || !formData.model_id}>
              {submitting ? 'Creating...' : 'Create Twin'}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
};

// Device Management component
const DeviceManagement = ({ selectedEnvironment }) => {
  const [devices, setDevices] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [formData, setFormData] = useState({ device_id: '', description: '', properties: '{}' });
  const [submitting, setSubmitting] = useState(false);
  const [selectedDevice, setSelectedDevice] = useState(null);
  const [isViewModalOpen, setIsViewModalOpen] = useState(false);

  useEffect(() => {
    if (selectedEnvironment) {
      fetchDevices();
    }
  }, [selectedEnvironment]);

  const fetchDevices = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await DigitalTwinAPI.getDevices(selectedEnvironment.environment_id);
      setDevices(data);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleCreate = async () => {
    try {
      setSubmitting(true);
      await DigitalTwinAPI.createDevice(selectedEnvironment.environment_id, {
        ...formData,
        properties: JSON.parse(formData.properties)
      });
      setIsModalOpen(false);
      fetchDevices();
    } catch (err) {
      alert(`Failed to create device: ${err.message}`);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (deviceId) => {
    if (window.confirm('Are you sure you want to delete this device?')) {
      try {
        await DigitalTwinAPI.deleteDevice(selectedEnvironment.environment_id, deviceId);
        fetchDevices();
      } catch (err) {
        alert(`Failed to delete device: ${err.message}`);
      }
    }
  };

  const handleView = (device) => {
    setSelectedDevice(device);
    setIsViewModalOpen(true);
  };

  return (
    <div style={styles.mainContent}>
      <div style={styles.contentHeader}>
        <h1 style={styles.contentTitle}>
          <Wifi style={{ marginRight: '12px' }} />
          Device Management
        </h1>
        <button style={styles.primaryButton} onClick={() => { setFormData({ device_id: '', description: '', properties: '{}' }); setIsModalOpen(true); }}>
          <Plus style={styles.buttonIcon} />
          Create Device
        </button>
      </div>
      <div style={styles.content}>
        {loading && (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
          </div>
        )}

        {error && (
          <div style={styles.errorMessage}>
            <div style={styles.errorText}>
              <AlertCircle style={{ marginRight: '8px' }} />
              {error}
            </div>
            <button onClick={fetchDevices} style={styles.cancelButton}>
              Retry
            </button>
          </div>
        )}

        {!loading && !error && devices.length === 0 && (
          <div style={styles.emptyState}>
            <WifiOff style={styles.emptyStateIcon} />
            <p>No devices found for this environment.</p>
          </div>
        )}

        {!loading && !error && devices.length > 0 && (
          <div>
            {devices.map(device => (
              <div key={device.device_id} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{device.device_id}</h3>
                    {device.description && <p style={styles.itemDescription}>{device.description}</p>}
                    <div style={{ marginTop: '8px' }}>
                      <span style={{ ...styles.badge, ...styles.badgeGray }}>Properties: {Object.keys(device.properties || {}).length}</span>
                    </div>
                  </div>
                  <div style={styles.itemActions}>
                    <button onClick={() => handleView(device)} style={{ ...styles.actionButton, ...styles.actionButtonBlue }}>
                      <Eye style={styles.actionIcon} />
                    </button>
                    <button onClick={() => handleDelete(device.device_id)} style={{ ...styles.actionButton, ...styles.actionButtonRed }}>
                      <Trash2 style={styles.actionIcon} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <Modal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} title="Create New Device">
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Device ID *</label>
            <input type="text" value={formData.device_id} onChange={(e) => setFormData({ ...formData, device_id: e.target.value })} style={styles.formInput} required />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Properties (JSON)</label>
            <JsonEditor value={formData.properties} onChange={(val) => setFormData({ ...formData, properties: val })} />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Description</label>
            <textarea value={formData.description} onChange={(e) => setFormData({ ...formData, description: e.target.value })} style={styles.formTextarea} rows="3" />
          </div>
          <div style={styles.formActions}>
            <button onClick={() => setIsModalOpen(false)} style={styles.cancelButton}>Cancel</button>
            <button onClick={handleCreate} style={styles.primaryButton} disabled={submitting || !formData.device_id}>
              {submitting ? 'Creating...' : 'Create Device'}
            </button>
          </div>
        </div>
      </Modal>

      <Modal isOpen={isViewModalOpen} onClose={() => setIsViewModalOpen(false)} title="Device Details">
        {selectedDevice && (
          <div>
            <div style={styles.formGroup}>
              <label style={styles.formLabel}>Device ID</label>
              <p>{selectedDevice.device_id}</p>
            </div>
            <div style={styles.formGroup}>
              <label style={styles.formLabel}>Properties</label>
              <pre style={styles.preCode}>{JSON.stringify(selectedDevice.properties, null, 2)}</pre>
            </div>
            <div style={styles.grid2}>
              <div>
                <label style={styles.formLabel}>Created</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{formatDate(selectedDevice.created_at)}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Last Updated</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{formatDate(selectedDevice.updated_at)}</p>
              </div>
            </div>
            {selectedDevice.description && (
              <div style={{ margin: '24px 0' }}>
                <label style={styles.formLabel}>Description</label>
                <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>{selectedDevice.description}</p>
              </div>
            )}
            <div style={{ marginTop: '24px' }}>
              <label style={styles.formLabel}>Last Seen</label>
              <p style={{ margin: '4px 0 0 0', fontSize: '14px' }}>
                {selectedDevice.last_seen ? formatDate(selectedDevice.last_seen) : 'Never'}
              </p>
            </div>
            <div style={{ marginTop: '24px' }}>
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
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};

// Mapping Management component
const MappingManagement = ({ selectedEnvironment }) => {
  const [mappings, setMappings] = useState([]);
  const [devices, setDevices] = useState([]);
  const [twins, setTwins] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [formData, setFormData] = useState({ device_id: '', twin_id: '', mapping_type: '', properties: '{}' });
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (selectedEnvironment) {
      fetchData();
    }
  }, [selectedEnvironment]);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const mappingsData = await DigitalTwinAPI.getDeviceTwinMappings(selectedEnvironment.environment_id);
      const devicesData = await DigitalTwinAPI.getDevices(selectedEnvironment.environment_id);
      const twinsData = await DigitalTwinAPI.getTwins(selectedEnvironment.environment_id);
      setMappings(mappingsData);
      setDevices(devicesData);
      setTwins(twinsData);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleCreate = async () => {
    try {
      setSubmitting(true);
      await DigitalTwinAPI.createDeviceTwinMapping(selectedEnvironment.environment_id, {
        ...formData,
        properties: JSON.parse(formData.properties)
      });
      setIsModalOpen(false);
      fetchData();
    } catch (err) {
      alert(`Failed to create mapping: ${err.message}`);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (deviceId, twinId) => {
    if (window.confirm('Are you sure you want to delete this mapping?')) {
      try {
        await DigitalTwinAPI.deleteDeviceTwinMapping(selectedEnvironment.environment_id, deviceId, twinId);
        fetchData();
      } catch (err) {
        alert(`Failed to delete mapping: ${err.message}`);
      }
    }
  };

  return (
    <div style={styles.mainContent}>
      <div style={styles.contentHeader}>
        <h1 style={styles.contentTitle}>
          <Link style={{ marginRight: '12px' }} />
          Device-Twin Mappings
        </h1>
        <button style={styles.primaryButton} onClick={() => { setFormData({ device_id: '', twin_id: '', mapping_type: '', properties: '{}' }); setIsModalOpen(true); }}>
          <Plus style={styles.buttonIcon} />
          Create Mapping
        </button>
      </div>
      <div style={styles.content}>
        {loading && (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
          </div>
        )}
        {error && (
          <div style={styles.errorMessage}>
            <div style={styles.errorText}>
              <AlertCircle style={{ marginRight: '8px' }} />
              {error}
            </div>
            <button onClick={fetchData} style={styles.cancelButton}>
              Retry
            </button>
          </div>
        )}
        {!loading && !error && mappings.length === 0 && (
          <div style={styles.emptyState}>
            <Unlink style={styles.emptyStateIcon} />
            <p>No device-twin mappings found for this environment.</p>
          </div>
        )}
        {!loading && !error && mappings.length > 0 && (
          <div>
            {mappings.map((mapping, index) => (
              <div key={index} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{mapping.device_id} &rarr; {mapping.twin_id}</h3>
                    <p style={styles.itemSubtitle}>Type: {mapping.mapping_type}</p>
                    <p style={styles.itemMeta}>Created: {formatDate(mapping.created_at)}</p>
                  </div>
                  <div style={styles.itemActions}>
                    <button onClick={() => handleDelete(mapping.device_id, mapping.twin_id)} style={{ ...styles.actionButton, ...styles.actionButtonRed }}>
                      <Trash2 style={styles.actionIcon} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <Modal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} title="Create New Mapping">
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Device ID *</label>
            <select value={formData.device_id} onChange={(e) => setFormData({ ...formData, device_id: e.target.value })} style={styles.formInput} required>
              <option value="">Select a Device</option>
              {devices.map(device => (
                <option key={device.device_id} value={device.device_id}>{device.device_id}</option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Twin ID *</label>
            <select value={formData.twin_id} onChange={(e) => setFormData({ ...formData, twin_id: e.target.value })} style={styles.formInput} required>
              <option value="">Select a Twin</option>
              {twins.map(twin => (
                <option key={twin.twin_id} value={twin.twin_id}>{twin.twin_id}</option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Mapping Type *</label>
            <input type="text" value={formData.mapping_type} onChange={(e) => setFormData({ ...formData, mapping_type: e.target.value })} style={styles.formInput} required />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Properties (JSON)</label>
            <JsonEditor value={formData.properties} onChange={(val) => setFormData({ ...formData, properties: val })} />
          </div>
          <div style={styles.formActions}>
            <button onClick={() => setIsModalOpen(false)} style={styles.cancelButton}>Cancel</button>
            <button onClick={handleCreate} style={styles.primaryButton} disabled={submitting || !formData.device_id || !formData.twin_id || !formData.mapping_type}>
              {submitting ? 'Creating...' : 'Create Mapping'}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
};

// Relationship Management component
const RelationshipManagement = ({ selectedEnvironment }) => {
  const [relationships, setRelationships] = useState([]);
  const [twins, setTwins] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [formData, setFormData] = useState({ source_twin_id: '', target_twin_id: '', relationship_name: '', properties: '{}' });
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (selectedEnvironment) {
      fetchData();
    }
  }, [selectedEnvironment]);

  const fetchData = async () => {
    setLoading(true);
    setError(null);
    try {
      const relsData = await DigitalTwinAPI.getRelationships(selectedEnvironment.environment_id);
      const twinsData = await DigitalTwinAPI.getTwins(selectedEnvironment.environment_id);
      setRelationships(relsData);
      setTwins(twinsData);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleCreate = async () => {
    try {
      setSubmitting(true);
      await DigitalTwinAPI.createRelationship(selectedEnvironment.environment_id, {
        ...formData,
        properties: JSON.parse(formData.properties)
      });
      setIsModalOpen(false);
      fetchData();
    } catch (err) {
      alert(`Failed to create relationship: ${err.message}`);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (sourceId, targetId, relName) => {
    if (window.confirm('Are you sure you want to delete this relationship?')) {
      try {
        await DigitalTwinAPI.deleteRelationship(selectedEnvironment.environment_id, sourceId, targetId, relName);
        fetchData();
      } catch (err) {
        alert(`Failed to delete relationship: ${err.message}`);
      }
    }
  };

  return (
    <div style={styles.mainContent}>
      <div style={styles.contentHeader}>
        <h1 style={styles.contentTitle}>
          <Network style={{ marginRight: '12px' }} />
          Relationship Management
        </h1>
        <button style={styles.primaryButton} onClick={() => { setFormData({ source_twin_id: '', target_twin_id: '', relationship_name: '', properties: '{}' }); setIsModalOpen(true); }}>
          <Plus style={styles.buttonIcon} />
          Create Relationship
        </button>
      </div>
      <div style={styles.content}>
        {loading && (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
          </div>
        )}
        {error && (
          <div style={styles.errorMessage}>
            <div style={styles.errorText}>
              <AlertCircle style={{ marginRight: '8px' }} />
              {error}
            </div>
            <button onClick={fetchData} style={styles.cancelButton}>
              Retry
            </button>
          </div>
        )}
        {!loading && !error && relationships.length === 0 && (
          <div style={styles.emptyState}>
            <Network style={styles.emptyStateIcon} />
            <p>No relationships found for this environment.</p>
          </div>
        )}
        {!loading && !error && relationships.length > 0 && (
          <div>
            {relationships.map((rel, index) => (
              <div key={index} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{rel.relationship_name}</h3>
                    <p style={styles.itemSubtitle}>
                      {rel.source_twin_id} &rarr; {rel.target_twin_id}
                    </p>
                    <div style={{ marginTop: '8px' }}>
                      <span style={{ ...styles.badge, ...styles.badgeGray }}>Properties: {Object.keys(rel.properties || {}).length}</span>
                    </div>
                  </div>
                  <div style={styles.itemActions}>
                    <button onClick={() => handleDelete(rel.source_twin_id, rel.target_twin_id, rel.relationship_name)} style={{ ...styles.actionButton, ...styles.actionButtonRed }}>
                      <Trash2 style={styles.actionIcon} />
                    </button>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <Modal isOpen={isModalOpen} onClose={() => setIsModalOpen(false)} title="Create New Relationship">
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Source Twin *</label>
            <select value={formData.source_twin_id} onChange={(e) => setFormData({ ...formData, source_twin_id: e.target.value })} style={styles.formInput} required>
              <option value="">Select source twin</option>
              {twins.map(twin => (
                <option key={twin.twin_id} value={twin.twin_id}>{twin.twin_id}</option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Target Twin *</label>
            <select value={formData.target_twin_id} onChange={(e) => setFormData({ ...formData, target_twin_id: e.target.value })} style={styles.formInput} required>
              <option value="">Select target twin</option>
              {twins.filter(t => t.twin_id !== formData.source_twin_id).map(twin => (
                <option key={twin.twin_id} value={twin.twin_id}>{twin.twin_id}</option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Relationship Name *</label>
            <input type="text" value={formData.relationship_name} onChange={(e) => setFormData({ ...formData, relationship_name: e.target.value })} style={styles.formInput} required />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Properties (JSON)</label>
            <JsonEditor value={formData.properties} onChange={(val) => setFormData({ ...formData, properties: val })} />
          </div>
          <div style={styles.formActions}>
            <button onClick={() => setIsModalOpen(false)} style={styles.cancelButton}>Cancel</button>
            <button onClick={handleCreate} style={styles.primaryButton} disabled={submitting || !formData.source_twin_id || !formData.target_twin_id || !formData.relationship_name}>
              {submitting ? 'Creating...' : 'Create Relationship'}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
};

// Twin Graph Visualizer component
const TwinGraphVisualizer = ({ selectedEnvironment }) => {
  const svgRef = useRef(null);
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [isTooltipVisible, setIsTooltipVisible] = useState(false);
  const [tooltipContent, setTooltipContent] = useState('');
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });
  const [lastUpdated, setLastUpdated] = useState(null);

  const fetchGraphData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await DigitalTwinAPI.getTreeGraph(selectedEnvironment.environment_id);
      setGraphData(data);
      setLastUpdated(new Date());
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [selectedEnvironment]);

  useEffect(() => {
    if (selectedEnvironment) {
      fetchGraphData();
    }
  }, [selectedEnvironment, fetchGraphData]);

  useEffect(() => {
    if (!svgRef.current || !graphData || graphData.nodes.length === 0) {
      return;
    }

    const svg = d3.select(svgRef.current);
    const width = svg.node().clientWidth;
    const height = svg.node().clientHeight;
    svg.selectAll('*').remove();

    const g = svg.append("g").attr("transform", "translate(0,0)");
    const zoom = d3.zoom().scaleExtent([0.1, 4]).on("zoom", (event) => {
      g.attr("transform", event.transform);
    });
    svg.call(zoom);

    const nodes = graphData.nodes.map(d => ({ ...d }));
    const links = graphData.links.map(d => ({ ...d, source: d.source, target: d.target }));

    const simulation = d3.forceSimulation(nodes)
      .force("link", d3.forceLink(links).id(d => d.id).distance(150))
      .force("charge", d3.forceManyBody().strength(-300))
      .force("center", d3.forceCenter(width / 2, height / 2));

    const link = g.append("g")
      .attr("stroke", "#999")
      .attr("stroke-opacity", 0.6)
      .selectAll("line")
      .data(links)
      .join("line")
      .attr("stroke-width", d => Math.sqrt(d.value || 1));

    const node = g.append("g")
      .attr("stroke", "#fff")
      .attr("stroke-width", 1.5)
      .selectAll("circle")
      .data(nodes)
      .join("circle")
      .attr("r", 12)
      .attr("fill", "#2563eb")
      .on("mouseover", (event, d) => {
        setTooltipContent(`Twin: ${d.id}\nModel: ${d.model_id}`);
        setIsTooltipVisible(true);
        setTooltipPosition({ x: event.clientX, y: event.clientY });
      })
      .on("mousemove", (event) => {
        setTooltipPosition({ x: event.clientX, y: event.clientY });
      })
      .on("mouseout", () => {
        setIsTooltipVisible(false);
      })
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
        }));

    const labels = g.append("g")
      .attr("pointer-events", "none")
      .selectAll("text")
      .data(nodes)
      .join("text")
      .attr("text-anchor", "middle")
      .attr("dy", "2em")
      .attr("fill", "#333")
      .style("font-size", "10px")
      .text(d => d.id);

    simulation.on("tick", () => {
      link
        .attr("x1", d => d.source.x)
        .attr("y1", d => d.source.y)
        .attr("x2", d => d.target.x)
        .attr("y2", d => d.target.y);

      node
        .attr("cx", d => d.x)
        .attr("cy", d => d.y);

      labels
        .attr("x", d => d.x)
        .attr("y", d => d.y);
    });
  }, [graphData]);

  return (
    <div style={styles.mainContent}>
      <div style={styles.contentHeader}>
        <h1 style={styles.contentTitle}>
          <GitBranch style={{ marginRight: '12px' }} />
          Twin Graph Visualizer
        </h1>
        <button style={styles.primaryButton} onClick={fetchGraphData}>
          <RefreshCw style={styles.buttonIcon} />
          Refresh Graph
        </button>
      </div>
      <div style={styles.content}>
        {loading && (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
          </div>
        )}
        {error && (
          <div style={styles.errorMessage}>
            <div style={styles.errorText}>
              <AlertCircle style={{ marginRight: '8px' }} />
              {error}
            </div>
          </div>
        )}
        {!loading && !error && (
          <div style={styles.visualizerContainer}>
            <svg ref={svgRef} style={{ width: '100%', height: '100%' }}></svg>
            <div style={{ position: 'absolute', bottom: '16px', left: '16px', fontSize: '12px', color: '#6b7280' }}>
              {graphData.nodes.length} twins, {graphData.links.length} relationships
            </div>
            {lastUpdated && (
              <div style={styles.lastUpdatedBox}>
                <Clock style={{ width: '16px', height: '16px', color: '#0ea5e9' }} />
                <span style={styles.lastUpdatedText}>
                  Last Updated: {lastUpdated.toLocaleTimeString()}
                </span>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

// Workflow management component
const Workflow = ({ selectedEnvironment, environments }) => {
  const [workflows, setWorkflows] = useState([]);
  const [availableFunctions, setAvailableFunctions] = useState([]);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);
  const [showSubmitModal, setShowSubmitModal] = useState(false);
  const [submitForm, setSubmitForm] = useState({
    name: '',
    description: '',
    steps: '[]'
  });
  const [submitting, setSubmitting] = useState(false);
  const [selectedWorkflow, setSelectedWorkflow] = useState(null);

  useEffect(() => {
    if (selectedEnvironment) {
      fetchWorkflows();
      fetchAvailableFunctions();
    }
  }, [selectedEnvironment]);

  const fetchWorkflows = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await WorkflowAPI.listWorkflows(selectedEnvironment.environment_id);
      setWorkflows(data.items || data);
    } catch (err) {
      setError('Failed to fetch workflows: ' + err.message);
    } finally {
      setLoading(false);
    }
  };

  const fetchAvailableFunctions = async () => {
    try {
      const data = await WorkflowAPI.getAvailableFunctions();
      setAvailableFunctions(data.functions || data);
    } catch (err) {
      console.error('Failed to fetch available functions:', err);
    }
  };

  const handleSubmit = async () => {
    try {
      setSubmitting(true);
      setError(null);
      const workflowData = {
        name: submitForm.name,
        description: submitForm.description,
        steps: JSON.parse(submitForm.steps)
      };
      await WorkflowAPI.submitWorkflow(selectedEnvironment.environment_id, workflowData);
      setShowSubmitModal(false);
      fetchWorkflows();
    } catch (err) {
      setError('Failed to submit workflow: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (workflowId) => {
    if (window.confirm('Are you sure you want to delete this workflow?')) {
      try {
        await WorkflowAPI.deleteWorkflow(selectedEnvironment.environment_id, workflowId);
        fetchWorkflows();
      } catch (err) {
        setError('Failed to delete workflow: ' + err.message);
      }
    }
  };

  const handleRun = async (workflowId) => {
    try {
      await WorkflowAPI.submitWorkflow(selectedEnvironment.environment_id, {
        name: `Re-run of ${workflowId}`,
        steps: workflows.find(w => w.workflow_id === workflowId).steps
      });
      fetchWorkflows();
      alert('Workflow re-submitted successfully!');
    } catch (err) {
      setError('Failed to re-run workflow: ' + err.message);
    }
  };

  const renderStatusBadge = (status) => {
    switch (status) {
      case 'SUCCEEDED':
        return <span style={{ ...styles.badge, ...styles.badgeGreen }}>Succeeded</span>;
      case 'FAILED':
        return <span style={{ ...styles.badge, ...styles.badgeRed }}>Failed</span>;
      case 'RUNNING':
        return <span style={{ ...styles.badge, ...styles.badgeBlue }}>Running</span>;
      case 'PENDING':
        return <span style={{ ...styles.badge, ...styles.badgeYellow }}>Pending</span>;
      default:
        return <span style={{ ...styles.badge, ...styles.badgeGray }}>{status}</span>;
    }
  };

  return (
    <div style={styles.mainContent}>
      <div style={styles.contentHeader}>
        <h1 style={styles.contentTitle}>
          <GitBranch style={{ marginRight: '12px' }} />
          Workflows
        </h1>
        <button
          style={styles.primaryButton}
          onClick={() => setShowSubmitModal(true)}
        >
          <Plus style={styles.buttonIcon} />
          Submit Workflow
        </button>
      </div>
      <div style={styles.content}>
        {loading && (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
          </div>
        )}

        {error && (
          <div style={styles.errorMessage}>
            <div style={styles.errorText}>
              <AlertCircle style={{ marginRight: '8px' }} />
              {error}
            </div>
            <button onClick={fetchWorkflows} style={styles.cancelButton}>
              Retry
            </button>
          </div>
        )}

        {!loading && !error && workflows.length === 0 && (
          <div style={styles.emptyState}>
            <GitBranch style={styles.emptyStateIcon} />
            <p>No workflows found for this environment.</p>
          </div>
        )}

        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {workflows.map((workflow) => (
            <div
              key={workflow.workflow_id}
              style={styles.listItem}
              onClick={() => setSelectedWorkflow(selectedWorkflow?.workflow_id === workflow.workflow_id ? null : workflow)}
            >
              <div style={styles.itemContent}>
                <div style={styles.itemInfo}>
                  <h3 style={styles.itemTitle}>
                    {workflow.name}
                  </h3>
                  <p style={styles.itemSubtitle}>
                    ID: {workflow.workflow_id}
                  </p>
                  <p style={styles.itemMeta}>
                    Submitted: {formatDate(workflow.submitted_at)}
                  </p>
                  <div style={{ marginTop: '8px' }}>
                    {renderStatusBadge(workflow.status)}
                  </div>
                </div>
                <div style={styles.itemActions}>
                  <button
                    onClick={(e) => { e.stopPropagation(); handleRun(workflow.workflow_id); }}
                    style={{ ...styles.actionButton, ...styles.actionButtonGreen }}
                  >
                    <Play style={styles.actionIcon} />
                  </button>
                  <button
                    onClick={(e) => { e.stopPropagation(); handleDelete(workflow.workflow_id); }}
                    style={{ ...styles.actionButton, ...styles.actionButtonRed }}
                  >
                    <Trash2 style={styles.actionIcon} />
                  </button>
                  <button
                    onClick={(e) => e.stopPropagation()}
                    style={{ ...styles.actionButton, ...styles.actionButtonGray }}
                  >
                    {selectedWorkflow?.workflow_id === workflow.workflow_id ? <ChevronDown style={styles.actionIcon} /> : <ChevronRight style={styles.actionIcon} />}
                  </button>
                </div>
              </div>
              {selectedWorkflow?.workflow_id === workflow.workflow_id && (
                <div style={{ marginTop: '16px', borderTop: '1px solid #e2e8f0', paddingTop: '16px' }}>
                  <h4 style={{ fontSize: '16px', fontWeight: '500', margin: '0 0 8px 0' }}>Workflow Details</h4>
                  <p style={{ ...styles.itemDescription, margin: '0 0 16px 0' }}>{workflow.description}</p>
                  <div style={{ marginBottom: '16px' }}>
                    <h5 style={{ fontSize: '14px', fontWeight: '500', margin: '0 0 4px 0' }}>Steps:</h5>
                    <pre style={styles.preCode}>{JSON.stringify(workflow.steps, null, 2)}</pre>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      <Modal
        isOpen={showSubmitModal}
        onClose={() => {
          setShowSubmitModal(false);
          setSubmitForm({ name: '', description: '', steps: '[]' });
          setError(null);
        }}
        title="Submit New Workflow"
      >
        <div>
          {error && (
            <div style={styles.errorMessage}>
              <div style={styles.errorText}>
                <AlertCircle style={{ marginRight: '8px' }} />
                {error}
              </div>
              <button onClick={() => setError(null)} style={styles.cancelButton}>
                Dismiss
              </button>
            </div>
          )}
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Name *</label>
            <input
              type="text"
              value={submitForm.name}
              onChange={(e) => setSubmitForm({ ...submitForm, name: e.target.value })}
              style={styles.formInput}
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Description</label>
            <textarea
              value={submitForm.description}
              onChange={(e) => setSubmitForm({ ...submitForm, description: e.target.value })}
              style={styles.formTextarea}
              rows="3"
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Steps (JSON Array) *</label>
            <JsonEditor
              value={submitForm.steps}
              onChange={(val) => setSubmitForm({ ...submitForm, steps: val })}
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Available Functions</label>
            <pre style={{ ...styles.preCode, maxHeight: '120px' }}>
              {JSON.stringify(availableFunctions, null, 2)}
            </pre>
          </div>
          <div style={styles.formActions}>
            <button
              onClick={() => setShowSubmitModal(false)}
              style={styles.cancelButton}
            >
              Cancel
            </button>
            <button
              onClick={handleSubmit}
              style={{ ...styles.primaryButton, opacity: submitting ? 0.7 : 1 }}
              disabled={submitting || !submitForm.name || !submitForm.steps}
            >
              {submitting ? 'Submitting...' : 'Submit Workflow'}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
};


// Main application component
const DigitalTwinPlatform = () => {
  const [environments, setEnvironments] = useState([]);
  const [selectedEnvironment, setSelectedEnvironment] = useState(null);
  const [view, setView] = useState('twins');
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchEnvironments();
  }, []);

  const fetchEnvironments = async () => {
    setLoading(true);
    setError(null);
    try {
      const envs = await DigitalTwinAPI.getEnvironments();
      setEnvironments(envs);
      if (envs.length > 0) {
        setSelectedEnvironment(envs[0]);
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const renderContent = () => {
    if (loading) {
      return (
        <div style={styles.loadingContainer}>
          <div style={styles.spinner}></div>
        </div>
      );
    }

    if (error) {
      return (
        <div style={styles.emptyState}>
          <p style={styles.warningText}>Error: {error}</p>
        </div>
      );
    }

    if (!selectedEnvironment) {
      return (
        <div style={styles.emptyState}>
          <p>Please select or create an environment.</p>
        </div>
      );
    }

    switch (view) {
      case 'dashboard':
        return <Dashboard selectedEnvironment={selectedEnvironment} />;
      case 'environments':
        return <EnvironmentManagement selectedEnvironment={selectedEnvironment} setSelectedEnvironment={setSelectedEnvironment} environments={environments} fetchEnvironments={fetchEnvironments} />;
      case 'models':
        return <ModelManagement selectedEnvironment={selectedEnvironment} />;
      case 'twins':
        return <TwinManagement selectedEnvironment={selectedEnvironment} />;
      case 'devices':
        return <DeviceManagement selectedEnvironment={selectedEnvironment} />;
      case 'mappings':
        return <MappingManagement selectedEnvironment={selectedEnvironment} />;
      case 'relationships':
        return <RelationshipManagement selectedEnvironment={selectedEnvironment} />;
      case 'graph':
        return <TwinGraphVisualizer selectedEnvironment={selectedEnvironment} />;
      case 'workflow':
        return <Workflow selectedEnvironment={selectedEnvironment} environments={environments} />;
      default:
        return <div style={styles.emptyState}><p>Select a view from the sidebar.</p></div>;
    }
  };

  const renderSidebar = () => {
    return (
      <div style={styles.sidebar}>
        <button
          style={{
            ...styles.navButton,
            ...(view === 'dashboard' ? styles.navButtonActive : {})
          }}
          onClick={() => setView('dashboard')}
        >
          <BarChart3 style={styles.navIcon} />
          Dashboard
        </button>
        <button
          style={{
            ...styles.navButton,
            ...(view === 'environments' ? styles.navButtonActive : {})
          }}
          onClick={() => setView('environments')}
        >
          <Building style={styles.navIcon} />
          Environments
        </button>
        <button
          style={{
            ...styles.navButton,
            ...(view === 'models' ? styles.navButtonActive : {})
          }}
          onClick={() => setView('models')}
        >
          <Layers style={styles.navIcon} />
          Models
        </button>
        <button
          style={{
            ...styles.navButton,
            ...(view === 'twins' ? styles.navButtonActive : {})
          }}
          onClick={() => setView('twins')}
        >
          <Cpu style={styles.navIcon} />
          Digital Twins
        </button>
        <button
          style={{
            ...styles.navButton,
            ...(view === 'devices' ? styles.navButtonActive : {})
          }}
          onClick={() => setView('devices')}
        >
          <Wifi style={styles.navIcon} />
          Devices
        </button>
        <button
          style={{
            ...styles.navButton,
            ...(view === 'mappings' ? styles.navButtonActive : {})
          }}
          onClick={() => setView('mappings')}
        >
          <Link style={styles.navIcon} />
          Mappings
        </button>
        <button
          style={{
            ...styles.navButton,
            ...(view === 'relationships' ? styles.navButtonActive : {})
          }}
          onClick={() => setView('relationships')}
        >
          <Network style={styles.navIcon} />
          Relationships
        </button>
        <button
          style={{
            ...styles.navButton,
            ...(view === 'graph' ? styles.navButtonActive : {})
          }}
          onClick={() => setView('graph')}
        >
          <GitBranch style={styles.navIcon} />
          Graph Visualizer
        </button>
        <button
          style={{
            ...styles.navButton,
            ...(view === 'workflow' ? styles.navButtonActive : {})
          }}
          onClick={() => setView('workflow')}
        >
          <Activity style={styles.navIcon} />
          Workflows
        </button>
      </div>
    );
  };

  return (
    <div style={styles.app}>
      <header style={styles.header}>
        <div style={styles.headerContainer}>
          <div style={styles.headerContent}>
            <div style={styles.headerLeft}>
              <Database style={styles.logo} />
              <h1 style={styles.title}>Digital Twin Platform</h1>
            </div>
            <div style={styles.headerRight}>
              <select
                value={selectedEnvironment ? selectedEnvironment.environment_id : ''}
                onChange={(e) => {
                  const newEnv = environments.find(env => env.environment_id === e.target.value);
                  setSelectedEnvironment(newEnv);
                  setView('dashboard');
                }}
                style={styles.select}
                disabled={environments.length === 0}
              >
                <option value="">Select Environment</option>
                {environments.map(env => (
                  <option key={env.environment_id} value={env.environment_id}>
                    {env.display_name}
                  </option>
                ))}
              </select>
              <button
                onClick={fetchEnvironments}
                style={{ ...styles.primaryButton, backgroundColor: '#4b5563' }}
              >
                <RefreshCw style={styles.buttonIcon} />
                Refresh
              </button>
            </div>
          </div>
        </div>
      </header>

      <div style={styles.mainContainer}>
        <div style={styles.contentWrapper}>
          {renderSidebar()}
          {renderContent()}
        </div>
      </div>
    </div>
  );
};

export default DigitalTwinPlatform;