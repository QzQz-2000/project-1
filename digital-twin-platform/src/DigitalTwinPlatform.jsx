// // import React, { useState, useEffect, useRef } from 'react';
// // import { 
// //   Building, 
// //   Database, 
// //   Settings, 
// //   Plus, 
// //   Edit, 
// //   Trash2, 
// //   Search,
// //   Network,
// //   Upload,
// //   Play,
// //   Eye,
// //   ChevronRight,
// //   ChevronDown,
// //   Activity,
// //   Cpu,
// //   GitBranch,
// //   X,
// //   Check,
// //   AlertCircle,
// //   FileText,
// //   Save
// // } from 'lucide-react';
// // import * as d3 from 'd3';

// // // API base configuration
// // const API_BASE_URL = 'http://localhost:8000';

// // // Style definitions
// // const styles = {
// //   app: {
// //     minHeight: '100vh',
// //     backgroundColor: '#f8fafc',
// //     fontFamily: 'system-ui, -apple-system, sans-serif'
// //   },
// //   header: {
// //     backgroundColor: '#ffffff',
// //     borderBottom: '1px solid #e2e8f0',
// //     boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1)'
// //   },
// //   headerContainer: {
// //     maxWidth: '1200px',
// //     margin: '0 auto',
// //     padding: '0 24px'
// //   },
// //   headerContent: {
// //     display: 'flex',
// //     justifyContent: 'space-between',
// //     alignItems: 'center',
// //     height: '64px'
// //   },
// //   headerLeft: {
// //     display: 'flex',
// //     alignItems: 'center'
// //   },
// //   logo: {
// //     width: '32px',
// //     height: '32px',
// //     color: '#2563eb',
// //     marginRight: '12px'
// //   },
// //   title: {
// //     fontSize: '20px',
// //     fontWeight: '600',
// //     color: '#1f2937',
// //     margin: 0
// //   },
// //   select: {
// //     border: '1px solid #d1d5db',
// //     borderRadius: '6px',
// //     padding: '8px 12px',
// //     fontSize: '14px',
// //     backgroundColor: '#ffffff',
// //     minWidth: '200px'
// //   },
// //   mainContainer: {
// //     maxWidth: '1200px',
// //     margin: '0 auto',
// //     padding: '32px 24px'
// //   },
// //   contentWrapper: {
// //     display: 'flex',
// //     gap: '24px'
// //   },
// //   sidebar: {
// //     width: '256px',
// //     backgroundColor: '#ffffff',
// //     borderRadius: '8px',
// //     boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1)',
// //     padding: '16px'
// //   },
// //   navButton: {
// //     width: '100%',
// //     display: 'flex',
// //     alignItems: 'center',
// //     padding: '8px 12px',
// //     marginBottom: '4px',
// //     fontSize: '14px',
// //     fontWeight: '500',
// //     border: 'none',
// //     borderRadius: '6px',
// //     cursor: 'pointer',
// //     transition: 'all 0.2s',
// //     backgroundColor: 'transparent',
// //     color: '#4b5563'
// //   },
// //   navButtonActive: {
// //     backgroundColor: '#dbeafe',
// //     color: '#1d4ed8'
// //   },
// //   navIcon: {
// //     width: '20px',
// //     height: '20px',
// //     marginRight: '12px'
// //   },
// //   mainContent: {
// //     flex: 1,
// //     backgroundColor: '#ffffff',
// //     borderRadius: '8px',
// //     boxShadow: '0 1px 3px 0 rgba(0, 0, 0, 0.1)',
// //     minHeight: '600px'
// //   },
// //   contentHeader: {
// //     padding: '24px',
// //     borderBottom: '1px solid #e2e8f0',
// //     display: 'flex',
// //     justifyContent: 'space-between',
// //     alignItems: 'center'
// //   },
// //   contentTitle: {
// //     fontSize: '24px',
// //     fontWeight: '600',
// //     color: '#1f2937',
// //     margin: 0
// //   },
// //   primaryButton: {
// //     display: 'inline-flex',
// //     alignItems: 'center',
// //     padding: '8px 16px',
// //     border: 'none',
// //     fontSize: '14px',
// //     fontWeight: '500',
// //     borderRadius: '6px',
// //     boxShadow: '0 1px 2px 0 rgba(0, 0, 0, 0.05)',
// //     color: '#ffffff',
// //     backgroundColor: '#2563eb',
// //     cursor: 'pointer',
// //     transition: 'all 0.2s'
// //   },
// //   buttonIcon: {
// //     width: '16px',
// //     height: '16px',
// //     marginRight: '8px'
// //   },
// //   content: {
// //     padding: '24px'
// //   },
// //   errorMessage: {
// //     marginBottom: '16px',
// //     backgroundColor: '#fef2f2',
// //     border: '1px solid #fecaca',
// //     borderRadius: '6px',
// //     padding: '16px',
// //     display: 'flex',
// //     justifyContent: 'space-between',
// //     alignItems: 'center'
// //   },
// //   errorText: {
// //     display: 'flex',
// //     alignItems: 'center',
// //     color: '#b91c1c'
// //   },
// //   loadingContainer: {
// //     display: 'flex',
// //     alignItems: 'center',
// //     justifyContent: 'center',
// //     padding: '48px'
// //   },
// //   spinner: {
// //     width: '32px',
// //     height: '32px',
// //     border: '2px solid #e2e8f0',
// //     borderTop: '2px solid #2563eb',
// //     borderRadius: '50%',
// //     animation: 'spin 1s linear infinite'
// //   },
// //   emptyState: {
// //     textAlign: 'center',
// //     padding: '48px',
// //     color: '#6b7280'
// //   },
// //   emptyStateIcon: {
// //     width: '48px',
// //     height: '48px',
// //     color: '#9ca3af',
// //     margin: '0 auto 16px'
// //   },
// //   listItem: {
// //     border: '1px solid #e2e8f0',
// //     borderRadius: '8px',
// //     padding: '16px',
// //     marginBottom: '16px'
// //   },
// //   itemContent: {
// //     display: 'flex',
// //     justifyContent: 'space-between',
// //     alignItems: 'flex-start'
// //   },
// //   itemInfo: {
// //     flex: 1
// //   },
// //   itemTitle: {
// //     fontSize: '18px',
// //     fontWeight: '500',
// //     color: '#1f2937',
// //     margin: '0 0 4px 0'
// //   },
// //   itemSubtitle: {
// //     fontSize: '14px',
// //     color: '#6b7280',
// //     margin: '0 0 8px 0'
// //   },
// //   itemDescription: {
// //     color: '#4b5563',
// //     margin: '8px 0'
// //   },
// //   itemMeta: {
// //     fontSize: '12px',
// //     color: '#6b7280',
// //     margin: '8px 0 0 0'
// //   },
// //   itemActions: {
// //     display: 'flex',
// //     gap: '8px'
// //   },
// //   actionButton: {
// //     padding: '8px',
// //     border: 'none',
// //     borderRadius: '6px',
// //     cursor: 'pointer',
// //     transition: 'all 0.2s'
// //   },
// //   actionButtonBlue: {
// //     color: '#2563eb',
// //     backgroundColor: 'transparent'
// //   },
// //   actionButtonGray: {
// //     color: '#6b7280',
// //     backgroundColor: 'transparent'
// //   },
// //   actionButtonRed: {
// //     color: '#dc2626',
// //     backgroundColor: 'transparent'
// //   },
// //   actionIcon: {
// //     width: '16px',
// //     height: '16px'
// //   },
// //   modal: {
// //     position: 'fixed',
// //     top: 0,
// //     left: 0,
// //     right: 0,
// //     bottom: 0,
// //     backgroundColor: 'rgba(0, 0, 0, 0.5)',
// //     display: 'flex',
// //     alignItems: 'center',
// //     justifyContent: 'center',
// //     zIndex: 50
// //   },
// //   modalContent: {
// //     backgroundColor: '#ffffff',
// //     borderRadius: '8px',
// //     padding: '24px',
// //     maxWidth: '600px',
// //     width: '100%',
// //     margin: '16px',
// //     maxHeight: '90vh',
// //     overflowY: 'auto'
// //   },
// //   modalContentLarge: {
// //     backgroundColor: '#ffffff',
// //     borderRadius: '8px',
// //     padding: '24px',
// //     maxWidth: '800px',
// //     width: '100%',
// //     margin: '16px',
// //     maxHeight: '90vh',
// //     overflowY: 'auto'
// //   },
// //   modalHeader: {
// //     display: 'flex',
// //     justifyContent: 'space-between',
// //     alignItems: 'center',
// //     marginBottom: '16px'
// //   },
// //   modalTitle: {
// //     fontSize: '20px',
// //     fontWeight: '600',
// //     margin: 0
// //   },
// //   closeButton: {
// //     background: 'none',
// //     border: 'none',
// //     color: '#6b7280',
// //     cursor: 'pointer',
// //     padding: '4px'
// //   },
// //   formGroup: {
// //     marginBottom: '16px'
// //   },
// //   formLabel: {
// //     display: 'block',
// //     fontSize: '14px',
// //     fontWeight: '500',
// //     color: '#374151',
// //     marginBottom: '4px'
// //   },
// //   formInput: {
// //     width: '100%',
// //     padding: '8px 12px',
// //     border: '1px solid #d1d5db',
// //     borderRadius: '6px',
// //     fontSize: '14px',
// //     boxSizing: 'border-box'
// //   },
// //   formTextarea: {
// //     width: '100%',
// //     padding: '8px 12px',
// //     border: '1px solid #d1d5db',
// //     borderRadius: '6px',
// //     fontSize: '14px',
// //     fontFamily: 'inherit',
// //     resize: 'vertical',
// //     boxSizing: 'border-box'
// //   },
// //   formActions: {
// //     display: 'flex',
// //     justifyContent: 'flex-end',
// //     gap: '12px',
// //     marginTop: '24px'
// //   },
// //   cancelButton: {
// //     padding: '8px 16px',
// //     border: '1px solid #d1d5db',
// //     borderRadius: '6px',
// //     fontSize: '14px',
// //     fontWeight: '500',
// //     color: '#374151',
// //     backgroundColor: '#ffffff',
// //     cursor: 'pointer'
// //   },
// //   jsonEditor: {
// //     width: '100%',
// //     height: '160px',
// //     padding: '12px',
// //     border: '1px solid #d1d5db',
// //     borderRadius: '6px',
// //     fontFamily: 'Monaco, Consolas, monospace',
// //     fontSize: '12px',
// //     boxSizing: 'border-box'
// //   },
// //   jsonError: {
// //     marginTop: '4px',
// //     fontSize: '12px',
// //     color: '#dc2626',
// //     display: 'flex',
// //     alignItems: 'center'
// //   },
// //   badge: {
// //     display: 'inline-flex',
// //     alignItems: 'center',
// //     padding: '2px 8px',
// //     borderRadius: '9999px',
// //     fontSize: '12px',
// //     fontWeight: '500'
// //   },
// //   badgeGreen: {
// //     backgroundColor: '#dcfce7',
// //     color: '#166534'
// //   },
// //   badgeGray: {
// //     backgroundColor: '#f3f4f6',
// //     color: '#374151'
// //   },
// //   badgeRed: {
// //     backgroundColor: '#fee2e2',
// //     color: '#991b1b'
// //   },
// //   badgeYellow: {
// //     backgroundColor: '#fef3c7',
// //     color: '#92400e'
// //   },
// //   preCode: {
// //     backgroundColor: '#f8fafc',
// //     padding: '16px',
// //     borderRadius: '6px',
// //     fontSize: '12px',
// //     overflowX: 'auto',
// //     maxHeight: '384px',
// //     whiteSpace: 'pre-wrap',
// //     fontFamily: 'Monaco, Consolas, monospace'
// //   },
// //   grid2: {
// //     display: 'grid',
// //     gridTemplateColumns: '1fr 1fr',
// //     gap: '16px'
// //   },
// //   warningMessage: {
// //     marginBottom: '16px',
// //     backgroundColor: '#fefce8',
// //     border: '1px solid #fde047',
// //     borderRadius: '6px',
// //     padding: '16px'
// //   },
// //   warningText: {
// //     color: '#a16207'
// //   }
// // };

// // // Add CSS animation
// // const spinKeyframes = `
// // @keyframes spin {
// //   from { transform: rotate(0deg); }
// //   to { transform: rotate(360deg); }
// // }
// // `;

// // // Insert CSS into head
// // if (typeof document !== 'undefined') {
// //   const style = document.createElement('style');
// //   style.textContent = spinKeyframes;
// //   document.head.appendChild(style);
// // }

// // // Date formatting helper function
// // const formatDate = (dateString) => {
// //   if (!dateString) return 'Unknown';
  
// //   try {
// //     let cleanDateString = dateString;
// //     if (typeof dateString === 'string' && dateString.includes('+00:00Z')) {
// //       cleanDateString = dateString.replace('+00:00Z', '+00:00');
// //     }
    
// //     const date = new Date(cleanDateString);
    
// //     if (isNaN(date.getTime())) {
// //       console.error('Date parsing failed:', dateString);
// //       return 'Invalid Format';
// //     }
    
// //     return date.toLocaleString('en-US', {
// //       year: 'numeric',
// //       month: '2-digit',
// //       day: '2-digit',
// //       hour: '2-digit',
// //       minute: '2-digit'
// //     });
// //   } catch (error) {
// //     console.error('Date formatting error:', error);
// //     return 'Parse Error';
// //   }
// // };

// // // API service class
// // class DigitalTwinAPI {
// //   static async request(endpoint, options = {}) {
// //     try {
// //       const response = await fetch(`${API_BASE_URL}${endpoint}`, {
// //         headers: {
// //           'Content-Type': 'application/json',
// //           ...options.headers
// //         },
// //         ...options
// //       });
      
// //       if (!response.ok) {
// //         const errorData = await response.json().catch(() => ({}));
// //         throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
// //       }
      
// //       return response.json();
// //     } catch (error) {
// //       console.error('API Request failed:', error);
// //       throw error;
// //     }
// //   }

// //   // Environment management
// //   static async getEnvironments() {
// //     const response = await this.request('/environments');
// //     return response.items || response;
// //   }

// //   static async createEnvironment(data) {
// //     return this.request('/environments', {
// //       method: 'POST',
// //       body: JSON.stringify(data)
// //     });
// //   }

// //   static async deleteEnvironment(envId) {
// //     return this.request(`/environments/${envId}`, {
// //       method: 'DELETE'
// //     });
// //   }

// //   // Model management
// //   static async getModels(envId) {
// //     const response = await this.request(`/environments/${envId}/models`);
// //     return response.items || response;
// //   }

// //   static async getModel(envId, modelId) {
// //     return this.request(`/environments/${envId}/models/${modelId}`);
// //   }

// //   static async createModel(envId, data) {
// //     return this.request(`/environments/${envId}/models`, {
// //       method: 'POST',
// //       body: JSON.stringify(data)
// //     });
// //   }

// //   static async updateModel(envId, modelId, data) {
// //     return this.request(`/environments/${envId}/models/${modelId}`, {
// //       method: 'PUT',
// //       body: JSON.stringify(data)
// //     });
// //   }

// //   static async deleteModel(envId, modelId) {
// //     return this.request(`/environments/${envId}/models/${modelId}`, {
// //       method: 'DELETE'
// //     });
// //   }

// //   // Digital twin management
// //   static async getTwins(envId) {
// //     const response = await this.request(`/environments/${envId}/twins`);
// //     return response.items || response;
// //   }

// //   static async getTwin(envId, twinId) {
// //     return this.request(`/environments/${envId}/twins/${twinId}`);
// //   }

// //   static async createTwin(envId, data) {
// //     return this.request(`/environments/${envId}/twins`, {
// //       method: 'POST',
// //       body: JSON.stringify(data)
// //     });
// //   }

// //   static async updateTwin(envId, twinId, data) {
// //     return this.request(`/environments/${envId}/twins/${twinId}`, {
// //       method: 'PUT',
// //       body: JSON.stringify(data)
// //     });
// //   }

// //   static async deleteTwin(envId, twinId) {
// //     return this.request(`/environments/${envId}/twins/${twinId}`, {
// //       method: 'DELETE'
// //     });
// //   }

// //   // Device management
// //   static async getDevices(envId) {
// //     const response = await this.request(`/environments/${envId}/devices`);
// //     return response.items || response;
// //   }

// //   static async getDevice(envId, deviceId) {
// //     return this.request(`/environments/${envId}/devices/${deviceId}`);
// //   }

// //   static async createDevice(envId, data) {
// //     return this.request(`/environments/${envId}/devices`, {
// //       method: 'POST',
// //       body: JSON.stringify(data)
// //     });
// //   }

// //   static async updateDevice(envId, deviceId, data) {
// //     return this.request(`/environments/${envId}/devices/${deviceId}`, {
// //       method: 'PUT',
// //       body: JSON.stringify(data)
// //     });
// //   }

// //   static async deleteDevice(envId, deviceId) {
// //     return this.request(`/environments/${envId}/devices/${deviceId}`, {
// //       method: 'DELETE'
// //     });
// //   }

// //   // Relationship management - MongoDB only
// //   static async getRelationships(envId) {
// //     const response = await this.request(`/environments/${envId}/relationships`);
// //     return response.items || response;
// //   }

// //   static async createRelationship(envId, data) {
// //     return this.request(`/environments/${envId}/relationships`, {
// //       method: 'POST',
// //       body: JSON.stringify(data)
// //     });
// //   }

// //   static async deleteRelationship(envId, sourceId, relationshipName, targetId) {
// //     return this.request(`/environments/${envId}/relationships/${sourceId}/${relationshipName}/${targetId}`, {
// //       method: 'DELETE'
// //     });
// //   }

// //   // Advanced relationship features
// //   static async getRelationshipStats(envId) {
// //     return this.request(`/environments/${envId}/relationships/stats`);
// //   }

// //   static async findPath(envId, sourceId, targetId, maxDepth = 5) {
// //     return this.request(`/environments/${envId}/relationships/path/${sourceId}/${targetId}?max_depth=${maxDepth}`);
// //   }

// //   static async getRelationshipGraph(envId, maxDepth = 3) {
// //     return this.request(`/environments/${envId}/relationships/graph?max_depth=${maxDepth}`);
// //   }
// // }

// // // Generic modal component
// // const Modal = ({ isOpen, onClose, title, children, size = 'medium' }) => {
// //   if (!isOpen) return null;

// //   const modalStyle = size === 'large' ? styles.modalContentLarge : styles.modalContent;

// //   return (
// //     <div style={styles.modal}>
// //       <div style={modalStyle}>
// //         <div style={styles.modalHeader}>
// //           <h2 style={styles.modalTitle}>{title}</h2>
// //           <button onClick={onClose} style={styles.closeButton}>
// //             <X style={{width: '24px', height: '24px'}} />
// //           </button>
// //         </div>
// //         {children}
// //       </div>
// //     </div>
// //   );
// // };

// // // JSON editor component
// // const JsonEditor = ({ value, onChange, placeholder = "Enter valid JSON..." }) => {
// //   const [isValid, setIsValid] = useState(true);
// //   const [error, setError] = useState('');

// //   const handleChange = (e) => {
// //     const newValue = e.target.value;
// //     onChange(newValue);
    
// //     if (!newValue.trim()) {
// //       setIsValid(true);
// //       setError('');
// //       return;
// //     }

// //     try {
// //       JSON.parse(newValue);
// //       setIsValid(true);
// //       setError('');
// //     } catch (err) {
// //       setIsValid(false);
// //       setError(err.message);
// //     }
// //   };

// //   return (
// //     <div>
// //       <textarea
// //         value={value}
// //         onChange={handleChange}
// //         style={{
// //           ...styles.jsonEditor,
// //           borderColor: isValid ? '#d1d5db' : '#dc2626'
// //         }}
// //         placeholder={placeholder}
// //       />
// //       {!isValid && (
// //         <div style={styles.jsonError}>
// //           <AlertCircle style={{width: '16px', height: '16px', marginRight: '4px'}} />
// //           JSON format error: {error}
// //         </div>
// //       )}
// //     </div>
// //   );
// // };

// // // Enhanced Graph visualization component for MongoDB-only architecture
// // const TwinGraphVisualizer = ({ selectedEnvironment }) => {
// //   const [twins, setTwins] = useState([]);
// //   const [relationships, setRelationships] = useState([]);
// //   const [models, setModels] = useState([]);
// //   const [loading, setLoading] = useState(false);
// //   const [submitting, setSubmitting] = useState(false);
// //   const [error, setError] = useState(null);
// //   const [stats, setStats] = useState(null);
  
// //   // UI state
// //   const [selectedNode, setSelectedNode] = useState(null);
// //   const [showCreateTwinForm, setShowCreateTwinForm] = useState(false);
// //   const [showCreateRelForm, setShowCreateRelForm] = useState(false);
// //   const [showDetailModal, setShowDetailModal] = useState(false);
// //   const [showContextMenu, setShowContextMenu] = useState(false);
// //   const [showPathFinder, setShowPathFinder] = useState(false);
// //   const [contextMenuPos, setContextMenuPos] = useState({ x: 0, y: 0 });
// //   const [contextMenuNode, setContextMenuNode] = useState(null);
  
// //   // Path finder state
// //   const [pathSource, setPathSource] = useState('');
// //   const [pathTarget, setPathTarget] = useState('');
// //   const [foundPath, setFoundPath] = useState([]);
// //   const [highlightedPath, setHighlightedPath] = useState(new Set());
  
// //   // Form data
// //   const [twinFormData, setTwinFormData] = useState({
// //     twin_id: '',
// //     model_id: '',
// //     properties: '{}'
// //   });
  
// //   const [relFormData, setRelFormData] = useState({
// //     source_twin_id: '',
// //     target_twin_id: '',
// //     relationship_name: ''
// //   });

// //   // Refs for D3
// //   const svgRef = useRef(null);
// //   const containerRef = useRef(null);
// //   const simulationRef = useRef(null);

// //   useEffect(() => {
// //     if (selectedEnvironment) {
// //       loadData();
// //       loadStats();
// //     }
// //   }, [selectedEnvironment]);

// //   useEffect(() => {
// //     if (twins.length > 0 && containerRef.current) {
// //       initializeGraph();
// //     }
// //   }, [twins, relationships, highlightedPath]);

// //   // Handle window resize
// //   useEffect(() => {
// //     const handleResize = () => {
// //       if (twins.length > 0 && containerRef.current) {
// //         initializeGraph();
// //       }
// //     };
    
// //     window.addEventListener('resize', handleResize);
// //     return () => window.removeEventListener('resize', handleResize);
// //   }, [twins, relationships, highlightedPath]);

// //   // Close context menu when clicking outside
// //   useEffect(() => {
// //     const handleClickOutside = () => setShowContextMenu(false);
// //     document.addEventListener('click', handleClickOutside);
// //     return () => document.removeEventListener('click', handleClickOutside);
// //   }, []);

// //   const loadData = async () => {
// //     try {
// //       setLoading(true);
// //       setError(null);
      
// //       const [twinsData, relationshipsData, modelsData] = await Promise.all([
// //         DigitalTwinAPI.getTwins(selectedEnvironment.environment_id),
// //         DigitalTwinAPI.getRelationships(selectedEnvironment.environment_id),
// //         DigitalTwinAPI.getModels(selectedEnvironment.environment_id)
// //       ]);
      
// //       setTwins(Array.isArray(twinsData) ? twinsData : []);
// //       setRelationships(Array.isArray(relationshipsData) ? relationshipsData : []);
// //       setModels(Array.isArray(modelsData) ? modelsData : []);
// //     } catch (err) {
// //       setError('Failed to load data: ' + err.message);
// //     } finally {
// //       setLoading(false);
// //     }
// //   };

// //   const loadStats = async () => {
// //     try {
// //       const statsData = await DigitalTwinAPI.getRelationshipStats(selectedEnvironment.environment_id);
// //       setStats(statsData);
// //     } catch (err) {
// //       console.error('Failed to load statistics:', err);
// //     }
// //   };

// //   const findPath = async () => {
// //     if (!pathSource || !pathTarget || pathSource === pathTarget) {
// //       alert('Please select different source and target twins');
// //       return;
// //     }

// //     try {
// //       const path = await DigitalTwinAPI.findPath(selectedEnvironment.environment_id, pathSource, pathTarget);
// //       setFoundPath(path);
      
// //       // Highlight the path
// //       const pathIds = new Set();
// //       path.forEach(rel => {
// //         pathIds.add(`${rel.source_twin_id}-${rel.target_twin_id}-${rel.relationship_name}`);
// //       });
// //       setHighlightedPath(pathIds);
      
// //       if (path.length === 0) {
// //         alert('No path found between the selected twins');
// //       }
// //     } catch (err) {
// //       alert('Failed to find path: ' + err.message);
// //     }
// //   };

// //   const clearPath = () => {
// //     setFoundPath([]);
// //     setHighlightedPath(new Set());
// //     setPathSource('');
// //     setPathTarget('');
// //   };

// //   const initializeGraph = () => {
// //     if (!containerRef.current) return;
    
// //     const svg = d3.select(svgRef.current);
// //     svg.selectAll("*").remove();

// //     // Get container dimensions dynamically
// //     const containerRect = containerRef.current.getBoundingClientRect();
// //     const width = containerRect.width;
// //     const height = containerRect.height;
    
// //     svg.attr("width", width).attr("height", height);

// //     // Create zoom behavior
// //     const zoom = d3.zoom()
// //       .scaleExtent([0.1, 4])
// //       .on("zoom", (event) => {
// //         container.attr("transform", event.transform);
// //       });

// //     svg.call(zoom);

// //     const container = svg.append("g");

// //     // Create force simulation
// //     const simulation = d3.forceSimulation(twins)
// //       .force("link", d3.forceLink(relationships)
// //         .id(d => d.twin_id)
// //         .distance(120))
// //       .force("charge", d3.forceManyBody().strength(-400))
// //       .force("center", d3.forceCenter(width / 2, height / 2))
// //       .force("collision", d3.forceCollide().radius(35));

// //     simulationRef.current = simulation;

// //     // Create arrowhead marker
// //     svg.append("defs").append("marker")
// //       .attr("id", "arrowhead")
// //       .attr("viewBox", "0 -5 10 10")
// //       .attr("refX", 25)
// //       .attr("refY", 0)
// //       .attr("markerWidth", 6)
// //       .attr("markerHeight", 6)
// //       .attr("orient", "auto")
// //       .append("path")
// //       .attr("d", "M0,-5L10,0L0,5")
// //       .attr("fill", "#999");

// //     // Create highlighted arrowhead marker
// //     svg.append("defs").append("marker")
// //       .attr("id", "arrowhead-highlight")
// //       .attr("viewBox", "0 -5 10 10")
// //       .attr("refX", 25)
// //       .attr("refY", 0)
// //       .attr("markerWidth", 6)
// //       .attr("markerHeight", 6)
// //       .attr("orient", "auto")
// //       .append("path")
// //       .attr("d", "M0,-5L10,0L0,5")
// //       .attr("fill", "#f59e0b");

// //     // Create links
// //     const link = container.append("g")
// //       .selectAll("line")
// //       .data(relationships)
// //       .enter().append("line")
// //       .attr("stroke", d => {
// //         const pathId = `${d.source_twin_id}-${d.target_twin_id}-${d.relationship_name}`;
// //         return highlightedPath.has(pathId) ? "#f59e0b" : "#999";
// //       })
// //       .attr("stroke-opacity", d => {
// //         const pathId = `${d.source_twin_id}-${d.target_twin_id}-${d.relationship_name}`;
// //         return highlightedPath.has(pathId) ? 0.9 : 0.6;
// //       })
// //       .attr("stroke-width", d => {
// //         const pathId = `${d.source_twin_id}-${d.target_twin_id}-${d.relationship_name}`;
// //         return highlightedPath.has(pathId) ? 3 : 2;
// //       })
// //       .attr("marker-end", d => {
// //         const pathId = `${d.source_twin_id}-${d.target_twin_id}-${d.relationship_name}`;
// //         return highlightedPath.has(pathId) ? "url(#arrowhead-highlight)" : "url(#arrowhead)";
// //       });

// //     // Create link labels
// //     const linkLabel = container.append("g")
// //       .selectAll("text")
// //       .data(relationships)
// //       .enter().append("text")
// //       .attr("font-size", "11px")
// //       .attr("fill", d => {
// //         const pathId = `${d.source_twin_id}-${d.target_twin_id}-${d.relationship_name}`;
// //         return highlightedPath.has(pathId) ? "#f59e0b" : "#666";
// //       })
// //       .attr("font-weight", d => {
// //         const pathId = `${d.source_twin_id}-${d.target_twin_id}-${d.relationship_name}`;
// //         return highlightedPath.has(pathId) ? "600" : "normal";
// //       })
// //       .attr("text-anchor", "middle")
// //       .attr("dy", "-5px")
// //       .style("pointer-events", "none")
// //       .text(d => d.relationship_name);

// //     // Create node groups
// //     const nodeGroup = container.append("g")
// //       .selectAll("g")
// //       .data(twins)
// //       .enter().append("g")
// //       .call(d3.drag()
// //         .on("start", dragStarted)
// //         .on("drag", dragged)
// //         .on("end", dragEnded));

// //     // Create circles for nodes
// //     const node = nodeGroup.append("circle")
// //       .attr("r", 22)
// //       .attr("fill", d => getNodeColor(d.model_id))
// //       .attr("stroke", d => {
// //         if (selectedNode?.twin_id === d.twin_id) return "#2563eb";
// //         if (pathSource === d.twin_id) return "#10b981";
// //         if (pathTarget === d.twin_id) return "#ef4444";
// //         return "#fff";
// //       })
// //       .attr("stroke-width", d => {
// //         if (selectedNode?.twin_id === d.twin_id || pathSource === d.twin_id || pathTarget === d.twin_id) return 3;
// //         return 2;
// //       })
// //       .style("cursor", "pointer")
// //       .on("click", handleNodeClick)
// //       .on("contextmenu", handleNodeRightClick)
// //       .on("mouseover", function(event, d) {
// //         d3.select(this).attr("stroke-width", 3);
// //       })
// //       .on("mouseout", function(event, d) {
// //         if (selectedNode?.twin_id !== d.twin_id && pathSource !== d.twin_id && pathTarget !== d.twin_id) {
// //           d3.select(this).attr("stroke-width", 2);
// //         }
// //       });

// //     // Create labels for nodes
// //     const nodeLabel = nodeGroup.append("text")
// //       .attr("dx", 0)
// //       .attr("dy", 35)
// //       .attr("text-anchor", "middle")
// //       .attr("font-size", "12px")
// //       .attr("font-weight", "500")
// //       .attr("fill", "#333")
// //       .style("pointer-events", "none")
// //       .text(d => d.twin_id);

// //     // Update positions on simulation tick
// //     simulation.on("tick", () => {
// //       link
// //         .attr("x1", d => d.source.x)
// //         .attr("y1", d => d.source.y)
// //         .attr("x2", d => d.target.x)
// //         .attr("y2", d => d.target.y);

// //       linkLabel
// //         .attr("x", d => (d.source.x + d.target.x) / 2)
// //         .attr("y", d => (d.source.y + d.target.y) / 2);

// //       nodeGroup
// //         .attr("transform", d => `translate(${d.x},${d.y})`);
// //     });

// //     function dragStarted(event, d) {
// //       if (!event.active) simulation.alphaTarget(0.3).restart();
// //       d.fx = d.x;
// //       d.fy = d.y;
// //     }

// //     function dragged(event, d) {
// //       d.fx = event.x;
// //       d.fy = event.y;
// //     }

// //     function dragEnded(event, d) {
// //       if (!event.active) simulation.alphaTarget(0);
// //       d.fx = null;
// //       d.fy = null;
// //     }

// //     function handleNodeClick(event, d) {
// //       event.stopPropagation();
// //       setSelectedNode(d);
// //       setShowContextMenu(false);
      
// //       // Update visual selection
// //       container.selectAll("circle")
// //         .attr("stroke", node => {
// //           if (d.twin_id === node.twin_id) return "#2563eb";
// //           if (pathSource === node.twin_id) return "#10b981";
// //           if (pathTarget === node.twin_id) return "#ef4444";
// //           return "#fff";
// //         })
// //         .attr("stroke-width", node => {
// //           if (d.twin_id === node.twin_id || pathSource === node.twin_id || pathTarget === node.twin_id) return 3;
// //           return 2;
// //         });
// //     }

// //     function handleNodeRightClick(event, d) {
// //       event.preventDefault();
// //       event.stopPropagation();
      
// //       const svgRect = svg.node().getBoundingClientRect();
// //       setContextMenuPos({
// //         x: event.clientX - svgRect.left,
// //         y: event.clientY - svgRect.top
// //       });
// //       setContextMenuNode(d);
// //       setShowContextMenu(true);
// //     }
// //   };

// //   const getNodeColor = (modelId) => {
// //     const colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4'];
// //     const modelList = [...new Set(twins.map(t => t.model_id))];
// //     const index = modelList.indexOf(modelId) % colors.length;
// //     return colors[index];
// //   };

// //   // Twin management functions
// //   const handleCreateTwin = async () => {
// //     try {
// //       setSubmitting(true);
      
// //       let properties = {};
// //       if (twinFormData.properties.trim()) {
// //         try {
// //           properties = JSON.parse(twinFormData.properties);
// //         } catch (err) {
// //           throw new Error('Property JSON format error: ' + err.message);
// //         }
// //       }

// //       const twinData = {
// //         twin_id: twinFormData.twin_id,
// //         model_id: twinFormData.model_id,
// //         properties: properties
// //       };

// //       await DigitalTwinAPI.createTwin(selectedEnvironment.environment_id, twinData);
// //       setShowCreateTwinForm(false);
// //       setTwinFormData({ twin_id: '', model_id: '', properties: '{}' });
// //       await loadData();
// //       await loadStats();
// //     } catch (err) {
// //       alert('Failed to create digital twin: ' + err.message);
// //     } finally {
// //       setSubmitting(false);
// //     }
// //   };

// //   const handleDeleteTwin = async (twinId) => {
// //     if (window.confirm('Are you sure you want to delete this digital twin? This will also delete related relationships.')) {
// //       try {
// //         await DigitalTwinAPI.deleteTwin(selectedEnvironment.environment_id, twinId);
// //         setSelectedNode(null);
// //         await loadData();
// //         await loadStats();
// //       } catch (err) {
// //         alert('Failed to delete digital twin: ' + err.message);
// //       }
// //     }
// //   };

// //   const handleCreateRelationship = async () => {
// //     try {
// //       setSubmitting(true);
// //       await DigitalTwinAPI.createRelationship(selectedEnvironment.environment_id, relFormData);
// //       setShowCreateRelForm(false);
// //       setRelFormData({ source_twin_id: '', target_twin_id: '', relationship_name: '' });
// //       await loadData();
// //       await loadStats();
// //     } catch (err) {
// //       alert('Failed to create relationship: ' + err.message);
// //     } finally {
// //       setSubmitting(false);
// //     }
// //   };

// //   const handleDeleteRelationship = async (relationship) => {
// //     if (window.confirm('Are you sure you want to delete this relationship?')) {
// //       try {
// //         await DigitalTwinAPI.deleteRelationship(
// //           selectedEnvironment.environment_id, 
// //           relationship.source_twin_id,
// //           relationship.relationship_name,
// //           relationship.target_twin_id
// //         );
// //         await loadData();
// //         await loadStats();
// //       } catch (err) {
// //         alert('Failed to delete relationship: ' + err.message);
// //       }
// //     }
// //   };

// //   const handleViewTwinDetails = async (twin) => {
// //     try {
// //       const fullTwin = await DigitalTwinAPI.getTwin(selectedEnvironment.environment_id, twin.twin_id);
// //       setSelectedNode(fullTwin);
// //       setShowDetailModal(true);
// //     } catch (err) {
// //       alert('Failed to get twin details: ' + err.message);
// //     }
// //   };

// //   if (!selectedEnvironment) {
// //     return (
// //       <div style={styles.emptyState}>
// //         <Network style={styles.emptyStateIcon} />
// //         <p>Please select an environment first</p>
// //       </div>
// //     );
// //   }

// //   return (
// //     <div style={{display: 'flex', height: 'calc(100vh - 160px)', minHeight: '600px'}}>
// //       {/* Enhanced Left sidebar */}
// //       <div style={{
// //         width: '320px',
// //         minWidth: '320px',
// //         backgroundColor: '#f8fafc',
// //         borderRight: '1px solid #e2e8f0',
// //         display: 'flex',
// //         flexDirection: 'column'
// //       }}>
// //         {/* Header section */}
// //         <div style={{
// //           padding: '16px',
// //           borderBottom: '1px solid #e2e8f0',
// //           backgroundColor: '#ffffff'
// //         }}>
// //           <h2 style={{margin: '0 0 16px 0', fontSize: '18px', fontWeight: '600'}}>
// //             Digital Twin Graph
// //           </h2>
          
// //           <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
// //             <button
// //               onClick={() => setShowCreateTwinForm(true)}
// //               style={{
// //                 ...styles.primaryButton,
// //                 width: '100%',
// //                 fontSize: '12px',
// //                 padding: '8px 12px',
// //                 opacity: models.length === 0 ? 0.5 : 1
// //               }}
// //               disabled={models.length === 0}
// //             >
// //               <Plus style={{width: '14px', height: '14px', marginRight: '6px'}} />
// //               Create Twin
// //             </button>

// //             <button
// //               onClick={() => setShowPathFinder(!showPathFinder)}
// //               style={{
// //                 ...styles.primaryButton,
// //                 width: '100%',
// //                 fontSize: '12px',
// //                 padding: '8px 12px',
// //                 backgroundColor: showPathFinder ? '#dc2626' : '#8b5cf6',
// //                 opacity: twins.length < 2 ? 0.5 : 1
// //               }}
// //               disabled={twins.length < 2}
// //             >
// //               <Search style={{width: '14px', height: '14px', marginRight: '6px'}} />
// //               {showPathFinder ? 'Close Path Finder' : 'Find Path'}
// //             </button>

// //             {highlightedPath.size > 0 && (
// //               <button
// //                 onClick={clearPath}
// //                 style={{
// //                   ...styles.primaryButton,
// //                   width: '100%',
// //                   fontSize: '12px',
// //                   padding: '8px 12px',
// //                   backgroundColor: '#f59e0b'
// //                 }}
// //               >
// //                 <X style={{width: '14px', height: '14px', marginRight: '6px'}} />
// //                 Clear Path
// //               </button>
// //             )}
// //           </div>
// //         </div>

// //         {/* Statistics section */}
// //         {stats && (
// //           <div style={{
// //             padding: '16px',
// //             borderBottom: '1px solid #e2e8f0',
// //             backgroundColor: '#ffffff'
// //           }}>
// //             <h3 style={{margin: '0 0 12px 0', fontSize: '14px', fontWeight: '600'}}>
// //               Graph Statistics
// //             </h3>
// //             <div style={{fontSize: '12px', color: '#6b7280'}}>
// //               <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '4px'}}>
// //                 <span>Digital Twins:</span>
// //                 <span style={{fontWeight: '500', color: '#1f2937'}}>{twins.length}</span>
// //               </div>
// //               <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '4px'}}>
// //                 <span>Relationships:</span>
// //                 <span style={{fontWeight: '500', color: '#1f2937'}}>{stats.total_relationships}</span>
// //               </div>
// //               <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '4px'}}>
// //                 <span>Connected Twins:</span>
// //                 <span style={{fontWeight: '500', color: '#1f2937'}}>{stats.unique_twins_count}</span>
// //               </div>
// //               <div style={{display: 'flex', justifyContent: 'space-between'}}>
// //                 <span>Relation Types:</span>
// //                 <span style={{fontWeight: '500', color: '#1f2937'}}>{stats.relationship_types_count}</span>
// //               </div>
// //             </div>
// //           </div>
// //         )}

// //         {/* Path finder section */}
// //         {showPathFinder && (
// //           <div style={{
// //             padding: '16px',
// //             borderBottom: '1px solid #e2e8f0',
// //             backgroundColor: '#fef3c7'
// //           }}>
// //             <h3 style={{margin: '0 0 12px 0', fontSize: '14px', fontWeight: '600', color: '#92400e'}}>
// //               Path Finder
// //             </h3>
// //             <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
// //               <select
// //                 value={pathSource}
// //                 onChange={(e) => setPathSource(e.target.value)}
// //                 style={{...styles.formInput, fontSize: '12px', padding: '6px 8px'}}
// //               >
// //                 <option value="">Select source twin</option>
// //                 {twins.map(twin => (
// //                   <option key={twin.twin_id} value={twin.twin_id}>
// //                     {twin.twin_id}
// //                   </option>
// //                 ))}
// //               </select>
              
// //               <select
// //                 value={pathTarget}
// //                 onChange={(e) => setPathTarget(e.target.value)}
// //                 style={{...styles.formInput, fontSize: '12px', padding: '6px 8px'}}
// //               >
// //                 <option value="">Select target twin</option>
// //                 {twins.filter(t => t.twin_id !== pathSource).map(twin => (
// //                   <option key={twin.twin_id} value={twin.twin_id}>
// //                     {twin.twin_id}
// //                   </option>
// //                 ))}
// //               </select>

// //               <button
// //                 onClick={findPath}
// //                 style={{
// //                   ...styles.primaryButton,
// //                   width: '100%',
// //                   fontSize: '11px',
// //                   padding: '6px 8px',
// //                   backgroundColor: '#059669',
// //                   opacity: (!pathSource || !pathTarget || pathSource === pathTarget) ? 0.5 : 1
// //                 }}
// //                 disabled={!pathSource || !pathTarget || pathSource === pathTarget}
// //               >
// //                 Find Path
// //               </button>

// //               {foundPath.length > 0 && (
// //                 <div style={{
// //                   marginTop: '8px',
// //                   padding: '8px',
// //                   backgroundColor: '#ecfdf5',
// //                   borderRadius: '4px',
// //                   fontSize: '11px'
// //                 }}>
// //                   <div style={{fontWeight: '500', color: '#065f46', marginBottom: '4px'}}>
// //                     Path found ({foundPath.length} steps):
// //                   </div>
// //                   {foundPath.map((rel, index) => (
// //                     <div key={index} style={{color: '#047857'}}>
// //                       {index + 1}. {rel.source_twin_id} → {rel.target_twin_id}
// //                       <span style={{color: '#6b7280'}}> ({rel.relationship_name})</span>
// //                     </div>
// //                   ))}
// //                 </div>
// //               )}
// //             </div>
// //           </div>
// //         )}

// //         {/* Node details panel */}
// //         <div style={{flex: 1, overflow: 'auto', padding: '16px'}}>
// //           {selectedNode ? (
// //             <div>
// //               <h3 style={{margin: '0 0 12px 0', fontSize: '16px'}}>Selected Twin</h3>
// //               <div style={{
// //                 backgroundColor: '#ffffff',
// //                 padding: '12px',
// //                 borderRadius: '6px',
// //                 boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
// //                 border: '2px solid #2563eb'
// //               }}>
// //                 <p style={{margin: '0 0 8px 0', fontWeight: '600', color: '#2563eb'}}>{selectedNode.twin_id}</p>
// //                 <p style={{margin: '0 0 8px 0', fontSize: '12px', color: '#6b7280'}}>
// //                   Model: {selectedNode.model_id}
// //                 </p>
// //                 <p style={{margin: '0 0 12px 0', fontSize: '12px', color: '#6b7280'}}>
// //                   Properties: {Object.keys(selectedNode.properties || {}).length}
// //                 </p>
                
// //                 <div style={{display: 'flex', gap: '8px', flexWrap: 'wrap'}}>
// //                   <button
// //                     onClick={() => handleViewTwinDetails(selectedNode)}
// //                     style={{
// //                       ...styles.actionButton,
// //                       ...styles.actionButtonBlue,
// //                       fontSize: '12px',
// //                       padding: '6px 8px'
// //                     }}
// //                   >
// //                     <Eye style={{width: '12px', height: '12px', marginRight: '4px'}} />
// //                     Details
// //                   </button>
// //                   <button
// //                     onClick={() => {
// //                       setPathSource(selectedNode.twin_id);
// //                       setShowPathFinder(true);
// //                     }}
// //                     style={{
// //                       ...styles.actionButton,
// //                       ...styles.actionButtonGray,
// //                       fontSize: '12px',
// //                       padding: '6px 8px'
// //                     }}
// //                   >
// //                     <Search style={{width: '12px', height: '12px', marginRight: '4px'}} />
// //                     Path From
// //                   </button>
// //                   <button
// //                     onClick={() => handleDeleteTwin(selectedNode.twin_id)}
// //                     style={{
// //                       ...styles.actionButton,
// //                       ...styles.actionButtonRed,
// //                       fontSize: '12px',
// //                       padding: '6px 8px'
// //                     }}
// //                   >
// //                     <Trash2 style={{width: '12px', height: '12px', marginRight: '4px'}} />
// //                     Delete
// //                   </button>
// //                 </div>
// //               </div>
// //             </div>
// //           ) : (
// //             <div style={{textAlign: 'center', color: '#6b7280', fontSize: '14px'}}>
// //               <Network style={{width: '32px', height: '32px', margin: '0 auto 8px'}} />
// //               <p>Click a twin to view details</p>
// //               <div style={{
// //                 marginTop: '12px',
// //                 padding: '12px',
// //                 backgroundColor: '#e0f2fe',
// //                 borderRadius: '6px',
// //                 fontSize: '12px',
// //                 color: '#0c4a6e',
// //                 textAlign: 'left'
// //               }}>
// //                 <p style={{margin: '0 0 4px 0', fontWeight: '500'}}>Legend:</p>
// //                 <p style={{margin: 0}}>
// //                   🔵 Selected twin<br/>
// //                   🟢 Path source<br/>
// //                   🔴 Path target<br/>
// //                   🟡 Highlighted path
// //                 </p>
// //               </div>
// //             </div>
// //           )}

// //           {/* Relationships list */}
// //           <div style={{marginTop: '24px'}}>
// //             <h3 style={{margin: '0 0 12px 0', fontSize: '16px'}}>
// //               Relationships ({relationships.length})
// //             </h3>
// //             {relationships.length === 0 ? (
// //               <p style={{fontSize: '12px', color: '#6b7280', textAlign: 'center'}}>
// //                 No relationships yet. Right-click on any twin to create relationships.
// //               </p>
// //             ) : (
// //               <div style={{maxHeight: '200px', overflow: 'auto'}}>
// //                 {relationships.map((rel, index) => {
// //                   const pathId = `${rel.source_twin_id}-${rel.target_twin_id}-${rel.relationship_name}`;
// //                   const isHighlighted = highlightedPath.has(pathId);
                  
// //                   return (
// //                     <div key={index} style={{
// //                       backgroundColor: isHighlighted ? '#fef3c7' : '#ffffff',
// //                       padding: '8px',
// //                       marginBottom: '8px',
// //                       borderRadius: '4px',
// //                       fontSize: '12px',
// //                       boxShadow: '0 1px 2px rgba(0,0,0,0.05)',
// //                       border: isHighlighted ? '1px solid #f59e0b' : 'none'
// //                     }}>
// //                       <div style={{fontWeight: '500', color: isHighlighted ? '#92400e' : '#1f2937'}}>
// //                         {rel.source_twin_id} → {rel.target_twin_id}
// //                       </div>
// //                       <div style={{color: isHighlighted ? '#f59e0b' : '#2563eb', margin: '2px 0'}}>
// //                         {rel.relationship_name}
// //                       </div>
// //                       <button
// //                         onClick={() => handleDeleteRelationship(rel)}
// //                         style={{
// //                           ...styles.actionButton,
// //                           ...styles.actionButtonRed,
// //                           fontSize: '10px',
// //                           padding: '2px 6px',
// //                           marginTop: '4px'
// //                         }}
// //                       >
// //                         <Trash2 style={{width: '10px', height: '10px'}} />
// //                       </button>
// //                     </div>
// //                   );
// //                 })}
// //               </div>
// //             )}
// //           </div>
// //         </div>
// //       </div>

// //       {/* Enhanced Main graph area */}
// //       <div ref={containerRef} style={{flex: 1, position: 'relative', overflow: 'hidden'}}>
// //         {error && (
// //           <div style={{
// //             ...styles.errorMessage,
// //             position: 'absolute',
// //             top: '16px',
// //             left: '16px',
// //             right: '16px',
// //             zIndex: 10
// //           }}>
// //             <span style={styles.errorText}>{error}</span>
// //             <button onClick={() => setError(null)} style={{...styles.closeButton, color: '#dc2626'}}>
// //               <X style={{width: '16px', height: '16px'}} />
// //             </button>
// //           </div>
// //         )}

// //         {loading ? (
// //           <div style={{
// //             ...styles.loadingContainer,
// //             position: 'absolute',
// //             top: '50%',
// //             left: '50%',
// //             transform: 'translate(-50%, -50%)'
// //           }}>
// //             <div style={styles.spinner}></div>
// //             <span style={{marginLeft: '8px', color: '#6b7280'}}>Loading graph...</span>
// //           </div>
// //         ) : twins.length === 0 ? (
// //           <div style={{
// //             ...styles.emptyState,
// //             position: 'absolute',
// //             top: '50%',
// //             left: '50%',
// //             transform: 'translate(-50%, -50%)'
// //           }}>
// //             <Network style={styles.emptyStateIcon} />
// //             <p>No digital twins yet. Create your first twin to start building the graph.</p>
// //             {models.length === 0 && (
// //               <p style={{color: '#f59e0b', fontSize: '14px', marginTop: '8px'}}>
// //                 ⚠️ Create models first before adding twins
// //               </p>
// //             )}
// //           </div>
// //         ) : (
// //           <div style={{width: '100%', height: '100%', position: 'relative'}}>
// //             <svg ref={svgRef} style={{width: '100%', height: '100%', display: 'block'}} />
            
// //             {/* Enhanced Context menu */}
// //             {showContextMenu && contextMenuNode && (
// //               <div style={{
// //                 position: 'absolute',
// //                 left: contextMenuPos.x,
// //                 top: contextMenuPos.y,
// //                 backgroundColor: '#ffffff',
// //                 border: '1px solid #e2e8f0',
// //                 borderRadius: '8px',
// //                 boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
// //                 zIndex: 20,
// //                 minWidth: '200px',
// //                 overflow: 'hidden'
// //               }}>
// //                 {/* Context menu header */}
// //                 <div style={{
// //                   padding: '8px 12px',
// //                   backgroundColor: '#f8fafc',
// //                   borderBottom: '1px solid #e2e8f0',
// //                   fontSize: '12px',
// //                   fontWeight: '600',
// //                   color: '#6b7280'
// //                 }}>
// //                   Twin: {contextMenuNode.twin_id}
// //                 </div>

// //                 {/* Main actions */}
// //                 <button
// //                   onClick={() => {
// //                     handleViewTwinDetails(contextMenuNode);
// //                     setShowContextMenu(false);
// //                   }}
// //                   style={{
// //                     width: '100%',
// //                     padding: '10px 12px',
// //                     border: 'none',
// //                     backgroundColor: 'transparent',
// //                     textAlign: 'left',
// //                     fontSize: '14px',
// //                     cursor: 'pointer',
// //                     borderBottom: '1px solid #f3f4f6'
// //                   }}
// //                   onMouseEnter={(e) => e.target.style.backgroundColor = '#f8fafc'}
// //                   onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                 >
// //                   <Eye style={{width: '14px', height: '14px', marginRight: '8px', display: 'inline'}} />
// //                   View Details
// //                 </button>

// //                 {/* Path finder actions */}
// //                 <button
// //                   onClick={() => {
// //                     setPathSource(contextMenuNode.twin_id);
// //                     setShowPathFinder(true);
// //                     setShowContextMenu(false);
// //                   }}
// //                   style={{
// //                     width: '100%',
// //                     padding: '10px 12px',
// //                     border: 'none',
// //                     backgroundColor: 'transparent',
// //                     textAlign: 'left',
// //                     fontSize: '14px',
// //                     cursor: 'pointer',
// //                     borderBottom: '1px solid #f3f4f6'
// //                   }}
// //                   onMouseEnter={(e) => e.target.style.backgroundColor = '#ecfdf5'}
// //                   onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                 >
// //                   <Search style={{width: '14px', height: '14px', marginRight: '8px', display: 'inline'}} />
// //                   Set as Path Source
// //                 </button>
                
// //                 {pathSource && pathSource !== contextMenuNode.twin_id && (
// //                   <button
// //                     onClick={() => {
// //                       setPathTarget(contextMenuNode.twin_id);
// //                       findPath();
// //                       setShowContextMenu(false);
// //                     }}
// //                     style={{
// //                       width: '100%',
// //                       padding: '10px 12px',
// //                       border: 'none',
// //                       backgroundColor: 'transparent',
// //                       textAlign: 'left',
// //                       fontSize: '14px',
// //                       cursor: 'pointer',
// //                       borderBottom: '1px solid #f3f4f6'
// //                     }}
// //                     onMouseEnter={(e) => e.target.style.backgroundColor = '#fef2f2'}
// //                     onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                   >
// //                     <GitBranch style={{width: '14px', height: '14px', marginRight: '8px', display: 'inline'}} />
// //                     Find Path from {pathSource}
// //                   </button>
// //                 )}
                
// //                 {/* Create relationship options */}
// //                 {twins.filter(t => t.twin_id !== contextMenuNode.twin_id).length > 0 && (
// //                   <div style={{borderBottom: '1px solid #f3f4f6'}}>
// //                     <div style={{
// //                       padding: '8px 12px',
// //                       fontSize: '12px',
// //                       color: '#6b7280',
// //                       fontWeight: '500',
// //                       backgroundColor: '#f8fafc'
// //                     }}>
// //                       Create Relationship To:
// //                     </div>
// //                     {twins.filter(t => t.twin_id !== contextMenuNode.twin_id).slice(0, 4).map(targetTwin => (
// //                       <button
// //                         key={targetTwin.twin_id}
// //                         onClick={() => {
// //                           setRelFormData({
// //                             source_twin_id: contextMenuNode.twin_id,
// //                             target_twin_id: targetTwin.twin_id,
// //                             relationship_name: ''
// //                           });
// //                           setShowCreateRelForm(true);
// //                           setShowContextMenu(false);
// //                         }}
// //                         style={{
// //                           width: '100%',
// //                           padding: '8px 12px 8px 24px',
// //                           border: 'none',
// //                           backgroundColor: 'transparent',
// //                           textAlign: 'left',
// //                           fontSize: '13px',
// //                           cursor: 'pointer',
// //                           color: '#4b5563'
// //                         }}
// //                         onMouseEnter={(e) => e.target.style.backgroundColor = '#dbeafe'}
// //                         onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                       >
// //                         → {targetTwin.twin_id}
// //                       </button>
// //                     ))}
// //                     {twins.filter(t => t.twin_id !== contextMenuNode.twin_id).length > 4 && (
// //                       <div style={{
// //                         padding: '4px 12px',
// //                         fontSize: '11px',
// //                         color: '#9ca3af',
// //                         fontStyle: 'italic'
// //                       }}>
// //                         ... and {twins.filter(t => t.twin_id !== contextMenuNode.twin_id).length - 4} more
// //                       </div>
// //                     )}
// //                   </div>
// //                 )}
                
// //                 {/* Delete action */}
// //                 <button
// //                   onClick={() => {
// //                     handleDeleteTwin(contextMenuNode.twin_id);
// //                     setShowContextMenu(false);
// //                   }}
// //                   style={{
// //                     width: '100%',
// //                     padding: '10px 12px',
// //                     border: 'none',
// //                     backgroundColor: 'transparent',
// //                     textAlign: 'left',
// //                     fontSize: '14px',
// //                     cursor: 'pointer',
// //                     color: '#dc2626'
// //                   }}
// //                   onMouseEnter={(e) => e.target.style.backgroundColor = '#fef2f2'}
// //                   onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                 >
// //                   <Trash2 style={{width: '14px', height: '14px', marginRight: '8px', display: 'inline'}} />
// //                   Delete Twin
// //                 </button>
// //               </div>
// //             )}
            
// //             {/* Enhanced Graph controls */}
// //             <div style={{
// //               position: 'absolute',
// //               bottom: '16px',
// //               right: '16px',
// //               backgroundColor: '#ffffff',
// //               padding: '16px',
// //               borderRadius: '8px',
// //               boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
// //               minWidth: '200px'
// //             }}>
// //               <div style={{fontSize: '12px', color: '#6b7280', marginBottom: '12px', fontWeight: '600'}}>
// //                 Graph Overview
// //               </div>
// //               <div style={{fontSize: '12px', display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px'}}>
// //                 <div>
// //                   <div style={{color: '#6b7280'}}>Digital Twins</div>
// //                   <div style={{fontWeight: '600', color: '#1f2937', fontSize: '16px'}}>{twins.length}</div>
// //                 </div>
// //                 <div>
// //                   <div style={{color: '#6b7280'}}>Relationships</div>
// //                   <div style={{fontWeight: '600', color: '#1f2937', fontSize: '16px'}}>{relationships.length}</div>
// //                 </div>
// //                 <div>
// //                   <div style={{color: '#6b7280'}}>Models</div>
// //                   <div style={{fontWeight: '600', color: '#1f2937', fontSize: '16px'}}>{models.length}</div>
// //                 </div>
// //                 <div>
// //                   <div style={{color: '#6b7280'}}>Connected</div>
// //                   <div style={{fontWeight: '600', color: '#1f2937', fontSize: '16px'}}>
// //                     {stats ? stats.unique_twins_count : '...'}
// //                   </div>
// //                 </div>
// //               </div>
              
// //               {foundPath.length > 0 && (
// //                 <div style={{
// //                   marginTop: '12px',
// //                   padding: '8px',
// //                   backgroundColor: '#fef3c7',
// //                   borderRadius: '4px',
// //                   fontSize: '11px'
// //                 }}>
// //                   <div style={{fontWeight: '600', color: '#92400e'}}>
// //                     Active Path: {foundPath.length} hops
// //                   </div>
// //                   <div style={{color: '#a16207'}}>
// //                     {pathSource} → {pathTarget}
// //                   </div>
// //                 </div>
// //               )}
// //             </div>
// //           </div>
// //         )}
// //       </div>

// //       {/* Enhanced Create twin modal */}
// //       <Modal
// //         isOpen={showCreateTwinForm}
// //         onClose={() => setShowCreateTwinForm(false)}
// //         title="Create New Digital Twin"
// //         size="large"
// //       >
// //         <div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Twin ID *</label>
// //             <input
// //               type="text"
// //               value={twinFormData.twin_id}
// //               onChange={(e) => setTwinFormData({...twinFormData, twin_id: e.target.value})}
// //               style={styles.formInput}
// //               placeholder="e.g. sensor_001, controller_main"
// //               required
// //             />
// //             <div style={{fontSize: '11px', color: '#6b7280', marginTop: '2px'}}>
// //               Only letters, numbers, hyphens and underscores allowed
// //             </div>
// //           </div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Select Model *</label>
// //             <select
// //               value={twinFormData.model_id}
// //               onChange={(e) => setTwinFormData({...twinFormData, model_id: e.target.value})}
// //               style={styles.formInput}
// //               required
// //             >
// //               <option value="">Please select a model</option>
// //               {models.map(model => (
// //                 <option key={model.model_id} value={model.model_id}>
// //                   {model.display_name} ({model.model_id})
// //                 </option>
// //               ))}
// //             </select>
// //             {models.length === 0 && (
// //               <div style={{fontSize: '11px', color: '#f59e0b', marginTop: '2px'}}>
// //                 ⚠️ No models available. Create models first in the Models tab.
// //               </div>
// //             )}
// //           </div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Initial Property Values (JSON Format)</label>
// //             <JsonEditor
// //               value={twinFormData.properties}
// //               onChange={(value) => setTwinFormData({...twinFormData, properties: value})}
// //               placeholder={`{
// //   "temperature": 25.5,
// //   "status": "online",
// //   "location": {
// //     "x": 100,
// //     "y": 200
// //   }
// // }`}
// //             />
// //           </div>
// //           <div style={styles.formActions}>
// //             <button
// //               type="button"
// //               onClick={() => setShowCreateTwinForm(false)}
// //               style={styles.cancelButton}
// //               disabled={submitting}
// //             >
// //               Cancel
// //             </button>
// //             <button
// //               type="button"
// //               onClick={handleCreateTwin}
// //               style={{
// //                 ...styles.primaryButton,
// //                 opacity: (submitting || !twinFormData.twin_id || !twinFormData.model_id) ? 0.5 : 1
// //               }}
// //               disabled={submitting || !twinFormData.twin_id || !twinFormData.model_id}
// //             >
// //               {submitting ? 'Creating...' : 'Create Twin'}
// //             </button>
// //           </div>
// //         </div>
// //       </Modal>

// //       {/* Enhanced Create relationship modal */}
// //       <Modal
// //         isOpen={showCreateRelForm}
// //         onClose={() => setShowCreateRelForm(false)}
// //         title="Create New Relationship"
// //       >
// //         <div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Source Twin</label>
// //             <input
// //               type="text"
// //               value={relFormData.source_twin_id}
// //               style={{...styles.formInput, backgroundColor: '#f9fafb'}}
// //               disabled
// //             />
// //           </div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Target Twin</label>
// //             <input
// //               type="text"
// //               value={relFormData.target_twin_id}
// //               style={{...styles.formInput, backgroundColor: '#f9fafb'}}
// //               disabled
// //             />
// //           </div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Relationship Name *</label>
// //             <input
// //               type="text"
// //               value={relFormData.relationship_name}
// //               onChange={(e) => setRelFormData({...relFormData, relationship_name: e.target.value})}
// //               style={styles.formInput}
// //               placeholder="e.g.: connects_to, contains, controls, feeds_data_to"
// //               required
// //             />
// //             <div style={{fontSize: '11px', color: '#6b7280', marginTop: '2px'}}>
// //               Choose a descriptive name that explains how the source twin relates to the target twin
// //             </div>
// //           </div>

// //           {/* Relationship suggestions */}
// //           <div style={{marginBottom: '16px'}}>
// //             <label style={{...styles.formLabel, marginBottom: '8px'}}>Common Relationships:</label>
// //             <div style={{display: 'flex', flexWrap: 'wrap', gap: '6px'}}>
// //               {['connects_to', 'contains', 'controls', 'monitors', 'feeds_data_to', 'depends_on'].map(suggestion => (
// //                 <button
// //                   key={suggestion}
// //                   type="button"
// //                   onClick={() => setRelFormData({...relFormData, relationship_name: suggestion})}
// //                   style={{
// //                     padding: '4px 8px',
// //                     fontSize: '11px',
// //                     border: '1px solid #d1d5db',
// //                     borderRadius: '4px',
// //                     backgroundColor: relFormData.relationship_name === suggestion ? '#dbeafe' : '#ffffff',
// //                     color: relFormData.relationship_name === suggestion ? '#1d4ed8' : '#6b7280',
// //                     cursor: 'pointer'
// //                   }}
// //                 >
// //                   {suggestion}
// //                 </button>
// //               ))}
// //             </div>
// //           </div>

// //           <div style={styles.formActions}>
// //             <button
// //               type="button"
// //               onClick={() => setShowCreateRelForm(false)}
// //               style={styles.cancelButton}
// //               disabled={submitting}
// //             >
// //               Cancel
// //             </button>
// //             <button
// //               type="button"
// //               onClick={handleCreateRelationship}
// //               style={{
// //                 ...styles.primaryButton,
// //                 opacity: (submitting || !relFormData.relationship_name) ? 0.5 : 1
// //               }}
// //               disabled={submitting || !relFormData.relationship_name}
// //             >
// //               {submitting ? 'Creating...' : 'Create Relationship'}
// //             </button>
// //           </div>
// //         </div>
// //       </Modal>

// //       {/* Enhanced Twin details modal */}
// //       <Modal
// //         isOpen={showDetailModal}
// //         onClose={() => setShowDetailModal(false)}
// //         title="Digital Twin Details"
// //         size="large"
// //       >
// //         {selectedNode && (
// //           <div>
// //             {/* Basic Information */}
// //             <div style={styles.grid2}>
// //               <div>
// //                 <label style={styles.formLabel}>Twin ID</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px', fontWeight: '500'}}>{selectedNode.twin_id}</p>
// //               </div>
// //               <div>
// //                 <label style={styles.formLabel}>Model ID</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedNode.model_id}</p>
// //               </div>
// //               <div>
// //                 <label style={styles.formLabel}>Created</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedNode.created_at)}</p>
// //               </div>
// //               <div>
// //                 <label style={styles.formLabel}>Last Updated</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedNode.updated_at)}</p>
// //               </div>
// //             </div>

// //             {/* Properties */}
// //             <div style={{marginTop: '24px'}}>
// //               <label style={{...styles.formLabel, marginBottom: '8px'}}>Properties</label>
// //               <pre style={styles.preCode}>
// //                 {JSON.stringify(selectedNode.properties || {}, null, 2)}
// //               </pre>
// //             </div>

// //             {/* Related Relationships */}
// //             <div style={{marginTop: '24px'}}>
// //               <label style={{...styles.formLabel, marginBottom: '8px'}}>
// //                 Connected Relationships ({relationships.filter(r => 
// //                   r.source_twin_id === selectedNode.twin_id || r.target_twin_id === selectedNode.twin_id
// //                 ).length})
// //               </label>
// //               <div style={{maxHeight: '150px', overflow: 'auto'}}>
// //                 {relationships
// //                   .filter(r => r.source_twin_id === selectedNode.twin_id || r.target_twin_id === selectedNode.twin_id)
// //                   .map((rel, index) => (
// //                     <div key={index} style={{
// //                       backgroundColor: '#f8fafc',
// //                       padding: '8px 12px',
// //                       marginBottom: '4px',
// //                       borderRadius: '4px',
// //                       fontSize: '12px'
// //                     }}>
// //                       {rel.source_twin_id === selectedNode.twin_id ? (
// //                         <span>
// //                           <strong>{selectedNode.twin_id}</strong> 
// //                           <span style={{color: '#2563eb'}}> {rel.relationship_name} </span>
// //                           → {rel.target_twin_id}
// //                         </span>
// //                       ) : (
// //                         <span>
// //                           {rel.source_twin_id} 
// //                           <span style={{color: '#2563eb'}}> {rel.relationship_name} </span>
// //                           → <strong>{selectedNode.twin_id}</strong>
// //                         </span>
// //                       )}
// //                     </div>
// //                   ))}
// //               </div>
// //               {relationships.filter(r => 
// //                 r.source_twin_id === selectedNode.twin_id || r.target_twin_id === selectedNode.twin_id
// //               ).length === 0 && (
// //                 <p style={{fontSize: '12px', color: '#6b7280', fontStyle: 'italic'}}>
// //                   No relationships connected to this twin yet.
// //                 </p>
// //               )}
// //             </div>
// //           </div>
// //         )}
// //       </Modal>
// //     </div>
// //   );
// // };

// // // Main interface component
// // const DigitalTwinPlatform = () => {
// //   const [activeTab, setActiveTab] = useState('environments');
// //   const [selectedEnvironment, setSelectedEnvironment] = useState(null);
// //   const [environments, setEnvironments] = useState([]);
// //   const [loading, setLoading] = useState(false);
// //   const [error, setError] = useState(null);

// //   useEffect(() => {
// //     loadEnvironments();
// //   }, []);

// //   const loadEnvironments = async () => {
// //     try {
// //       setLoading(true);
// //       setError(null);
// //       const data = await DigitalTwinAPI.getEnvironments();
// //       setEnvironments(Array.isArray(data) ? data : []);
// //     } catch (err) {
// //       setError('Failed to load environments: ' + err.message);
// //       setEnvironments([]);
// //     } finally {
// //       setLoading(false);
// //     }
// //   };

// //   const tabs = [
// //     { id: 'environments', label: 'Environments', icon: Building },
// //     { id: 'models', label: 'Models', icon: Database },
// //     { id: 'graph', label: 'Graph Explorer', icon: Network },
// //     { id: 'devices', label: 'Devices', icon: Cpu },
// //   ];

// //   return (
// //     <div style={styles.app}>
// //       {/* Top navigation */}
// //       <header style={styles.header}>
// //         <div style={styles.headerContainer}>
// //           <div style={styles.headerContent}>
// //             <div style={styles.headerLeft}>
// //               <Network style={styles.logo} />
// //               <h1 style={styles.title}>Digital Twin Platform</h1>
// //             </div>
// //             <div>
// //               <select 
// //                 style={styles.select}
// //                 value={selectedEnvironment?.environment_id || ''}
// //                 onChange={(e) => {
// //                   const env = environments.find(env => env.environment_id === e.target.value);
// //                   setSelectedEnvironment(env);
// //                 }}
// //               >
// //                 <option value="">Select Environment</option>
// //                 {environments.map(env => (
// //                   <option key={env.environment_id} value={env.environment_id}>
// //                     {env.display_name}
// //                   </option>
// //                 ))}
// //               </select>
// //             </div>
// //           </div>
// //         </div>
// //       </header>

// //       <div style={styles.mainContainer}>
// //         {/* Error message */}
// //         {error && (
// //           <div style={styles.errorMessage}>
// //             <div style={styles.errorText}>
// //               <AlertCircle style={{width: '20px', height: '20px', marginRight: '8px'}} />
// //               <span>{error}</span>
// //             </div>
// //             <button 
// //               onClick={() => setError(null)}
// //               style={{...styles.closeButton, color: '#dc2626'}}
// //             >
// //               <X style={{width: '20px', height: '20px'}} />
// //             </button>
// //           </div>
// //         )}

// //         <div style={styles.contentWrapper}>
// //           {/* Left sidebar */}
// //           <nav style={styles.sidebar}>
// //             {tabs.map(tab => {
// //               const Icon = tab.icon;
// //               const isActive = activeTab === tab.id;
// //               return (
// //                 <button
// //                   key={tab.id}
// //                   onClick={() => setActiveTab(tab.id)}
// //                   style={{
// //                     ...styles.navButton,
// //                     ...(isActive ? styles.navButtonActive : {}),
// //                   }}
// //                   onMouseEnter={(e) => {
// //                     if (!isActive) e.target.style.backgroundColor = '#f3f4f6';
// //                   }}
// //                   onMouseLeave={(e) => {
// //                     if (!isActive) e.target.style.backgroundColor = 'transparent';
// //                   }}
// //                 >
// //                   <Icon style={styles.navIcon} />
// //                   {tab.label}
// //                 </button>
// //               );
// //             })}
// //           </nav>

// //           {/* Main content area */}
// //           <main style={styles.mainContent}>
// //             {activeTab === 'environments' && (
// //               <EnvironmentManager 
// //                 environments={environments}
// //                 onEnvironmentChange={loadEnvironments}
// //                 loading={loading}
// //               />
// //             )}
// //             {activeTab === 'models' && (
// //               <ModelManager 
// //                 selectedEnvironment={selectedEnvironment}
// //               />
// //             )}
// //             {activeTab === 'graph' && (
// //               <TwinGraphVisualizer 
// //                 selectedEnvironment={selectedEnvironment}
// //               />
// //             )}
// //             {activeTab === 'devices' && (
// //               <DeviceManager 
// //                 selectedEnvironment={selectedEnvironment}
// //               />
// //             )}
// //           </main>
// //         </div>
// //       </div>
// //     </div>
// //   );
// // };

// // // Environment management component
// // const EnvironmentManager = ({ environments, onEnvironmentChange, loading }) => {
// //   const [showCreateForm, setShowCreateForm] = useState(false);
// //   const [submitting, setSubmitting] = useState(false);
// //   const [formData, setFormData] = useState({
// //     environment_id: '',
// //     display_name: '',
// //     description: ''
// //   });

// //   const handleSubmit = async () => {
// //     try {
// //       setSubmitting(true);
// //       await DigitalTwinAPI.createEnvironment(formData);
// //       setShowCreateForm(false);
// //       setFormData({ environment_id: '', display_name: '', description: '' });
// //       onEnvironmentChange();
// //     } catch (err) {
// //       alert('Failed to create environment: ' + err.message);
// //     } finally {
// //       setSubmitting(false);
// //     }
// //   };

// //   const handleDelete = async (envId) => {
// //     if (window.confirm('Are you sure you want to delete this environment? This will delete all related data.')) {
// //       try {
// //         await DigitalTwinAPI.deleteEnvironment(envId);
// //         onEnvironmentChange();
// //       } catch (err) {
// //         alert('Failed to delete environment: ' + err.message);
// //       }
// //     }
// //   };

// //   return (
// //     <div>
// //       <div style={styles.contentHeader}>
// //         <h2 style={styles.contentTitle}>Environment Management</h2>
// //         <button
// //           onClick={() => setShowCreateForm(true)}
// //           style={styles.primaryButton}
// //           disabled={submitting}
// //           onMouseEnter={(e) => e.target.style.backgroundColor = '#1d4ed8'}
// //           onMouseLeave={(e) => e.target.style.backgroundColor = '#2563eb'}
// //         >
// //           <Plus style={styles.buttonIcon} />
// //           Create Environment
// //         </button>
// //       </div>

// //       <div style={styles.content}>
// //         {loading ? (
// //           <div style={styles.loadingContainer}>
// //             <div style={styles.spinner}></div>
// //             <span style={{marginLeft: '8px', color: '#6b7280'}}>Loading...</span>
// //           </div>
// //         ) : environments.length === 0 ? (
// //           <div style={styles.emptyState}>
// //             <Building style={styles.emptyStateIcon} />
// //             <p>No environments yet. Click the button above to create your first environment.</p>
// //           </div>
// //         ) : (
// //           <div>
// //             {environments.map(env => (
// //               <div key={env.environment_id} style={styles.listItem}>
// //                 <div style={styles.itemContent}>
// //                   <div style={styles.itemInfo}>
// //                     <h3 style={styles.itemTitle}>{env.display_name}</h3>
// //                     <p style={styles.itemSubtitle}>ID: {env.environment_id}</p>
// //                     {env.description && (
// //                       <p style={styles.itemDescription}>{env.description}</p>
// //                     )}
// //                     <p style={styles.itemMeta}>
// //                       Created: {formatDate(env.created_at)}
// //                     </p>
// //                   </div>
// //                   <div style={styles.itemActions}>
// //                     <button
// //                       onClick={() => handleDelete(env.environment_id)}
// //                       style={{...styles.actionButton, ...styles.actionButtonRed}}
// //                       title="Delete Environment"
// //                       onMouseEnter={(e) => e.target.style.backgroundColor = '#fee2e2'}
// //                       onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                     >
// //                       <Trash2 style={styles.actionIcon} />
// //                     </button>
// //                   </div>
// //                 </div>
// //               </div>
// //             ))}
// //           </div>
// //         )}
// //       </div>

// //       {/* Create environment modal */}
// //       <Modal
// //         isOpen={showCreateForm}
// //         onClose={() => setShowCreateForm(false)}
// //         title="Create New Environment"
// //       >
// //         <div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Environment ID *</label>
// //             <input
// //               type="text"
// //               value={formData.environment_id}
// //               onChange={(e) => setFormData({...formData, environment_id: e.target.value})}
// //               style={styles.formInput}
// //               placeholder="Only letters, numbers, hyphens and underscores allowed"
// //               required
// //             />
// //           </div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Display Name *</label>
// //             <input
// //               type="text"
// //               value={formData.display_name}
// //               onChange={(e) => setFormData({...formData, display_name: e.target.value})}
// //               style={styles.formInput}
// //               required
// //             />
// //           </div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Description</label>
// //             <textarea
// //               value={formData.description}
// //               onChange={(e) => setFormData({...formData, description: e.target.value})}
// //               style={styles.formTextarea}
// //               rows="3"
// //               placeholder="Detailed description of the environment (optional)"
// //             />
// //           </div>
// //           <div style={styles.formActions}>
// //             <button
// //               type="button"
// //               onClick={() => setShowCreateForm(false)}
// //               style={styles.cancelButton}
// //               disabled={submitting}
// //             >
// //               Cancel
// //             </button>
// //             <button
// //               type="button"
// //               onClick={handleSubmit}
// //               style={{
// //                 ...styles.primaryButton,
// //                 opacity: (submitting || !formData.environment_id || !formData.display_name) ? 0.5 : 1
// //               }}
// //               disabled={submitting || !formData.environment_id || !formData.display_name}
// //             >
// //               {submitting ? 'Creating...' : 'Create'}
// //             </button>
// //           </div>
// //         </div>
// //       </Modal>
// //     </div>
// //   );
// // };

// // // Model management component
// // const ModelManager = ({ selectedEnvironment }) => {
// //   const [models, setModels] = useState([]);
// //   const [loading, setLoading] = useState(false);
// //   const [submitting, setSubmitting] = useState(false);
// //   const [showCreateForm, setShowCreateForm] = useState(false);
// //   const [showDetailModal, setShowDetailModal] = useState(false);
// //   const [selectedModel, setSelectedModel] = useState(null);
// //   const [editingModel, setEditingModel] = useState(null);
// //   const [error, setError] = useState(null);
  
// //   const [formData, setFormData] = useState({
// //     model_id: '',
// //     display_name: '',
// //     description: '',
// //     properties: '{}'
// //   });

// //   useEffect(() => {
// //     if (selectedEnvironment) {
// //       loadModels();
// //     }
// //   }, [selectedEnvironment]);

// //   const loadModels = async () => {
// //     try {
// //       setLoading(true);
// //       setError(null);
// //       const data = await DigitalTwinAPI.getModels(selectedEnvironment.environment_id);
// //       setModels(Array.isArray(data) ? data : []);
// //     } catch (err) {
// //       setError('Failed to load models: ' + err.message);
// //       setModels([]);
// //     } finally {
// //       setLoading(false);
// //     }
// //   };

// //   const handleSubmit = async () => {
// //     try {
// //       setSubmitting(true);
      
// //       let properties = {};
// //       if (formData.properties.trim()) {
// //         try {
// //           properties = JSON.parse(formData.properties);
// //         } catch (err) {
// //           throw new Error('Property definition JSON format error: ' + err.message);
// //         }
// //       }

// //       const modelData = {
// //         model_id: formData.model_id,
// //         display_name: formData.display_name,
// //         description: formData.description,
// //         properties: properties
// //       };

// //       if (editingModel) {
// //         await DigitalTwinAPI.updateModel(selectedEnvironment.environment_id, editingModel.model_id, {
// //           description: formData.description,
// //           properties: properties
// //         });
// //       } else {
// //         await DigitalTwinAPI.createModel(selectedEnvironment.environment_id, modelData);
// //       }

// //       setShowCreateForm(false);
// //       setEditingModel(null);
// //       setFormData({ model_id: '', display_name: '', description: '', properties: '{}' });
// //       loadModels();
// //     } catch (err) {
// //       alert('Failed to ' + (editingModel ? 'update' : 'create') + ' model: ' + err.message);
// //     } finally {
// //       setSubmitting(false);
// //     }
// //   };

// //   const handleEdit = (model) => {
// //     setEditingModel(model);
// //     setFormData({
// //       model_id: model.model_id,
// //       display_name: model.display_name,
// //       description: model.description || '',
// //       properties: JSON.stringify(model.properties || {}, null, 2)
// //     });
// //     setShowCreateForm(true);
// //   };

// //   const handleDelete = async (modelId) => {
// //     if (window.confirm('Are you sure you want to delete this model?')) {
// //       try {
// //         await DigitalTwinAPI.deleteModel(selectedEnvironment.environment_id, modelId);
// //         loadModels();
// //       } catch (err) {
// //         alert('Failed to delete model: ' + err.message);
// //       }
// //     }
// //   };

// //   const handleViewDetails = async (model) => {
// //     try {
// //       const fullModel = await DigitalTwinAPI.getModel(selectedEnvironment.environment_id, model.model_id);
// //       setSelectedModel(fullModel);
// //       setShowDetailModal(true);
// //     } catch (err) {
// //       alert('Failed to get model details: ' + err.message);
// //     }
// //   };

// //   if (!selectedEnvironment) {
// //     return (
// //       <div style={styles.emptyState}>
// //         <Database style={styles.emptyStateIcon} />
// //         <p>Please select an environment first</p>
// //       </div>
// //     );
// //   }

// //   return (
// //     <div>
// //       <div style={styles.contentHeader}>
// //         <h2 style={styles.contentTitle}>Model Management</h2>
// //         <button 
// //           style={styles.primaryButton}
// //           onClick={() => {
// //             setEditingModel(null);
// //             setFormData({ model_id: '', display_name: '', description: '', properties: '{}' });
// //             setShowCreateForm(true);
// //           }}
// //           disabled={submitting}
// //           onMouseEnter={(e) => e.target.style.backgroundColor = '#1d4ed8'}
// //           onMouseLeave={(e) => e.target.style.backgroundColor = '#2563eb'}
// //         >
// //           <Plus style={styles.buttonIcon} />
// //           Create Model
// //         </button>
// //       </div>

// //       <div style={styles.content}>
// //         {error && (
// //           <div style={styles.errorMessage}>
// //             <span style={styles.errorText}>{error}</span>
// //             <button onClick={() => setError(null)} style={{...styles.closeButton, color: '#dc2626'}}>
// //               <X style={{width: '16px', height: '16px'}} />
// //             </button>
// //           </div>
// //         )}

// //         {loading ? (
// //           <div style={styles.loadingContainer}>
// //             <div style={styles.spinner}></div>
// //             <span style={{marginLeft: '8px', color: '#6b7280'}}>Loading...</span>
// //           </div>
// //         ) : models.length === 0 ? (
// //           <div style={styles.emptyState}>
// //             <Database style={styles.emptyStateIcon} />
// //             <p>No models yet. Click the button above to create your first model.</p>
// //           </div>
// //         ) : (
// //           <div>
// //             {models.map(model => (
// //               <div key={model.model_id} style={styles.listItem}>
// //                 <div style={styles.itemContent}>
// //                   <div style={styles.itemInfo}>
// //                     <h3 style={styles.itemTitle}>{model.display_name}</h3>
// //                     <p style={styles.itemSubtitle}>ID: {model.model_id}</p>
// //                     {model.description && (
// //                       <p style={styles.itemDescription}>{model.description}</p>
// //                     )}
// //                     <p style={styles.itemMeta}>
// //                       Created: {formatDate(model.created_at)}
// //                     </p>
// //                     <p style={styles.itemMeta}>
// //                       Properties: {Object.keys(model.properties || {}).length}
// //                     </p>
// //                   </div>
// //                   <div style={styles.itemActions}>
// //                     <button
// //                       onClick={() => handleViewDetails(model)}
// //                       style={{...styles.actionButton, ...styles.actionButtonBlue}}
// //                       title="View Details"
// //                       onMouseEnter={(e) => e.target.style.backgroundColor = '#dbeafe'}
// //                       onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                     >
// //                       <Eye style={styles.actionIcon} />
// //                     </button>
// //                     <button
// //                       onClick={() => handleEdit(model)}
// //                       style={{...styles.actionButton, ...styles.actionButtonGray}}
// //                       title="Edit"
// //                       onMouseEnter={(e) => e.target.style.backgroundColor = '#f3f4f6'}
// //                       onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                     >
// //                       <Edit style={styles.actionIcon} />
// //                     </button>
// //                     <button
// //                       onClick={() => handleDelete(model.model_id)}
// //                       style={{...styles.actionButton, ...styles.actionButtonRed}}
// //                       title="Delete"
// //                       onMouseEnter={(e) => e.target.style.backgroundColor = '#fee2e2'}
// //                       onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                     >
// //                       <Trash2 style={styles.actionIcon} />
// //                     </button>
// //                   </div>
// //                 </div>
// //               </div>
// //             ))}
// //           </div>
// //         )}
// //       </div>

// //       {/* Create/Edit model modal */}
// //       <Modal
// //         isOpen={showCreateForm}
// //         onClose={() => setShowCreateForm(false)}
// //         title={editingModel ? "Edit Model" : "Create New Model"}
// //         size="large"
// //       >
// //         <div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Model ID *</label>
// //             <input
// //               type="text"
// //               value={formData.model_id}
// //               onChange={(e) => setFormData({...formData, model_id: e.target.value})}
// //               style={styles.formInput}
// //               disabled={editingModel}
// //               required
// //             />
// //           </div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Display Name *</label>
// //             <input
// //               type="text"
// //               value={formData.display_name}
// //               onChange={(e) => setFormData({...formData, display_name: e.target.value})}
// //               style={styles.formInput}
// //               disabled={editingModel}
// //               required
// //             />
// //           </div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Description</label>
// //             <textarea
// //               value={formData.description}
// //               onChange={(e) => setFormData({...formData, description: e.target.value})}
// //               style={styles.formTextarea}
// //               rows="3"
// //             />
// //           </div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Property Definition (JSON Format)</label>
// //             <JsonEditor
// //               value={formData.properties}
// //               onChange={(value) => setFormData({...formData, properties: value})}
// //               placeholder={`{
// //   "temperature": {
// //     "type": "number",
// //     "unit": "°C",
// //     "description": "Temperature sensor reading",
// //     "is_required": true,
// //     "constraints": {
// //       "min": -50,
// //       "max": 100
// //     }
// //   }
// // }`}
// //             />
// //           </div>
// //           <div style={styles.formActions}>
// //             <button
// //               type="button"
// //               onClick={() => setShowCreateForm(false)}
// //               style={styles.cancelButton}
// //               disabled={submitting}
// //             >
// //               Cancel
// //             </button>
// //             <button
// //               type="button"
// //               onClick={handleSubmit}
// //               style={{
// //                 ...styles.primaryButton,
// //                 opacity: (submitting || !formData.model_id || !formData.display_name) ? 0.5 : 1
// //               }}
// //               disabled={submitting || !formData.model_id || !formData.display_name}
// //             >
// //               {submitting ? (editingModel ? 'Updating...' : 'Creating...') : (editingModel ? 'Update' : 'Create')}
// //             </button>
// //           </div>
// //         </div>
// //       </Modal>

// //       {/* Model details modal */}
// //       <Modal
// //         isOpen={showDetailModal}
// //         onClose={() => setShowDetailModal(false)}
// //         title="Model Details"
// //         size="large"
// //       >
// //         {selectedModel && (
// //           <div>
// //             <div style={styles.grid2}>
// //               <div>
// //                 <label style={styles.formLabel}>Model ID</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedModel.model_id}</p>
// //               </div>
// //               <div>
// //                 <label style={styles.formLabel}>Display Name</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedModel.display_name}</p>
// //               </div>
// //               <div>
// //                 <label style={styles.formLabel}>Created</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedModel.created_at)}</p>
// //               </div>
// //               <div>
// //                 <label style={styles.formLabel}>Updated</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedModel.updated_at)}</p>
// //               </div>
// //             </div>
            
// //             {selectedModel.description && (
// //               <div style={{margin: '24px 0'}}>
// //                 <label style={styles.formLabel}>Description</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedModel.description}</p>
// //               </div>
// //             )}
            
// //             <div>
// //               <label style={{...styles.formLabel, marginBottom: '8px'}}>Property Definition</label>
// //               <pre style={styles.preCode}>
// //                 {JSON.stringify(selectedModel.properties || {}, null, 2)}
// //               </pre>
// //             </div>
// //           </div>
// //         )}
// //       </Modal>
// //     </div>
// //   );
// // };

// // // Device management component
// // const DeviceManager = ({ selectedEnvironment }) => {
// //   const [devices, setDevices] = useState([]);
// //   const [loading, setLoading] = useState(false);
// //   const [submitting, setSubmitting] = useState(false);
// //   const [showCreateForm, setShowCreateForm] = useState(false);
// //   const [showDetailModal, setShowDetailModal] = useState(false);
// //   const [selectedDevice, setSelectedDevice] = useState(null);
// //   const [editingDevice, setEditingDevice] = useState(null);
// //   const [error, setError] = useState(null);
  
// //   const [formData, setFormData] = useState({
// //     device_id: '',
// //     display_name: '',
// //     description: ''
// //   });

// //   useEffect(() => {
// //     if (selectedEnvironment) {
// //       loadDevices();
// //     }
// //   }, [selectedEnvironment]);

// //   const loadDevices = async () => {
// //     try {
// //       setLoading(true);
// //       setError(null);
// //       const data = await DigitalTwinAPI.getDevices(selectedEnvironment.environment_id);
// //       setDevices(Array.isArray(data) ? data : []);
// //     } catch (err) {
// //       setError('Failed to load devices: ' + err.message);
// //       setDevices([]);
// //     } finally {
// //       setLoading(false);
// //     }
// //   };

// //   const handleSubmit = async () => {
// //     try {
// //       setSubmitting(true);
      
// //       const deviceData = {
// //         device_id: formData.device_id,
// //         display_name: formData.display_name,
// //         description: formData.description
// //       };
      
// //       if (editingDevice) {
// //         await DigitalTwinAPI.updateDevice(selectedEnvironment.environment_id, editingDevice.device_id, {
// //           display_name: formData.display_name,
// //           description: formData.description
// //         });
// //       } else {
// //         await DigitalTwinAPI.createDevice(selectedEnvironment.environment_id, deviceData);
// //       }

// //       setShowCreateForm(false);
// //       setEditingDevice(null);
// //       setFormData({ device_id: '', display_name: '', description: '' });
// //       loadDevices();
// //     } catch (err) {
// //       alert('Failed to ' + (editingDevice ? 'update' : 'register') + ' device: ' + err.message);
// //     } finally {
// //       setSubmitting(false);
// //     }
// //   };

// //   const handleEdit = (device) => {
// //     setEditingDevice(device);
// //     setFormData({
// //       device_id: device.device_id,
// //       display_name: device.display_name,
// //       description: device.description || ''
// //     });
// //     setShowCreateForm(true);
// //   };

// //   const handleDelete = async (deviceId) => {
// //     if (window.confirm('Are you sure you want to delete this device?')) {
// //       try {
// //         await DigitalTwinAPI.deleteDevice(selectedEnvironment.environment_id, deviceId);
// //         loadDevices();
// //       } catch (err) {
// //         alert('Failed to delete device: ' + err.message);
// //       }
// //     }
// //   };

// //   const handleViewDetails = async (device) => {
// //     try {
// //       const fullDevice = await DigitalTwinAPI.getDevice(selectedEnvironment.environment_id, device.device_id);
// //       setSelectedDevice(fullDevice);
// //       setShowDetailModal(true);
// //     } catch (err) {
// //       alert('Failed to get device details: ' + err.message);
// //     }
// //   };

// //   if (!selectedEnvironment) {
// //     return (
// //       <div style={styles.emptyState}>
// //         <Cpu style={styles.emptyStateIcon} />
// //         <p>Please select an environment first</p>
// //       </div>
// //     );
// //   }

// //   return (
// //     <div>
// //       <div style={styles.contentHeader}>
// //         <h2 style={styles.contentTitle}>Device Management</h2>
// //         <button 
// //           style={styles.primaryButton}
// //           onClick={() => {
// //             setEditingDevice(null);
// //             setFormData({ device_id: '', display_name: '', description: '' });
// //             setShowCreateForm(true);
// //           }}
// //           disabled={submitting}
// //           onMouseEnter={(e) => e.target.style.backgroundColor = '#1d4ed8'}
// //           onMouseLeave={(e) => e.target.style.backgroundColor = '#2563eb'}
// //         >
// //           <Plus style={styles.buttonIcon} />
// //           Register Device
// //         </button>
// //       </div>

// //       <div style={styles.content}>
// //         {error && (
// //           <div style={styles.errorMessage}>
// //             <span style={styles.errorText}>{error}</span>
// //             <button onClick={() => setError(null)} style={{...styles.closeButton, color: '#dc2626'}}>
// //               <X style={{width: '16px', height: '16px'}} />
// //             </button>
// //           </div>
// //         )}

// //         {loading ? (
// //           <div style={styles.loadingContainer}>
// //             <div style={styles.spinner}></div>
// //             <span style={{marginLeft: '8px', color: '#6b7280'}}>Loading...</span>
// //           </div>
// //         ) : devices.length === 0 ? (
// //           <div style={styles.emptyState}>
// //             <Cpu style={styles.emptyStateIcon} />
// //             <p>No devices yet. Click the button above to register your first device.</p>
// //           </div>
// //         ) : (
// //           <div>
// //             {devices.map(device => (
// //               <div key={device.device_id} style={styles.listItem}>
// //                 <div style={styles.itemContent}>
// //                   <div style={styles.itemInfo}>
// //                     <h3 style={styles.itemTitle}>{device.display_name}</h3>
// //                     <p style={styles.itemSubtitle}>ID: {device.device_id}</p>
// //                     {device.description && (
// //                       <p style={styles.itemDescription}>{device.description}</p>
// //                     )}
// //                     <p style={styles.itemMeta}>
// //                       Created: {formatDate(device.created_at)}
// //                     </p>
// //                   </div>
// //                   <div style={styles.itemActions}>
// //                     <button
// //                       onClick={() => handleViewDetails(device)}
// //                       style={{...styles.actionButton, ...styles.actionButtonBlue}}
// //                       title="View Details"
// //                       onMouseEnter={(e) => e.target.style.backgroundColor = '#dbeafe'}
// //                       onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                     >
// //                       <Eye style={styles.actionIcon} />
// //                     </button>
// //                     <button
// //                       onClick={() => handleEdit(device)}
// //                       style={{...styles.actionButton, ...styles.actionButtonGray}}
// //                       title="Edit"
// //                       onMouseEnter={(e) => e.target.style.backgroundColor = '#f3f4f6'}
// //                       onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                     >
// //                       <Edit style={styles.actionIcon} />
// //                     </button>
// //                     <button
// //                       onClick={() => handleDelete(device.device_id)}
// //                       style={{...styles.actionButton, ...styles.actionButtonRed}}
// //                       title="Delete"
// //                       onMouseEnter={(e) => e.target.style.backgroundColor = '#fee2e2'}
// //                       onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
// //                     >
// //                       <Trash2 style={styles.actionIcon} />
// //                     </button>
// //                   </div>
// //                 </div>
// //               </div>
// //             ))}
// //           </div>
// //         )}
// //       </div>

// //       {/* Create/Edit device modal */}
// //       <Modal
// //         isOpen={showCreateForm}
// //         onClose={() => setShowCreateForm(false)}
// //         title={editingDevice ? "Edit Device" : "Register New Device"}
// //       >
// //         <div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Device ID *</label>
// //             <input
// //               type="text"
// //               value={formData.device_id}
// //               onChange={(e) => setFormData({...formData, device_id: e.target.value})}
// //               style={styles.formInput}
// //               placeholder="Only letters, numbers, hyphens and underscores allowed"
// //               disabled={editingDevice}
// //               required
// //             />
// //           </div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Device Name *</label>
// //             <input
// //               type="text"
// //               value={formData.display_name}
// //               onChange={(e) => setFormData({...formData, display_name: e.target.value})}
// //               style={styles.formInput}
// //               required
// //             />
// //           </div>
// //           <div style={styles.formGroup}>
// //             <label style={styles.formLabel}>Description</label>
// //             <textarea
// //               value={formData.description}
// //               onChange={(e) => setFormData({...formData, description: e.target.value})}
// //               style={styles.formTextarea}
// //               rows="3"
// //               placeholder="Device description, type information, etc. (optional)"
// //             />
// //           </div>
// //           <div style={styles.formActions}>
// //             <button
// //               type="button"
// //               onClick={() => setShowCreateForm(false)}
// //               style={styles.cancelButton}
// //               disabled={submitting}
// //             >
// //               Cancel
// //             </button>
// //             <button
// //               type="button"
// //               onClick={handleSubmit}
// //               style={{
// //                 ...styles.primaryButton,
// //                 opacity: (submitting || !formData.device_id || !formData.display_name) ? 0.5 : 1
// //               }}
// //               disabled={submitting || !formData.device_id || !formData.display_name}
// //             >
// //               {submitting ? (editingDevice ? 'Updating...' : 'Registering...') : (editingDevice ? 'Update' : 'Register')}
// //             </button>
// //           </div>
// //         </div>
// //       </Modal>

// //       {/* Device details modal */}
// //       <Modal
// //         isOpen={showDetailModal}
// //         onClose={() => setShowDetailModal(false)}
// //         title="Device Details"
// //       >
// //         {selectedDevice && (
// //           <div>
// //             <div style={styles.grid2}>
// //               <div>
// //                 <label style={styles.formLabel}>Device ID</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedDevice.device_id}</p>
// //               </div>
// //               <div>
// //                 <label style={styles.formLabel}>Device Name</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedDevice.display_name}</p>
// //               </div>
// //               <div>
// //                 <label style={styles.formLabel}>Created</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedDevice.created_at)}</p>
// //               </div>
// //               <div>
// //                 <label style={styles.formLabel}>Updated</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedDevice.updated_at)}</p>
// //               </div>
// //             </div>
            
// //             {selectedDevice.description && (
// //               <div style={{margin: '16px 0'}}>
// //                 <label style={styles.formLabel}>Description</label>
// //                 <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedDevice.description}</p>
// //               </div>
// //             )}

// //             <div>
// //               <label style={styles.formLabel}>Telemetry Data Points</label>
// //               <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedDevice.telemetry_points_count || 0}</p>
// //             </div>
// //           </div>
// //         )}
// //       </Modal>
// //     </div>
// //   );
// // };

// // export default DigitalTwinPlatform;

import React, { useState, useEffect, useRef } from 'react';
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
  Layers
} from 'lucide-react';
import * as d3 from 'd3';

// API base configuration
const API_BASE_URL = 'http://localhost:8000';

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
    const url = rootTwinId 
      ? `/environments/${envId}/tree-graph?root_twin_id=${rootTwinId}`
      : `/environments/${envId}/tree-graph`;
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
          <AlertCircle style={{width: '16px', height: '16px', marginRight: '4px'}} />
          JSON format error: {error}
        </div>
      )}
    </div>
  );
};

// Enhanced Tree Graph Visualizer using new tree-graph API
const TwinGraphVisualizer = ({ selectedEnvironment }) => {
  const [treeData, setTreeData] = useState({ nodes: [], relationships: [] });
  const [twins, setTwins] = useState([]);
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);
  const [stats, setStats] = useState(null);
  
  // UI state
  const [selectedNode, setSelectedNode] = useState(null);
  const [showCreateTwinForm, setShowCreateTwinForm] = useState(false);
  const [showCreateRelForm, setShowCreateRelForm] = useState(false);
  const [showDetailModal, setShowDetailModal] = useState(false);
  const [rootTwinId, setRootTwinId] = useState(null);
  
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

  useEffect(() => {
    if (selectedEnvironment) {
      loadData();
      loadStats();
    }
  }, [selectedEnvironment]);

  useEffect(() => {
    if (treeData.nodes.length > 0 && containerRef.current) {
      renderTreeGraph();
    }
  }, [treeData, selectedNode]);

  const loadData = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const [treeGraphData, twinsData, modelsData] = await Promise.all([
        DigitalTwinAPI.getTreeGraph(selectedEnvironment.environment_id, rootTwinId),
        DigitalTwinAPI.getTwins(selectedEnvironment.environment_id),
        DigitalTwinAPI.getModels(selectedEnvironment.environment_id)
      ]);
      
      setTreeData(treeGraphData || { nodes: [], relationships: [] });
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

  const renderTreeGraph = () => {
    if (!containerRef.current) return;
    
    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const containerRect = containerRef.current.getBoundingClientRect();
    const width = containerRect.width;
    const height = containerRect.height;
    
    svg.attr("width", width).attr("height", height);

    // Convert tree data to D3 hierarchy
    const hierarchyData = convertToHierarchy(treeData.nodes);
    
    if (!hierarchyData) {
      // No tree structure, render as simple list
      renderFlatNodeList(svg, width, height);
      return;
    }

    // Create tree layout
    const treeLayout = d3.tree()
      .size([height - 100, width - 200]);

    const root = d3.hierarchy(hierarchyData);
    treeLayout(root);

    const container = svg.append("g")
      .attr("transform", "translate(100, 50)");

    // Create zoom behavior
    const zoom = d3.zoom()
      .scaleExtent([0.1, 4])
      .on("zoom", (event) => {
        container.attr("transform", `translate(100, 50) ${event.transform}`);
      });

    svg.call(zoom);

    // Draw links
    const links = container.selectAll(".link")
      .data(root.links())
      .enter().append("path")
      .attr("class", "link")
      .attr("d", d3.linkHorizontal()
        .x(d => d.y)
        .y(d => d.x))
      .attr("stroke", "#999")
      .attr("stroke-width", 2)
      .attr("fill", "none");

    // Draw nodes
    const nodeGroups = container.selectAll(".node")
      .data(root.descendants())
      .enter().append("g")
      .attr("class", "node")
      .attr("transform", d => `translate(${d.y},${d.x})`)
      .style("cursor", "pointer")
      .on("click", handleNodeClick);

    // Node circles
    nodeGroups.append("circle")
      .attr("r", 20)
      .attr("fill", d => getNodeColor(d.data.metadata?.model_id))
      .attr("stroke", d => selectedNode?.id === d.data.id ? "#2563eb" : "#fff")
      .attr("stroke-width", d => selectedNode?.id === d.data.id ? 3 : 2);

    // Node labels
    nodeGroups.append("text")
      .attr("dy", "0.31em")
      .attr("x", d => d.children ? -25 : 25)
      .attr("text-anchor", d => d.children ? "end" : "start")
      .attr("font-size", "12px")
      .attr("font-weight", "500")
      .text(d => d.data.label || d.data.id);

    function handleNodeClick(event, d) {
      setSelectedNode(d.data);
    }
  };

  const renderFlatNodeList = (svg, width, height) => {
    // Render twins as a simple grid when no tree structure exists
    const twins = treeData.nodes;
    if (twins.length === 0) return;

    const container = svg.append("g");
    const nodeRadius = 25;
    const padding = 60;
    const cols = Math.floor((width - 100) / padding);
    
    const nodeGroups = container.selectAll(".node")
      .data(twins)
      .enter().append("g")
      .attr("class", "node")
      .attr("transform", (d, i) => {
        const row = Math.floor(i / cols);
        const col = i % cols;
        const x = 50 + col * padding;
        const y = 50 + row * padding;
        return `translate(${x},${y})`;
      })
      .style("cursor", "pointer")
      .on("click", (event, d) => setSelectedNode(d));

    // Node circles
    nodeGroups.append("circle")
      .attr("r", nodeRadius)
      .attr("fill", d => getNodeColor(d.metadata?.model_id))
      .attr("stroke", d => selectedNode?.id === d.id ? "#2563eb" : "#fff")
      .attr("stroke-width", d => selectedNode?.id === d.id ? 3 : 2);

    // Node labels
    nodeGroups.append("text")
      .attr("dy", nodeRadius + 15)
      .attr("text-anchor", "middle")
      .attr("font-size", "12px")
      .attr("font-weight", "500")
      .text(d => d.label || d.id);
  };

  const convertToHierarchy = (nodes) => {
    if (!nodes || nodes.length === 0) return null;
    
    // Simple conversion - use first node as root, others as children
    const rootNode = nodes[0];
    return {
      ...rootNode,
      children: nodes.slice(1).map(node => ({
        ...node,
        children: node.children || []
      }))
    };
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

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <Network style={styles.emptyStateIcon} />
        <p>Please select an environment first</p>
      </div>
    );
  }

  return (
    <div style={{display: 'flex', height: 'calc(100vh - 160px)', minHeight: '600px'}}>
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
          <h2 style={{margin: '0 0 16px 0', fontSize: '18px', fontWeight: '600'}}>
            Digital Twin Tree
          </h2>
          
          <div style={{display: 'flex', flexDirection: 'column', gap: '8px'}}>
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
              <Plus style={{width: '14px', height: '14px', marginRight: '6px'}} />
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
              <GitBranch style={{width: '14px', height: '14px', marginRight: '6px'}} />
              Add Relationship
            </button>
          </div>

          {/* Root Twin Selector */}
          <div style={{marginTop: '12px'}}>
            <label style={{...styles.formLabel, fontSize: '12px'}}>Tree Root:</label>
            <select
              value={rootTwinId || ''}
              onChange={(e) => {
                setRootTwinId(e.target.value || null);
                // Reload data with new root
                setTimeout(() => loadData(), 100);
              }}
              style={{...styles.formInput, fontSize: '12px', padding: '6px 8px'}}
            >
              <option value="">Auto (find roots)</option>
              {twins.map(twin => (
                <option key={twin.twin_id} value={twin.twin_id}>
                  {twin.twin_id}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Statistics section */}
        {stats && (
          <div style={{
            padding: '16px',
            borderBottom: '1px solid #e2e8f0',
            backgroundColor: '#ffffff'
          }}>
            <h3 style={{margin: '0 0 12px 0', fontSize: '14px', fontWeight: '600'}}>
              Graph Statistics
            </h3>
            <div style={{fontSize: '12px', color: '#6b7280'}}>
              <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '4px'}}>
                <span>Digital Twins:</span>
                <span style={{fontWeight: '500', color: '#1f2937'}}>{twins.length}</span>
              </div>
              <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '4px'}}>
                <span>Relationships:</span>
                <span style={{fontWeight: '500', color: '#1f2937'}}>{stats.total_relationships}</span>
              </div>
              <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '4px'}}>
                <span>Connected Twins:</span>
                <span style={{fontWeight: '500', color: '#1f2937'}}>{stats.unique_twins_count}</span>
              </div>
              <div style={{display: 'flex', justifyContent: 'space-between'}}>
                <span>Relation Types:</span>
                <span style={{fontWeight: '500', color: '#1f2937'}}>{stats.relationship_types_count}</span>
              </div>
            </div>
          </div>
        )}

        {/* Node details panel */}
        <div style={{flex: 1, overflow: 'auto', padding: '16px'}}>
          {selectedNode ? (
            <div>
              <h3 style={{margin: '0 0 12px 0', fontSize: '16px'}}>Selected Twin</h3>
              <div style={{
                backgroundColor: '#ffffff',
                padding: '12px',
                borderRadius: '6px',
                boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                border: '2px solid #2563eb'
              }}>
                <p style={{margin: '0 0 8px 0', fontWeight: '600', color: '#2563eb'}}>{selectedNode.label || selectedNode.id}</p>
                <p style={{margin: '0 0 8px 0', fontSize: '12px', color: '#6b7280'}}>
                  Model: {selectedNode.metadata?.model_id || 'Unknown'}
                </p>
                <p style={{margin: '0 0 12px 0', fontSize: '12px', color: '#6b7280'}}>
                  Type: {selectedNode.type}
                </p>
                
                <div style={{display: 'flex', gap: '8px', flexWrap: 'wrap'}}>
                  <button
                    onClick={() => setShowDetailModal(true)}
                    style={{
                      ...styles.actionButton,
                      ...styles.actionButtonBlue,
                      fontSize: '12px',
                      padding: '6px 8px'
                    }}
                  >
                    <Eye style={{width: '12px', height: '12px', marginRight: '4px'}} />
                    Details
                  </button>
                </div>
              </div>
            </div>
          ) : (
            <div style={{textAlign: 'center', color: '#6b7280', fontSize: '14px'}}>
              <Network style={{width: '32px', height: '32px', margin: '0 auto 8px'}} />
              <p>Click a twin to view details</p>
            </div>
          )}

          {/* Tree structure display */}
          <div style={{marginTop: '24px'}}>
            <h3 style={{margin: '0 0 12px 0', fontSize: '16px'}}>
              Tree Structure
            </h3>
            {treeData.nodes.length === 0 ? (
              <p style={{fontSize: '12px', color: '#6b7280', textAlign: 'center'}}>
                No twins yet. Create twins and relationships to build the tree.
              </p>
            ) : (
              <div style={{maxHeight: '300px', overflow: 'auto'}}>
                {treeData.nodes.map((node, index) => (
                  <div key={index} style={{
                    ...styles.treeNodeItem,
                    backgroundColor: selectedNode?.id === node.id ? '#dbeafe' : '#ffffff',
                    borderColor: selectedNode?.id === node.id ? '#2563eb' : '#e2e8f0'
                  }}
                  onClick={() => setSelectedNode(node)}
                  >
                    <div style={{fontWeight: '500'}}>{node.label || node.id}</div>
                    <div style={{fontSize: '11px', color: '#6b7280'}}>
                      {node.type} • {node.metadata?.model_id || 'No model'}
                    </div>
                    {node.children && node.children.length > 0 && (
                      <div style={{fontSize: '11px', color: '#16a34a', marginTop: '2px'}}>
                        {node.children.length} children
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Main graph area */}
      <div ref={containerRef} style={{flex: 1, position: 'relative', overflow: 'hidden'}}>
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
            <button onClick={() => setError(null)} style={{...styles.closeButton, color: '#dc2626'}}>
              <X style={{width: '16px', height: '16px'}} />
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
            <span style={{marginLeft: '8px', color: '#6b7280'}}>Loading tree...</span>
          </div>
        ) : treeData.nodes.length === 0 ? (
          <div style={{
            ...styles.emptyState,
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)'
          }}>
            <Network style={styles.emptyStateIcon} />
            <p>No digital twins yet. Create your first twin to start building the tree.</p>
            {models.length === 0 && (
              <p style={{color: '#f59e0b', fontSize: '14px', marginTop: '8px'}}>
                ⚠️ Create models first before adding twins
              </p>
            )}
          </div>
        ) : (
          <div style={{width: '100%', height: '100%', position: 'relative'}}>
            <svg ref={svgRef} style={{width: '100%', height: '100%', display: 'block'}} />
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
              onChange={(e) => setTwinFormData({...twinFormData, twin_id: e.target.value})}
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
              onChange={(e) => setTwinFormData({...twinFormData, display_name: e.target.value})}
              style={styles.formInput}
              placeholder="Human-readable name (optional)"
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Select Model *</label>
            <select
              value={twinFormData.model_id}
              onChange={(e) => setTwinFormData({...twinFormData, model_id: e.target.value})}
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
              onChange={(value) => setTwinFormData({...twinFormData, properties: value})}
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
              onChange={(e) => setRelFormData({...relFormData, source_twin_id: e.target.value})}
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
              onChange={(e) => setRelFormData({...relFormData, target_twin_id: e.target.value})}
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
              onChange={(e) => setRelFormData({...relFormData, relationship_name: e.target.value})}
              style={styles.formInput}
              placeholder="e.g.: connects_to, contains, controls"
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Relationship Properties (JSON Format)</label>
            <JsonEditor
              value={relFormData.properties}
              onChange={(value) => setRelFormData({...relFormData, properties: value})}
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
                <p style={{margin: '4px 0 0 0', fontSize: '14px', fontWeight: '500'}}>{selectedNode.id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Display Name</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedNode.label || selectedNode.id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Type</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedNode.type}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Model ID</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedNode.metadata?.model_id || 'Unknown'}</p>
              </div>
            </div>

            {/* Properties */}
            <div style={{marginTop: '24px'}}>
              <label style={{...styles.formLabel, marginBottom: '8px'}}>Properties</label>
              <pre style={styles.preCode}>
                {JSON.stringify(selectedNode.metadata?.properties || {}, null, 2)}
              </pre>
            </div>

            {/* Children */}
            {selectedNode.children && selectedNode.children.length > 0 && (
              <div style={{marginTop: '24px'}}>
                <label style={{...styles.formLabel, marginBottom: '8px'}}>
                  Child Nodes ({selectedNode.children.length})
                </label>
                <div style={{maxHeight: '150px', overflow: 'auto'}}>
                  {selectedNode.children.map((child, index) => (
                    <div key={index} style={{
                      backgroundColor: '#f8fafc',
                      padding: '8px 12px',
                      marginBottom: '4px',
                      borderRadius: '4px',
                      fontSize: '12px'
                    }}>
                      <strong>{child.label || child.id}</strong>
                      <span style={{color: '#6b7280', marginLeft: '8px'}}>({child.type})</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
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
            <button onClick={() => setError(null)} style={{...styles.closeButton, color: '#dc2626'}}>
              <X style={{width: '16px', height: '16px'}} />
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
            <span style={{marginLeft: '8px', color: '#6b7280'}}>Loading...</span>
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
                    <div style={{display: 'flex', gap: '8px', alignItems: 'center', margin: '8px 0'}}>
                      <span style={{...styles.badge, ...styles.badgeBlue}}>
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
                      style={{...styles.actionButton, ...styles.actionButtonRed}}
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
              onChange={(e) => setFormData({...formData, device_id: e.target.value})}
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
              onChange={(e) => setFormData({...formData, twin_id: e.target.value})}
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
              onChange={(e) => setFormData({...formData, mapping_type: e.target.value})}
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
              onChange={(e) => setFormData({...formData, description: e.target.value})}
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
  const [activeTab, setActiveTab] = useState('environments');
  const [selectedEnvironment, setSelectedEnvironment] = useState(null);
  const [environments, setEnvironments] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadEnvironments();
  }, []);

  const loadEnvironments = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await DigitalTwinAPI.getEnvironments();
      setEnvironments(Array.isArray(data) ? data : []);
    } catch (err) {
      setError('Failed to load environments: ' + err.message);
      setEnvironments([]);
    } finally {
      setLoading(false);
    }
  };

  const tabs = [
    { id: 'environments', label: 'Environments', icon: Building },
    { id: 'models', label: 'Models', icon: Database },
    { id: 'devices', label: 'Devices', icon: Cpu },
    { id: 'mappings', label: 'Device Mappings', icon: Link },
    { id: 'graph', label: 'Tree Explorer', icon: Network },
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
            <div>
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
            </div>
          </div>
        </div>
      </header>

      <div style={styles.mainContainer}>
        {/* Error message */}
        {error && (
          <div style={styles.errorMessage}>
            <div style={styles.errorText}>
              <AlertCircle style={{width: '20px', height: '20px', marginRight: '8px'}} />
              <span>{error}</span>
            </div>
            <button 
              onClick={() => setError(null)}
              style={{...styles.closeButton, color: '#dc2626'}}
            >
              <X style={{width: '20px', height: '20px'}} />
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
            {activeTab === 'graph' && (
              <TwinGraphVisualizer 
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
const EnvironmentManager = ({ environments, onEnvironmentChange, loading }) => {
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [formData, setFormData] = useState({
    environment_id: '',
    display_name: '',
    description: ''
  });

  const handleSubmit = async () => {
    try {
      setSubmitting(true);
      await DigitalTwinAPI.createEnvironment(formData);
      setShowCreateForm(false);
      setFormData({ environment_id: '', display_name: '', description: '' });
      onEnvironmentChange();
    } catch (err) {
      alert('Failed to create environment: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (envId) => {
    if (window.confirm('Are you sure you want to delete this environment? This will delete all related data.')) {
      try {
        await DigitalTwinAPI.deleteEnvironment(envId);
        onEnvironmentChange();
      } catch (err) {
        alert('Failed to delete environment: ' + err.message);
      }
    }
  };

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>Environment Management</h2>
        <button
          onClick={() => setShowCreateForm(true)}
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
            <span style={{marginLeft: '8px', color: '#6b7280'}}>Loading...</span>
          </div>
        ) : environments.length === 0 ? (
          <div style={styles.emptyState}>
            <Building style={styles.emptyStateIcon} />
            <p>No environments yet. Click the button above to create your first environment.</p>
          </div>
        ) : (
          <div>
            {environments.map(env => (
              <div key={env.environment_id} style={styles.listItem}>
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
                  </div>
                  <div style={styles.itemActions}>
                    <button
                      onClick={() => handleDelete(env.environment_id)}
                      style={{...styles.actionButton, ...styles.actionButtonRed}}
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

      {/* Create environment modal */}
      <Modal
        isOpen={showCreateForm}
        onClose={() => setShowCreateForm(false)}
        title="Create New Environment"
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Environment ID *</label>
            <input
              type="text"
              value={formData.environment_id}
              onChange={(e) => setFormData({...formData, environment_id: e.target.value})}
              style={styles.formInput}
              placeholder="Only letters, numbers, hyphens and underscores allowed"
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Display Name *</label>
            <input
              type="text"
              value={formData.display_name}
              onChange={(e) => setFormData({...formData, display_name: e.target.value})}
              style={styles.formInput}
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Description</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({...formData, description: e.target.value})}
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
              {submitting ? 'Creating...' : 'Create'}
            </button>
          </div>
        </div>
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

      if (editingModel) {
        await DigitalTwinAPI.updateModel(selectedEnvironment.environment_id, editingModel.model_id, {
          display_name: formData.display_name,
          description: formData.description,
          properties: properties
        });
      } else {
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
            <button onClick={() => setError(null)} style={{...styles.closeButton, color: '#dc2626'}}>
              <X style={{width: '16px', height: '16px'}} />
            </button>
          </div>
        )}

        {loading ? (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
            <span style={{marginLeft: '8px', color: '#6b7280'}}>Loading...</span>
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
                      style={{...styles.actionButton, ...styles.actionButtonBlue}}
                      title="View Details"
                    >
                      <Eye style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleEdit(model)}
                      style={{...styles.actionButton, ...styles.actionButtonGray}}
                      title="Edit"
                    >
                      <Edit style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleDelete(model.model_id)}
                      style={{...styles.actionButton, ...styles.actionButtonRed}}
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
              onChange={(e) => setFormData({...formData, model_id: e.target.value})}
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
              onChange={(e) => setFormData({...formData, display_name: e.target.value})}
              style={styles.formInput}
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Description</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({...formData, description: e.target.value})}
              style={styles.formTextarea}
              rows="3"
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Property Definition (JSON Format)</label>
            <JsonEditor
              value={formData.properties}
              onChange={(value) => setFormData({...formData, properties: value})}
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
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedModel.model_id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Display Name</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedModel.display_name}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Created</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedModel.created_at)}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Updated</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedModel.updated_at)}</p>
              </div>
            </div>
            
            {selectedModel.description && (
              <div style={{margin: '24px 0'}}>
                <label style={styles.formLabel}>Description</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedModel.description}</p>
              </div>
            )}
            
            <div>
              <label style={{...styles.formLabel, marginBottom: '8px'}}>Property Definition</label>
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
            <button onClick={() => setError(null)} style={{...styles.closeButton, color: '#dc2626'}}>
              <X style={{width: '16px', height: '16px'}} />
            </button>
          </div>
        )}

        {loading ? (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
            <span style={{marginLeft: '8px', color: '#6b7280'}}>Loading...</span>
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
                    <div style={{display: 'flex', gap: '8px', alignItems: 'center', margin: '8px 0'}}>
                      {device.device_type && (
                        <span style={{...styles.badge, ...styles.badgeGray}}>
                          {device.device_type}
                        </span>
                      )}
                      {device.location && (
                        <span style={{...styles.badge, ...styles.badgeBlue}}>
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
                      style={{...styles.actionButton, ...styles.actionButtonBlue}}
                      title="View Details"
                    >
                      <Eye style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleEdit(device)}
                      style={{...styles.actionButton, ...styles.actionButtonGray}}
                      title="Edit"
                    >
                      <Edit style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleDelete(device.device_id)}
                      style={{...styles.actionButton, ...styles.actionButtonRed}}
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
                onChange={(e) => setFormData({...formData, device_id: e.target.value})}
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
                onChange={(e) => setFormData({...formData, display_name: e.target.value})}
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
                onChange={(e) => setFormData({...formData, device_type: e.target.value})}
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
                onChange={(e) => setFormData({...formData, location: e.target.value})}
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
                onChange={(e) => setFormData({...formData, manufacturer: e.target.value})}
                style={styles.formInput}
                placeholder="e.g. Siemens, Honeywell"
              />
            </div>
            <div style={styles.formGroup}>
              <label style={styles.formLabel}>Model Number</label>
              <input
                type="text"
                value={formData.model_number}
                onChange={(e) => setFormData({...formData, model_number: e.target.value})}
                style={styles.formInput}
                placeholder="e.g. XYZ-123, Model-456"
              />
            </div>
          </div>

          <div style={styles.formGroup}>
            <label style={styles.formLabel}>Description</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({...formData, description: e.target.value})}
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
                <p style={{margin: '4px 0 0 0', fontSize: '14px', fontWeight: '500'}}>{selectedDevice.device_id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Device Name</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedDevice.display_name}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Device Type</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>
                  {selectedDevice.device_type || 'Not specified'}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Location</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>
                  {selectedDevice.location || 'Not specified'}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Manufacturer</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>
                  {selectedDevice.manufacturer || 'Not specified'}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Model Number</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>
                  {selectedDevice.model_number || 'Not specified'}
                </p>
              </div>
              <div>
                <label style={styles.formLabel}>Created</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedDevice.created_at)}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Updated</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedDevice.updated_at)}</p>
              </div>
              <div>
                <label style={styles.formLabel}>Last Seen</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>
                  {selectedDevice.last_seen ? formatDate(selectedDevice.last_seen) : 'Never'}
                </p>
              </div>
            </div>
            
            {selectedDevice.description && (
              <div style={{margin: '24px 0'}}>
                <label style={styles.formLabel}>Description</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedDevice.description}</p>
              </div>
            )}

            <div style={{marginTop: '24px'}}>
              <label style={styles.formLabel}>Telemetry Statistics</label>
              <div style={{
                backgroundColor: '#f8fafc',
                padding: '12px',
                borderRadius: '6px',
                fontSize: '14px'
              }}>
                <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '4px'}}>
                  <span>Total Data Points:</span>
                  <span style={{fontWeight: '500'}}>{selectedDevice.telemetry_points_count || 0}</span>
                </div>
              </div>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default DigitalTwinPlatform;
