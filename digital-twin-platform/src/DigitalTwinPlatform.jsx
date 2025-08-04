// import React, { useState, useEffect } from 'react';
// import { 
//   Building, 
//   Database, 
//   Settings, 
//   Plus, 
//   Edit, 
//   Trash2, 
//   Search,
//   Network,
//   Upload,
//   Play,
//   Eye,
//   ChevronRight,
//   ChevronDown,
//   Activity,
//   Cpu,
//   GitBranch
// } from 'lucide-react';
// import './DigitalTwinPlatform.css';

// // API 基础配置
// const API_BASE_URL = 'http://localhost:8000';

// // 日期格式化辅助函数
// const formatDate = (dateString) => {
//   if (!dateString) return '未知';
  
//   try {
//     // 修复无效的日期格式：移除 +00:00Z 中的重复 Z
//     let cleanDateString = dateString;
//     if (typeof dateString === 'string' && dateString.includes('+00:00Z')) {
//       cleanDateString = dateString.replace('+00:00Z', '+00:00');
//     }
    
//     const date = new Date(cleanDateString);
    
//     if (isNaN(date.getTime())) {
//       console.error('日期解析失败:', dateString, '清理后:', cleanDateString);
//       return '格式错误';
//     }
    
//     return date.toLocaleString('zh-CN', {
//       year: 'numeric',
//       month: '2-digit',
//       day: '2-digit',
//       hour: '2-digit',
//       minute: '2-digit'
//     });
//   } catch (error) {
//     console.error('日期格式化错误:', error);
//     return '解析错误';
//   }
// };

// // 修复后的API服务类
// class DigitalTwinAPI {
//   static async request(endpoint, options = {}) {
//     try {
//       const response = await fetch(`${API_BASE_URL}${endpoint}`, {
//         headers: {
//           'Content-Type': 'application/json',
//           ...options.headers
//         },
//         ...options
//       });
      
//       if (!response.ok) {
//         const errorData = await response.json().catch(() => ({}));
//         throw new Error(errorData.detail || `HTTP ${response.status}: ${response.statusText}`);
//       }
      
//       return response.json();
//     } catch (error) {
//       console.error('API Request failed:', error);
//       throw error;
//     }
//   }

//   // 环境管理 - 修复分页响应处理
//   static async getEnvironments() {
//     const response = await this.request('/environments');
//     return response.items || response; // 兼容处理分页响应
//   }

//   static async createEnvironment(data) {
//     return this.request('/environments', {
//       method: 'POST',
//       body: JSON.stringify(data)
//     });
//   }

//   static async deleteEnvironment(envId) {
//     return this.request(`/environments/${envId}`, {
//       method: 'DELETE'
//     });
//   }

//   // 模型管理 - 修复分页响应
//   static async getModels(envId) {
//     const response = await this.request(`/environments/${envId}/models`);
//     return response.items || response;
//   }

//   static async createModel(envId, data) {
//     return this.request(`/environments/${envId}/models`, {
//       method: 'POST',
//       body: JSON.stringify(data)
//     });
//   }

//   // 数字孪生管理 - 修复分页响应
//   static async getTwins(envId) {
//     const response = await this.request(`/environments/${envId}/twins`);
//     return response.items || response;
//   }

//   static async createTwin(envId, data) {
//     return this.request(`/environments/${envId}/twins`, {
//       method: 'POST',
//       body: JSON.stringify(data)
//     });
//   }

//   // 设备管理 - 修复分页响应
//   static async getDevices(envId) {
//     const response = await this.request(`/environments/${envId}/devices`);
//     return response.items || response;
//   }

//   static async createDevice(envId, data) {
//     return this.request(`/environments/${envId}/devices`, {
//       method: 'POST',
//       body: JSON.stringify(data)
//     });
//   }

//   // 关系管理 - 修复分页响应
//   static async getRelationships(envId) {
//     const response = await this.request(`/environments/${envId}/relationships`);
//     return response.items || response;
//   }

//   static async createRelationship(envId, data) {
//     return this.request(`/environments/${envId}/relationships`, {
//       method: 'POST',
//       body: JSON.stringify(data)
//     });
//   }

//   // 工作流管理 - 修复分页响应
//   static async getWorkflows(envId) {
//     const response = await this.request(`/environments/${envId}/workflows`);
//     return response.items || response;
//   }

//   static async uploadWorkflow(envId, data) {
//     return this.request(`/environments/${envId}/workflows`, {
//       method: 'POST',
//       body: JSON.stringify(data)
//     });
//   }
// }

// // 主界面组件
// const DigitalTwinPlatform = () => {
//   const [activeTab, setActiveTab] = useState('environments');
//   const [selectedEnvironment, setSelectedEnvironment] = useState(null);
//   const [environments, setEnvironments] = useState([]);
//   const [loading, setLoading] = useState(false);
//   const [error, setError] = useState(null);

//   useEffect(() => {
//     loadEnvironments();
//   }, []);

//   const loadEnvironments = async () => {
//     try {
//       setLoading(true);
//       setError(null);
//       const data = await DigitalTwinAPI.getEnvironments();
//       setEnvironments(Array.isArray(data) ? data : []);
//     } catch (err) {
//       setError('加载环境列表失败: ' + err.message);
//       setEnvironments([]);
//     } finally {
//       setLoading(false);
//     }
//   };

//   const tabs = [
//     { id: 'environments', label: '环境管理', icon: Building },
//     { id: 'models', label: '模型管理', icon: Database },
//     { id: 'twins', label: '数字孪生', icon: Network },
//     { id: 'devices', label: '设备管理', icon: Cpu },
//     { id: 'relationships', label: '关系管理', icon: GitBranch },
//     { id: 'workflows', label: '工作流', icon: Activity },
//   ];

//   return (
//     <div className="app">
//       {/* 顶部导航 */}
//       <header className="header">
//         <div className="header-container">
//           <div className="header-content">
//             <div className="header-left">
//               <Network className="logo" />
//               <h1 className="title">数字孪生平台</h1>
//             </div>
//             <div className="header-right">
//               <select 
//                 className="select"
//                 value={selectedEnvironment?.environment_id || ''}
//                 onChange={(e) => {
//                   const env = environments.find(env => env.environment_id === e.target.value);
//                   setSelectedEnvironment(env);
//                 }}
//               >
//                 <option value="">选择环境</option>
//                 {environments.map(env => (
//                   <option key={env.environment_id} value={env.environment_id}>
//                     {env.display_name}
//                   </option>
//                 ))}
//               </select>
//             </div>
//           </div>
//         </div>
//       </header>

//       <div className="main-container">
//         {/* 错误提示 */}
//         {error && (
//           <div className="error-message">
//             {error}
//             <button 
//               onClick={() => setError(null)}
//               style={{ float: 'right', background: 'none', border: 'none', cursor: 'pointer' }}
//             >
//               ×
//             </button>
//           </div>
//         )}

//         <div className="content-wrapper">
//           {/* 左侧导航 */}
//           <nav className="sidebar">
//             {tabs.map(tab => {
//               const Icon = tab.icon;
//               return (
//                 <button
//                   key={tab.id}
//                   onClick={() => setActiveTab(tab.id)}
//                   className={`nav-button ${activeTab === tab.id ? 'active' : ''}`}
//                 >
//                   <Icon className="nav-icon" />
//                   {tab.label}
//                 </button>
//               );
//             })}
//           </nav>

//           {/* 主内容区域 */}
//           <main className="main-content">
//             {activeTab === 'environments' && (
//               <EnvironmentManager 
//                 environments={environments}
//                 onEnvironmentChange={loadEnvironments}
//                 loading={loading}
//               />
//             )}
//             {activeTab === 'models' && (
//               <ModelManager 
//                 selectedEnvironment={selectedEnvironment}
//               />
//             )}
//             {activeTab === 'twins' && (
//               <TwinManager 
//                 selectedEnvironment={selectedEnvironment}
//               />
//             )}
//             {activeTab === 'devices' && (
//               <DeviceManager 
//                 selectedEnvironment={selectedEnvironment}
//               />
//             )}
//             {activeTab === 'relationships' && (
//               <RelationshipManager 
//                 selectedEnvironment={selectedEnvironment}
//               />
//             )}
//             {activeTab === 'workflows' && (
//               <WorkflowManager 
//                 selectedEnvironment={selectedEnvironment}
//               />
//             )}
//           </main>
//         </div>
//       </div>
//     </div>
//   );
// };

// // 环境管理组件
// const EnvironmentManager = ({ environments, onEnvironmentChange, loading }) => {
//   const [showCreateForm, setShowCreateForm] = useState(false);
//   const [submitting, setSubmitting] = useState(false);
//   const [formData, setFormData] = useState({
//     environment_id: '',
//     display_name: '',
//     description: ''
//   });

//   const handleSubmit = async () => {
//     try {
//       setSubmitting(true);
//       await DigitalTwinAPI.createEnvironment(formData);
//       setShowCreateForm(false);
//       setFormData({ environment_id: '', display_name: '', description: '' });
//       onEnvironmentChange();
//     } catch (err) {
//       alert('创建环境失败: ' + err.message);
//     } finally {
//       setSubmitting(false);
//     }
//   };

//   const handleDelete = async (envId) => {
//     if (window.confirm('确定要删除这个环境吗？这将删除所有相关数据。')) {
//       try {
//         await DigitalTwinAPI.deleteEnvironment(envId);
//         onEnvironmentChange();
//       } catch (err) {
//         alert('删除环境失败: ' + err.message);
//       }
//     }
//   };

//   return (
//     <div>
//       <div className="content-header">
//         <h2 className="content-title">环境管理</h2>
//         <button
//           onClick={() => setShowCreateForm(true)}
//           className="primary-button"
//           disabled={submitting}
//         >
//           <Plus className="button-icon" />
//           创建环境
//         </button>
//       </div>

//       {/* 创建环境表单 */}
//       {showCreateForm && (
//         <div className="form-container">
//           <h3 className="form-title">创建新环境</h3>
//           <div className="form-fields">
//             <div className="form-group">
//               <label className="form-label">环境ID *</label>
//               <input
//                 type="text"
//                 value={formData.environment_id}
//                 onChange={(e) => setFormData({...formData, environment_id: e.target.value})}
//                 className="form-input"
//                 placeholder="只能包含字母、数字、连字符和下划线"
//                 required
//               />
//             </div>
//             <div className="form-group">
//               <label className="form-label">显示名称 *</label>
//               <input
//                 type="text"
//                 value={formData.display_name}
//                 onChange={(e) => setFormData({...formData, display_name: e.target.value})}
//                 className="form-input"
//                 required
//               />
//             </div>
//             <div className="form-group">
//               <label className="form-label">描述</label>
//               <textarea
//                 value={formData.description}
//                 onChange={(e) => setFormData({...formData, description: e.target.value})}
//                 className="form-textarea"
//                 rows="3"
//                 placeholder="环境的详细描述（可选）"
//               />
//             </div>
//           </div>
//           <div className="form-actions">
//             <button
//               type="button"
//               onClick={handleSubmit}
//               className="primary-button"
//               disabled={submitting || !formData.environment_id || !formData.display_name}
//             >
//               {submitting ? '创建中...' : '创建'}
//             </button>
//             <button
//               type="button"
//               onClick={() => setShowCreateForm(false)}
//               className="cancel-button"
//               disabled={submitting}
//             >
//               取消
//             </button>
//           </div>
//         </div>
//       )}

//       {/* 环境列表 */}
//       <div className="card">
//         {loading ? (
//           <div className="loading-container">
//             <div className="loading-spinner"></div>
//             <p className="loading-text">加载中...</p>
//           </div>
//         ) : environments.length === 0 ? (
//           <div className="empty-state">
//             暂无环境，点击上方按钮创建第一个环境
//           </div>
//         ) : (
//           <div className="list-container">
//             {environments.map(env => (
//               <div key={env.environment_id} className="list-item">
//                 <div className="item-content">
//                   <div className="item-info">
//                     <h3 className="item-title">{env.display_name}</h3>
//                     <p className="item-subtitle">ID: {env.environment_id}</p>
//                     {env.description && (
//                       <p className="item-description">{env.description}</p>
//                     )}
//                     <p className="item-meta">
//                       创建时间: {formatDate(env.created_at)}
//                     </p>
//                   </div>
//                   <div className="item-actions">
//                     <button
//                       onClick={() => handleDelete(env.environment_id)}
//                       className="action-button red"
//                       title="删除环境"
//                     >
//                       <Trash2 className="action-icon" />
//                     </button>
//                   </div>
//                 </div>
//               </div>
//             ))}
//           </div>
//         )}
//       </div>
//     </div>
//   );
// };

// // 修复后的设备管理组件
// const DeviceManager = ({ selectedEnvironment }) => {
//   const [devices, setDevices] = useState([]);
//   const [loading, setLoading] = useState(false);
//   const [submitting, setSubmitting] = useState(false);
//   const [showCreateForm, setShowCreateForm] = useState(false);
//   const [error, setError] = useState(null);
  
//   // 修复：使用正确的字段名
//   const [formData, setFormData] = useState({
//     device_id: '',
//     display_name: '', // 修复：从device_name改为display_name
//     description: ''   // 修复：从device_type改为description
//   });

//   useEffect(() => {
//     if (selectedEnvironment) {
//       loadDevices();
//     }
//   }, [selectedEnvironment]);

//   const loadDevices = async () => {
//     try {
//       setLoading(true);
//       setError(null);
//       const data = await DigitalTwinAPI.getDevices(selectedEnvironment.environment_id);
//       setDevices(Array.isArray(data) ? data : []);
//     } catch (err) {
//       setError('加载设备失败: ' + err.message);
//       setDevices([]);
//     } finally {
//       setLoading(false);
//     }
//   };

//   const handleSubmit = async () => {
//     try {
//       setSubmitting(true);
      
//       // 修复：确保发送正确的数据结构
//       const deviceData = {
//         device_id: formData.device_id,
//         display_name: formData.display_name,
//         description: formData.description,
//         properties: {} // 添加空的properties对象
//       };
      
//       await DigitalTwinAPI.createDevice(selectedEnvironment.environment_id, deviceData);
//       setShowCreateForm(false);
//       setFormData({ device_id: '', display_name: '', description: '' });
//       loadDevices(); // 重新加载设备列表
//     } catch (err) {
//       alert('注册设备失败: ' + err.message);
//     } finally {
//       setSubmitting(false);
//     }
//   };

//   if (!selectedEnvironment) {
//     return (
//       <div className="empty-state-container">
//         <Cpu className="empty-state-icon" />
//         <p className="empty-state-text">请先选择一个环境</p>
//       </div>
//     );
//   }

//   return (
//     <div>
//       <div className="content-header">
//         <h2 className="content-title">设备管理</h2>
//         <button 
//           className="primary-button"
//           onClick={() => setShowCreateForm(true)}
//           disabled={submitting}
//         >
//           <Plus className="button-icon" />
//           注册设备
//         </button>
//       </div>

//       {/* 错误提示 */}
//       {error && (
//         <div className="error-message">
//           {error}
//           <button 
//             onClick={() => setError(null)}
//             style={{ float: 'right', background: 'none', border: 'none', cursor: 'pointer' }}
//           >
//             ×
//           </button>
//         </div>
//       )}

//       {/* 注册设备表单 - 修复字段 */}
//       {showCreateForm && (
//         <div className="form-container">
//           <h3 className="form-title">注册新设备</h3>
//           <div className="form-fields">
//             <div className="form-group">
//               <label className="form-label">设备ID *</label>
//               <input
//                 type="text"
//                 value={formData.device_id}
//                 onChange={(e) => setFormData({...formData, device_id: e.target.value})}
//                 className="form-input"
//                 placeholder="只能包含字母、数字、连字符和下划线"
//                 required
//               />
//             </div>
//             <div className="form-group">
//               <label className="form-label">设备名称 *</label>
//               <input
//                 type="text"
//                 value={formData.display_name}
//                 onChange={(e) => setFormData({...formData, display_name: e.target.value})}
//                 className="form-input"
//                 required
//               />
//             </div>
//             <div className="form-group">
//               <label className="form-label">描述</label>
//               <textarea
//                 value={formData.description}
//                 onChange={(e) => setFormData({...formData, description: e.target.value})}
//                 className="form-textarea"
//                 rows="3"
//                 placeholder="设备描述、类型信息等（可选）"
//               />
//             </div>
//           </div>
//           <div className="form-actions">
//             <button
//               type="button"
//               onClick={handleSubmit}
//               className="primary-button"
//               disabled={submitting || !formData.device_id || !formData.display_name}
//             >
//               {submitting ? '注册中...' : '注册'}
//             </button>
//             <button
//               type="button"
//               onClick={() => setShowCreateForm(false)}
//               className="cancel-button"
//               disabled={submitting}
//             >
//               取消
//             </button>
//           </div>
//         </div>
//       )}

//       <div className="card">
//         {loading ? (
//           <div className="loading-container">
//             <div className="loading-spinner"></div>
//             <p className="loading-text">加载中...</p>
//           </div>
//         ) : devices.length === 0 ? (
//           <div className="empty-state">
//             暂无设备，点击上方按钮注册第一个设备
//           </div>
//         ) : (
//           <div className="list-container">
//             {devices.map(device => (
//               <div key={device.device_id} className="list-item">
//                 <div className="item-content">
//                   <div className="item-info">
//                     <h3 className="item-title">{device.display_name}</h3>
//                     <p className="item-subtitle">ID: {device.device_id}</p>
//                     {device.description && (
//                       <p className="item-description">{device.description}</p>
//                     )}
//                     <p className="item-meta">
//                       创建时间: {formatDate(device.created_at)}
//                     </p>
//                   </div>
//                   <div className="item-actions">
//                     <button className="action-button blue" title="查看详情">
//                       <Eye className="action-icon" />
//                     </button>
//                     <button className="action-button gray" title="编辑">
//                       <Edit className="action-icon" />
//                     </button>
//                   </div>
//                 </div>
//               </div>
//             ))}
//           </div>
//         )}
//       </div>
//     </div>
//   );
// };

// // 其他组件保持基本不变，只是添加了更好的错误处理...
// // （为了节省空间，我只显示了主要的修复部分，其他组件可以按照相同的模式修复）

// // 模型管理组件（添加错误处理）
// const ModelManager = ({ selectedEnvironment }) => {
//   const [models, setModels] = useState([]);
//   const [loading, setLoading] = useState(false);
//   const [submitting, setSubmitting] = useState(false);
//   const [showCreateForm, setShowCreateForm] = useState(false);
//   const [error, setError] = useState(null);
//   const [formData, setFormData] = useState({
//     model_id: '',
//     display_name: '',
//     description: ''
//   });

//   useEffect(() => {
//     if (selectedEnvironment) {
//       loadModels();
//     }
//   }, [selectedEnvironment]);

//   const loadModels = async () => {
//     try {
//       setLoading(true);
//       setError(null);
//       const data = await DigitalTwinAPI.getModels(selectedEnvironment.environment_id);
//       setModels(Array.isArray(data) ? data : []);
//     } catch (err) {
//       setError('加载模型失败: ' + err.message);
//       setModels([]);
//     } finally {
//       setLoading(false);
//     }
//   };

//   const handleSubmit = async () => {
//     try {
//       setSubmitting(true);
//       await DigitalTwinAPI.createModel(selectedEnvironment.environment_id, formData);
//       setShowCreateForm(false);
//       setFormData({ model_id: '', display_name: '', description: '' });
//       loadModels();
//     } catch (err) {
//       alert('创建模型失败: ' + err.message);
//     } finally {
//       setSubmitting(false);
//     }
//   };

//   if (!selectedEnvironment) {
//     return (
//       <div className="empty-state-container">
//         <Database className="empty-state-icon" />
//         <p className="empty-state-text">请先选择一个环境</p>
//       </div>
//     );
//   }

//   return (
//     <div>
//       <div className="content-header">
//         <h2 className="content-title">模型管理</h2>
//         <button 
//           className="primary-button"
//           onClick={() => setShowCreateForm(true)}
//           disabled={submitting}
//         >
//           <Plus className="button-icon" />
//           创建模型
//         </button>
//       </div>

//       {error && (
//         <div className="error-message">
//           {error}
//           <button onClick={() => setError(null)} style={{ float: 'right', background: 'none', border: 'none', cursor: 'pointer' }}>×</button>
//         </div>
//       )}

//       {showCreateForm && (
//         <div className="form-container">
//           <h3 className="form-title">创建新模型</h3>
//           <div className="form-fields">
//             <div className="form-group">
//               <label className="form-label">模型ID *</label>
//               <input
//                 type="text"
//                 value={formData.model_id}
//                 onChange={(e) => setFormData({...formData, model_id: e.target.value})}
//                 className="form-input"
//                 required
//               />
//             </div>
//             <div className="form-group">
//               <label className="form-label">显示名称 *</label>
//               <input
//                 type="text"
//                 value={formData.display_name}
//                 onChange={(e) => setFormData({...formData, display_name: e.target.value})}
//                 className="form-input"
//                 required
//               />
//             </div>
//             <div className="form-group">
//               <label className="form-label">描述</label>
//               <textarea
//                 value={formData.description}
//                 onChange={(e) => setFormData({...formData, description: e.target.value})}
//                 className="form-textarea"
//                 rows="3"
//               />
//             </div>
//           </div>
//           <div className="form-actions">
//             <button
//               type="button"
//               onClick={handleSubmit}
//               className="primary-button"
//               disabled={submitting || !formData.model_id || !formData.display_name}
//             >
//               {submitting ? '创建中...' : '创建'}
//             </button>
//             <button
//               type="button"
//               onClick={() => setShowCreateForm(false)}
//               className="cancel-button"
//               disabled={submitting}
//             >
//               取消
//             </button>
//           </div>
//         </div>
//       )}

//       <div className="card">
//         {loading ? (
//           <div className="loading-container">
//             <div className="loading-spinner"></div>
//             <p className="loading-text">加载中...</p>
//           </div>
//         ) : models.length === 0 ? (
//           <div className="empty-state">
//             暂无模型，点击上方按钮创建第一个模型
//           </div>
//         ) : (
//           <div className="list-container">
//             {models.map(model => (
//               <div key={model.model_id} className="list-item">
//                 <div className="item-content">
//                   <div className="item-info">
//                     <h3 className="item-title">{model.display_name}</h3>
//                     <p className="item-subtitle">ID: {model.model_id}</p>
//                     {model.description && (
//                       <p className="item-description">{model.description}</p>
//                     )}
//                     <p className="item-meta">
//                       创建时间: {formatDate(model.created_at)}
//                     </p>
//                   </div>
//                   <div className="item-actions">
//                     <button className="action-button blue">
//                       <Eye className="action-icon" />
//                     </button>
//                     <button className="action-button gray">
//                       <Edit className="action-icon" />
//                     </button>
//                   </div>
//                 </div>
//               </div>
//             ))}
//           </div>
//         )}
//       </div>
//     </div>
//   );
// };

// // 简化的其他组件（只显示结构，实际使用时需要完整实现）
// const TwinManager = ({ selectedEnvironment }) => {
//   if (!selectedEnvironment) {
//     return (
//       <div className="empty-state-container">
//         <Network className="empty-state-icon" />
//         <p className="empty-state-text">请先选择一个环境</p>
//       </div>
//     );
//   }
  
//   return (
//     <div>
//       <div className="content-header">
//         <h2 className="content-title">数字孪生管理</h2>
//         <button className="primary-button">
//           <Plus className="button-icon" />
//           创建孪生
//         </button>
//       </div>
//       <div className="card">
//         <div className="empty-state">功能开发中...</div>
//       </div>
//     </div>
//   );
// };

// const RelationshipManager = ({ selectedEnvironment }) => {
//   if (!selectedEnvironment) {
//     return (
//       <div className="empty-state-container">
//         <GitBranch className="empty-state-icon" />
//         <p className="empty-state-text">请先选择一个环境</p>
//       </div>
//     );
//   }
  
//   return (
//     <div>
//       <div className="content-header">
//         <h2 className="content-title">关系管理</h2>
//         <button className="primary-button">
//           <Plus className="button-icon" />
//           创建关系
//         </button>
//       </div>
//       <div className="card">
//         <div className="empty-state">功能开发中...</div>
//       </div>
//     </div>
//   );
// };

// const WorkflowManager = ({ selectedEnvironment }) => {
//   if (!selectedEnvironment) {
//     return (
//       <div className="empty-state-container">
//         <Activity className="empty-state-icon" />
//         <p className="empty-state-text">请先选择一个环境</p>
//       </div>
//     );
//   }
  
//   return (
//     <div>
//       <div className="content-header">
//         <h2 className="content-title">工作流管理</h2>
//         <button className="primary-button">
//           <Upload className="button-icon" />
//           上传工作流
//         </button>
//       </div>
//       <div className="card">
//         <div className="empty-state">功能开发中...</div>
//       </div>
//     </div>
//   );
// };

// export default DigitalTwinPlatform;

import React, { useState, useEffect } from 'react';
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
  Save
} from 'lucide-react';

// API 基础配置
const API_BASE_URL = 'http://localhost:8000';

// 样式定义
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

// 添加CSS动画
const spinKeyframes = `
@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}
`;

// 将CSS插入到head中
if (typeof document !== 'undefined') {
  const style = document.createElement('style');
  style.textContent = spinKeyframes;
  document.head.appendChild(style);
}

// 日期格式化辅助函数
const formatDate = (dateString) => {
  if (!dateString) return '未知';
  
  try {
    let cleanDateString = dateString;
    if (typeof dateString === 'string' && dateString.includes('+00:00Z')) {
      cleanDateString = dateString.replace('+00:00Z', '+00:00');
    }
    
    const date = new Date(cleanDateString);
    
    if (isNaN(date.getTime())) {
      console.error('日期解析失败:', dateString);
      return '格式错误';
    }
    
    return date.toLocaleString('zh-CN', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    });
  } catch (error) {
    console.error('日期格式化错误:', error);
    return '解析错误';
  }
};

// API服务类
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

  // 环境管理
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

  // 模型管理
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

  // 数字孪生管理
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

  // 设备管理
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

  // 关系管理
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

  static async deleteRelationship(envId, sourceId, relationshipName, targetId) {
    return this.request(`/environments/${envId}/relationships/${sourceId}/${relationshipName}/${targetId}`, {
      method: 'DELETE'
    });
  }

  // 工作流管理
  static async getWorkflows(envId) {
    const response = await this.request(`/environments/${envId}/workflows`);
    return response.items || response;
  }

  static async getWorkflow(envId, workflowId) {
    return this.request(`/environments/${envId}/workflows/${workflowId}`);
  }

  static async createWorkflow(envId, data) {
    return this.request(`/environments/${envId}/workflows`, {
      method: 'POST',
      body: JSON.stringify(data)
    });
  }

  static async deleteWorkflow(envId, workflowId) {
    return this.request(`/environments/${envId}/workflows/${workflowId}`, {
      method: 'DELETE'
    });
  }
}

// 通用模态框组件
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

// JSON编辑器组件
const JsonEditor = ({ value, onChange, placeholder = "请输入有效的JSON..." }) => {
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
          JSON格式错误: {error}
        </div>
      )}
    </div>
  );
};

// 主界面组件
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
      setError('加载环境列表失败: ' + err.message);
      setEnvironments([]);
    } finally {
      setLoading(false);
    }
  };

  const tabs = [
    { id: 'environments', label: '环境管理', icon: Building },
    { id: 'models', label: '模型管理', icon: Database },
    { id: 'twins', label: '数字孪生', icon: Network },
    { id: 'devices', label: '设备管理', icon: Cpu },
    { id: 'relationships', label: '关系管理', icon: GitBranch },
    { id: 'workflows', label: '工作流', icon: Activity },
  ];

  return (
    <div style={styles.app}>
      {/* 顶部导航 */}
      <header style={styles.header}>
        <div style={styles.headerContainer}>
          <div style={styles.headerContent}>
            <div style={styles.headerLeft}>
              <Network style={styles.logo} />
              <h1 style={styles.title}>数字孪生平台</h1>
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
                <option value="">选择环境</option>
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
        {/* 错误提示 */}
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
          {/* 左侧导航 */}
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

          {/* 主内容区域 */}
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
            {activeTab === 'twins' && (
              <TwinManager 
                selectedEnvironment={selectedEnvironment}
              />
            )}
            {activeTab === 'devices' && (
              <DeviceManager 
                selectedEnvironment={selectedEnvironment}
              />
            )}
            {activeTab === 'relationships' && (
              <RelationshipManager 
                selectedEnvironment={selectedEnvironment}
              />
            )}
            {activeTab === 'workflows' && (
              <WorkflowManager 
                selectedEnvironment={selectedEnvironment}
              />
            )}
          </main>
        </div>
      </div>
    </div>
  );
};

// 环境管理组件
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
      alert('创建环境失败: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (envId) => {
    if (window.confirm('确定要删除这个环境吗？这将删除所有相关数据。')) {
      try {
        await DigitalTwinAPI.deleteEnvironment(envId);
        onEnvironmentChange();
      } catch (err) {
        alert('删除环境失败: ' + err.message);
      }
    }
  };

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>环境管理</h2>
        <button
          onClick={() => setShowCreateForm(true)}
          style={styles.primaryButton}
          disabled={submitting}
          onMouseEnter={(e) => e.target.style.backgroundColor = '#1d4ed8'}
          onMouseLeave={(e) => e.target.style.backgroundColor = '#2563eb'}
        >
          <Plus style={styles.buttonIcon} />
          创建环境
        </button>
      </div>

      <div style={styles.content}>
        {loading ? (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
            <span style={{marginLeft: '8px', color: '#6b7280'}}>加载中...</span>
          </div>
        ) : environments.length === 0 ? (
          <div style={styles.emptyState}>
            <Building style={styles.emptyStateIcon} />
            <p>暂无环境，点击上方按钮创建第一个环境</p>
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
                      创建时间: {formatDate(env.created_at)}
                    </p>
                  </div>
                  <div style={styles.itemActions}>
                    <button
                      onClick={() => handleDelete(env.environment_id)}
                      style={{...styles.actionButton, ...styles.actionButtonRed}}
                      title="删除环境"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#fee2e2'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
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

      {/* 创建环境模态框 */}
      <Modal
        isOpen={showCreateForm}
        onClose={() => setShowCreateForm(false)}
        title="创建新环境"
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>环境ID *</label>
            <input
              type="text"
              value={formData.environment_id}
              onChange={(e) => setFormData({...formData, environment_id: e.target.value})}
              style={styles.formInput}
              placeholder="只能包含字母、数字、连字符和下划线"
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>显示名称 *</label>
            <input
              type="text"
              value={formData.display_name}
              onChange={(e) => setFormData({...formData, display_name: e.target.value})}
              style={styles.formInput}
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>描述</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({...formData, description: e.target.value})}
              style={styles.formTextarea}
              rows="3"
              placeholder="环境的详细描述（可选）"
            />
          </div>
          <div style={styles.formActions}>
            <button
              type="button"
              onClick={() => setShowCreateForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              取消
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
              {submitting ? '创建中...' : '创建'}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
};

// 模型管理组件
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
      setError('加载模型失败: ' + err.message);
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
          throw new Error('属性定义JSON格式错误: ' + err.message);
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
      alert((editingModel ? '更新' : '创建') + '模型失败: ' + err.message);
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
    if (window.confirm('确定要删除这个模型吗？')) {
      try {
        await DigitalTwinAPI.deleteModel(selectedEnvironment.environment_id, modelId);
        loadModels();
      } catch (err) {
        alert('删除模型失败: ' + err.message);
      }
    }
  };

  const handleViewDetails = async (model) => {
    try {
      const fullModel = await DigitalTwinAPI.getModel(selectedEnvironment.environment_id, model.model_id);
      setSelectedModel(fullModel);
      setShowDetailModal(true);
    } catch (err) {
      alert('获取模型详情失败: ' + err.message);
    }
  };

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <Database style={styles.emptyStateIcon} />
        <p>请先选择一个环境</p>
      </div>
    );
  }

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>模型管理</h2>
        <button 
          style={styles.primaryButton}
          onClick={() => {
            setEditingModel(null);
            setFormData({ model_id: '', display_name: '', description: '', properties: '{}' });
            setShowCreateForm(true);
          }}
          disabled={submitting}
          onMouseEnter={(e) => e.target.style.backgroundColor = '#1d4ed8'}
          onMouseLeave={(e) => e.target.style.backgroundColor = '#2563eb'}
        >
          <Plus style={styles.buttonIcon} />
          创建模型
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
            <span style={{marginLeft: '8px', color: '#6b7280'}}>加载中...</span>
          </div>
        ) : models.length === 0 ? (
          <div style={styles.emptyState}>
            <Database style={styles.emptyStateIcon} />
            <p>暂无模型，点击上方按钮创建第一个模型</p>
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
                      创建时间: {formatDate(model.created_at)}
                    </p>
                    <p style={styles.itemMeta}>
                      属性数量: {Object.keys(model.properties || {}).length}
                    </p>
                  </div>
                  <div style={styles.itemActions}>
                    <button
                      onClick={() => handleViewDetails(model)}
                      style={{...styles.actionButton, ...styles.actionButtonBlue}}
                      title="查看详情"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#dbeafe'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
                    >
                      <Eye style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleEdit(model)}
                      style={{...styles.actionButton, ...styles.actionButtonGray}}
                      title="编辑"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#f3f4f6'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
                    >
                      <Edit style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleDelete(model.model_id)}
                      style={{...styles.actionButton, ...styles.actionButtonRed}}
                      title="删除"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#fee2e2'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
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

      {/* 创建/编辑模型模态框 */}
      <Modal
        isOpen={showCreateForm}
        onClose={() => setShowCreateForm(false)}
        title={editingModel ? "编辑模型" : "创建新模型"}
        size="large"
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>模型ID *</label>
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
            <label style={styles.formLabel}>显示名称 *</label>
            <input
              type="text"
              value={formData.display_name}
              onChange={(e) => setFormData({...formData, display_name: e.target.value})}
              style={styles.formInput}
              disabled={editingModel}
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>描述</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({...formData, description: e.target.value})}
              style={styles.formTextarea}
              rows="3"
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>属性定义 (JSON格式)</label>
            <JsonEditor
              value={formData.properties}
              onChange={(value) => setFormData({...formData, properties: value})}
              placeholder={`{
  "temperature": {
    "type": "number",
    "unit": "°C",
    "description": "温度传感器读数",
    "is_required": true,
    "constraints": {
      "min": -50,
      "max": 100
    }
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
              取消
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
              {submitting ? (editingModel ? '更新中...' : '创建中...') : (editingModel ? '更新' : '创建')}
            </button>
          </div>
        </div>
      </Modal>

      {/* 模型详情模态框 */}
      <Modal
        isOpen={showDetailModal}
        onClose={() => setShowDetailModal(false)}
        title="模型详情"
        size="large"
      >
        {selectedModel && (
          <div>
            <div style={styles.grid2}>
              <div>
                <label style={styles.formLabel}>模型ID</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedModel.model_id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>显示名称</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedModel.display_name}</p>
              </div>
              <div>
                <label style={styles.formLabel}>创建时间</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedModel.created_at)}</p>
              </div>
              <div>
                <label style={styles.formLabel}>更新时间</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedModel.updated_at)}</p>
              </div>
            </div>
            
            {selectedModel.description && (
              <div style={{margin: '24px 0'}}>
                <label style={styles.formLabel}>描述</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedModel.description}</p>
              </div>
            )}
            
            <div>
              <label style={{...styles.formLabel, marginBottom: '8px'}}>属性定义</label>
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

// 数字孪生管理组件
const TwinManager = ({ selectedEnvironment }) => {
  const [twins, setTwins] = useState([]);
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [showDetailModal, setShowDetailModal] = useState(false);
  const [selectedTwin, setSelectedTwin] = useState(null);
  const [error, setError] = useState(null);
  
  const [formData, setFormData] = useState({
    twin_id: '',
    model_id: '',
    properties: '{}'
  });

  useEffect(() => {
    if (selectedEnvironment) {
      loadTwins();
      loadModels();
    }
  }, [selectedEnvironment]);

  const loadTwins = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await DigitalTwinAPI.getTwins(selectedEnvironment.environment_id);
      setTwins(Array.isArray(data) ? data : []);
    } catch (err) {
      setError('加载数字孪生失败: ' + err.message);
      setTwins([]);
    } finally {
      setLoading(false);
    }
  };

  const loadModels = async () => {
    try {
      const data = await DigitalTwinAPI.getModels(selectedEnvironment.environment_id);
      setModels(Array.isArray(data) ? data : []);
    } catch (err) {
      console.error('加载模型列表失败:', err);
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
          throw new Error('属性JSON格式错误: ' + err.message);
        }
      }

      const twinData = {
        twin_id: formData.twin_id,
        model_id: formData.model_id,
        properties: properties
      };

      await DigitalTwinAPI.createTwin(selectedEnvironment.environment_id, twinData);
      setShowCreateForm(false);
      setFormData({ twin_id: '', model_id: '', properties: '{}' });
      loadTwins();
    } catch (err) {
      alert('创建数字孪生失败: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (twinId) => {
    if (window.confirm('确定要删除这个数字孪生吗？这将同时删除相关的关系。')) {
      try {
        await DigitalTwinAPI.deleteTwin(selectedEnvironment.environment_id, twinId);
        loadTwins();
      } catch (err) {
        alert('删除数字孪生失败: ' + err.message);
      }
    }
  };

  const handleViewDetails = async (twin) => {
    try {
      const fullTwin = await DigitalTwinAPI.getTwin(selectedEnvironment.environment_id, twin.twin_id);
      setSelectedTwin(fullTwin);
      setShowDetailModal(true);
    } catch (err) {
      alert('获取数字孪生详情失败: ' + err.message);
    }
  };

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <Network style={styles.emptyStateIcon} />
        <p>请先选择一个环境</p>
      </div>
    );
  }

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>数字孪生管理</h2>
        <button 
          style={{
            ...styles.primaryButton,
            opacity: (submitting || models.length === 0) ? 0.5 : 1
          }}
          onClick={() => setShowCreateForm(true)}
          disabled={submitting || models.length === 0}
          onMouseEnter={(e) => {
            if (!e.target.disabled) e.target.style.backgroundColor = '#1d4ed8';
          }}
          onMouseLeave={(e) => {
            if (!e.target.disabled) e.target.style.backgroundColor = '#2563eb';
          }}
        >
          <Plus style={styles.buttonIcon} />
          创建孪生
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

        {models.length === 0 && (
          <div style={styles.warningMessage}>
            <span style={styles.warningText}>请先创建模型才能创建数字孪生</span>
          </div>
        )}

        {loading ? (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
            <span style={{marginLeft: '8px', color: '#6b7280'}}>加载中...</span>
          </div>
        ) : twins.length === 0 ? (
          <div style={styles.emptyState}>
            <Network style={styles.emptyStateIcon} />
            <p>暂无数字孪生，点击上方按钮创建第一个孪生</p>
          </div>
        ) : (
          <div>
            {twins.map(twin => (
              <div key={twin.twin_id} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>孪生 ID: {twin.twin_id}</h3>
                    <p style={styles.itemSubtitle}>模型: {twin.model_id}</p>
                    <p style={styles.itemMeta}>
                      创建时间: {formatDate(twin.created_at)}
                    </p>
                    <p style={styles.itemMeta}>
                      属性数量: {Object.keys(twin.properties || {}).length}
                    </p>
                  </div>
                  <div style={styles.itemActions}>
                    <button
                      onClick={() => handleViewDetails(twin)}
                      style={{...styles.actionButton, ...styles.actionButtonBlue}}
                      title="查看详情"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#dbeafe'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
                    >
                      <Eye style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleDelete(twin.twin_id)}
                      style={{...styles.actionButton, ...styles.actionButtonRed}}
                      title="删除"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#fee2e2'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
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

      {/* 创建数字孪生模态框 */}
      <Modal
        isOpen={showCreateForm}
        onClose={() => setShowCreateForm(false)}
        title="创建新数字孪生"
        size="large"
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>孪生ID *</label>
            <input
              type="text"
              value={formData.twin_id}
              onChange={(e) => setFormData({...formData, twin_id: e.target.value})}
              style={styles.formInput}
              placeholder="只能包含字母、数字、连字符和下划线"
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>选择模型 *</label>
            <select
              value={formData.model_id}
              onChange={(e) => setFormData({...formData, model_id: e.target.value})}
              style={styles.formInput}
              required
            >
              <option value="">请选择模型</option>
              {models.map(model => (
                <option key={model.model_id} value={model.model_id}>
                  {model.display_name} ({model.model_id})
                </option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>初始属性值 (JSON格式)</label>
            <JsonEditor
              value={formData.properties}
              onChange={(value) => setFormData({...formData, properties: value})}
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
              onClick={() => setShowCreateForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              取消
            </button>
            <button
              type="button"
              onClick={handleSubmit}
              style={{
                ...styles.primaryButton,
                opacity: (submitting || !formData.twin_id || !formData.model_id) ? 0.5 : 1
              }}
              disabled={submitting || !formData.twin_id || !formData.model_id}
            >
              {submitting ? '创建中...' : '创建'}
            </button>
          </div>
        </div>
      </Modal>

      {/* 数字孪生详情模态框 */}
      <Modal
        isOpen={showDetailModal}
        onClose={() => setShowDetailModal(false)}
        title="数字孪生详情"
        size="large"
      >
        {selectedTwin && (
          <div>
            <div style={styles.grid2}>
              <div>
                <label style={styles.formLabel}>孪生ID</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedTwin.twin_id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>模型ID</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedTwin.model_id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>创建时间</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedTwin.created_at)}</p>
              </div>
              <div>
                <label style={styles.formLabel}>更新时间</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedTwin.updated_at)}</p>
              </div>
            </div>
            
            <div style={{marginTop: '24px'}}>
              <label style={{...styles.formLabel, marginBottom: '8px'}}>当前属性值</label>
              <pre style={styles.preCode}>
                {JSON.stringify(selectedTwin.properties || {}, null, 2)}
              </pre>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};

// 设备管理组件
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
    description: ''
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
      setError('加载设备失败: ' + err.message);
      setDevices([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = async () => {
    try {
      setSubmitting(true);
      
      const deviceData = {
        device_id: formData.device_id,
        display_name: formData.display_name,
        description: formData.description
      };
      
      if (editingDevice) {
        await DigitalTwinAPI.updateDevice(selectedEnvironment.environment_id, editingDevice.device_id, {
          display_name: formData.display_name,
          description: formData.description
        });
      } else {
        await DigitalTwinAPI.createDevice(selectedEnvironment.environment_id, deviceData);
      }

      setShowCreateForm(false);
      setEditingDevice(null);
      setFormData({ device_id: '', display_name: '', description: '' });
      loadDevices();
    } catch (err) {
      alert((editingDevice ? '更新' : '注册') + '设备失败: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleEdit = (device) => {
    setEditingDevice(device);
    setFormData({
      device_id: device.device_id,
      display_name: device.display_name,
      description: device.description || ''
    });
    setShowCreateForm(true);
  };

  const handleDelete = async (deviceId) => {
    if (window.confirm('确定要删除这个设备吗？')) {
      try {
        await DigitalTwinAPI.deleteDevice(selectedEnvironment.environment_id, deviceId);
        loadDevices();
      } catch (err) {
        alert('删除设备失败: ' + err.message);
      }
    }
  };

  const handleViewDetails = async (device) => {
    try {
      const fullDevice = await DigitalTwinAPI.getDevice(selectedEnvironment.environment_id, device.device_id);
      setSelectedDevice(fullDevice);
      setShowDetailModal(true);
    } catch (err) {
      alert('获取设备详情失败: ' + err.message);
    }
  };

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <Cpu style={styles.emptyStateIcon} />
        <p>请先选择一个环境</p>
      </div>
    );
  }

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>设备管理</h2>
        <button 
          style={styles.primaryButton}
          onClick={() => {
            setEditingDevice(null);
            setFormData({ device_id: '', display_name: '', description: '' });
            setShowCreateForm(true);
          }}
          disabled={submitting}
          onMouseEnter={(e) => e.target.style.backgroundColor = '#1d4ed8'}
          onMouseLeave={(e) => e.target.style.backgroundColor = '#2563eb'}
        >
          <Plus style={styles.buttonIcon} />
          注册设备
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
            <span style={{marginLeft: '8px', color: '#6b7280'}}>加载中...</span>
          </div>
        ) : devices.length === 0 ? (
          <div style={styles.emptyState}>
            <Cpu style={styles.emptyStateIcon} />
            <p>暂无设备，点击上方按钮注册第一个设备</p>
          </div>
        ) : (
          <div>
            {devices.map(device => (
              <div key={device.device_id} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{device.display_name}</h3>
                    <p style={styles.itemSubtitle}>ID: {device.device_id}</p>
                    {device.description && (
                      <p style={styles.itemDescription}>{device.description}</p>
                    )}
                    <p style={styles.itemMeta}>
                      创建时间: {formatDate(device.created_at)}
                    </p>
                  </div>
                  <div style={styles.itemActions}>
                    <button
                      onClick={() => handleViewDetails(device)}
                      style={{...styles.actionButton, ...styles.actionButtonBlue}}
                      title="查看详情"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#dbeafe'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
                    >
                      <Eye style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleEdit(device)}
                      style={{...styles.actionButton, ...styles.actionButtonGray}}
                      title="编辑"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#f3f4f6'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
                    >
                      <Edit style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleDelete(device.device_id)}
                      style={{...styles.actionButton, ...styles.actionButtonRed}}
                      title="删除"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#fee2e2'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
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

      {/* 创建/编辑设备模态框 */}
      <Modal
        isOpen={showCreateForm}
        onClose={() => setShowCreateForm(false)}
        title={editingDevice ? "编辑设备" : "注册新设备"}
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>设备ID *</label>
            <input
              type="text"
              value={formData.device_id}
              onChange={(e) => setFormData({...formData, device_id: e.target.value})}
              style={styles.formInput}
              placeholder="只能包含字母、数字、连字符和下划线"
              disabled={editingDevice}
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>设备名称 *</label>
            <input
              type="text"
              value={formData.display_name}
              onChange={(e) => setFormData({...formData, display_name: e.target.value})}
              style={styles.formInput}
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>描述</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({...formData, description: e.target.value})}
              style={styles.formTextarea}
              rows="3"
              placeholder="设备描述、类型信息等（可选）"
            />
          </div>
          <div style={styles.formActions}>
            <button
              type="button"
              onClick={() => setShowCreateForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              取消
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
              {submitting ? (editingDevice ? '更新中...' : '注册中...') : (editingDevice ? '更新' : '注册')}
            </button>
          </div>
        </div>
      </Modal>

      {/* 设备详情模态框 */}
      <Modal
        isOpen={showDetailModal}
        onClose={() => setShowDetailModal(false)}
        title="设备详情"
      >
        {selectedDevice && (
          <div>
            <div style={styles.grid2}>
              <div>
                <label style={styles.formLabel}>设备ID</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedDevice.device_id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>设备名称</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedDevice.display_name}</p>
              </div>
              <div>
                <label style={styles.formLabel}>创建时间</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedDevice.created_at)}</p>
              </div>
              <div>
                <label style={styles.formLabel}>更新时间</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{formatDate(selectedDevice.updated_at)}</p>
              </div>
            </div>
            
            {selectedDevice.description && (
              <div style={{margin: '16px 0'}}>
                <label style={styles.formLabel}>描述</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedDevice.description}</p>
              </div>
            )}

            <div>
              <label style={styles.formLabel}>遥测数据点数量</label>
              <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedDevice.telemetry_points_count || 0}</p>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};

// 关系管理组件
const RelationshipManager = ({ selectedEnvironment }) => {
  const [relationships, setRelationships] = useState([]);
  const [twins, setTwins] = useState([]);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [error, setError] = useState(null);
  
  const [formData, setFormData] = useState({
    source_twin_id: '',
    target_twin_id: '',
    relationship_name: ''
  });

  useEffect(() => {
    if (selectedEnvironment) {
      loadRelationships();
      loadTwins();
    }
  }, [selectedEnvironment]);

  const loadRelationships = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await DigitalTwinAPI.getRelationships(selectedEnvironment.environment_id);
      setRelationships(Array.isArray(data) ? data : []);
    } catch (err) {
      setError('加载关系失败: ' + err.message);
      setRelationships([]);
    } finally {
      setLoading(false);
    }
  };

  const loadTwins = async () => {
    try {
      const data = await DigitalTwinAPI.getTwins(selectedEnvironment.environment_id);
      setTwins(Array.isArray(data) ? data : []);
    } catch (err) {
      console.error('加载孪生列表失败:', err);
    }
  };

  const handleSubmit = async () => {
    try {
      setSubmitting(true);
      await DigitalTwinAPI.createRelationship(selectedEnvironment.environment_id, formData);
      setShowCreateForm(false);
      setFormData({ source_twin_id: '', target_twin_id: '', relationship_name: '' });
      loadRelationships();
    } catch (err) {
      alert('创建关系失败: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (relationship) => {
    if (window.confirm('确定要删除这个关系吗？')) {
      try {
        await DigitalTwinAPI.deleteRelationship(
          selectedEnvironment.environment_id, 
          relationship.source_twin_id,
          relationship.relationship_name,
          relationship.target_twin_id
        );
        loadRelationships();
      } catch (err) {
        alert('删除关系失败: ' + err.message);
      }
    }
  };

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <GitBranch style={styles.emptyStateIcon} />
        <p>请先选择一个环境</p>
      </div>
    );
  }

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>关系管理</h2>
        <button 
          style={{
            ...styles.primaryButton,
            opacity: (submitting || twins.length < 2) ? 0.5 : 1
          }}
          onClick={() => setShowCreateForm(true)}
          disabled={submitting || twins.length < 2}
          onMouseEnter={(e) => {
            if (!e.target.disabled) e.target.style.backgroundColor = '#1d4ed8';
          }}
          onMouseLeave={(e) => {
            if (!e.target.disabled) e.target.style.backgroundColor = '#2563eb';
          }}
        >
          <Plus style={styles.buttonIcon} />
          创建关系
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

        {twins.length < 2 && (
          <div style={styles.warningMessage}>
            <span style={styles.warningText}>需要至少2个数字孪生才能创建关系</span>
          </div>
        )}

        {loading ? (
          <div style={styles.loadingContainer}>
            <div style={styles.spinner}></div>
            <span style={{marginLeft: '8px', color: '#6b7280'}}>加载中...</span>
          </div>
        ) : relationships.length === 0 ? (
          <div style={styles.emptyState}>
            <GitBranch style={styles.emptyStateIcon} />
            <p>暂无关系，点击上方按钮创建第一个关系</p>
          </div>
        ) : (
          <div>
            {relationships.map((rel, index) => (
              <div key={index} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <div style={{display: 'flex', alignItems: 'center', gap: '8px', fontSize: '18px', fontWeight: '500', color: '#1f2937'}}>
                      <span>{rel.source_twin_id}</span>
                      <ChevronRight style={{width: '20px', height: '20px', color: '#9ca3af'}} />
                      <span style={{color: '#2563eb'}}>{rel.relationship_name}</span>
                      <ChevronRight style={{width: '20px', height: '20px', color: '#9ca3af'}} />
                      <span>{rel.target_twin_id}</span>
                    </div>
                    <p style={styles.itemMeta}>
                      创建时间: {formatDate(rel.created_at)}
                    </p>
                  </div>
                  <div style={styles.itemActions}>
                    <button
                      onClick={() => handleDelete(rel)}
                      style={{...styles.actionButton, ...styles.actionButtonRed}}
                      title="删除关系"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#fee2e2'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
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

      {/* 创建关系模态框 */}
      <Modal
        isOpen={showCreateForm}
        onClose={() => setShowCreateForm(false)}
        title="创建新关系"
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>源数字孪生 *</label>
            <select
              value={formData.source_twin_id}
              onChange={(e) => setFormData({...formData, source_twin_id: e.target.value})}
              style={styles.formInput}
              required
            >
              <option value="">请选择源孪生</option>
              {twins.map(twin => (
                <option key={twin.twin_id} value={twin.twin_id}>
                  {twin.twin_id} (模型: {twin.model_id})
                </option>
              ))}
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>关系名称 *</label>
            <input
              type="text"
              value={formData.relationship_name}
              onChange={(e) => setFormData({...formData, relationship_name: e.target.value})}
              style={styles.formInput}
              placeholder="例如: connects_to, contains, controls"
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>目标数字孪生 *</label>
            <select
              value={formData.target_twin_id}
              onChange={(e) => setFormData({...formData, target_twin_id: e.target.value})}
              style={styles.formInput}
              required
            >
              <option value="">请选择目标孪生</option>
              {twins.filter(twin => twin.twin_id !== formData.source_twin_id).map(twin => (
                <option key={twin.twin_id} value={twin.twin_id}>
                  {twin.twin_id} (模型: {twin.model_id})
                </option>
              ))}
            </select>
          </div>
          <div style={styles.formActions}>
            <button
              type="button"
              onClick={() => setShowCreateForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              取消
            </button>
            <button
              type="button"
              onClick={handleSubmit}
              style={{
                ...styles.primaryButton,
                opacity: (submitting || !formData.source_twin_id || !formData.target_twin_id || !formData.relationship_name) ? 0.5 : 1
              }}
              disabled={submitting || !formData.source_twin_id || !formData.target_twin_id || !formData.relationship_name}
            >
              {submitting ? '创建中...' : '创建'}
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
};

// 工作流管理组件
const WorkflowManager = ({ selectedEnvironment }) => {
  const [workflows, setWorkflows] = useState([]);
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [showDetailModal, setShowDetailModal] = useState(false);
  const [selectedWorkflow, setSelectedWorkflow] = useState(null);
  const [error, setError] = useState(null);
  
  const [formData, setFormData] = useState({
    workflow_id: '',
    file_name: '',
    content: '',
    description: '',
    workflow_type: 'custom'
  });

  useEffect(() => {
    if (selectedEnvironment) {
      loadWorkflows();
    }
  }, [selectedEnvironment]);

  const loadWorkflows = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await DigitalTwinAPI.getWorkflows(selectedEnvironment.environment_id);
      setWorkflows(Array.isArray(data) ? data : []);
    } catch (err) {
      setError('加载工作流失败: ' + err.message);
      setWorkflows([]);
    } finally {
      setLoading(false);
    }
  };

  const handleSubmit = async () => {
    try {
      setSubmitting(true);
      await DigitalTwinAPI.createWorkflow(selectedEnvironment.environment_id, formData);
      setShowCreateForm(false);
      setFormData({ workflow_id: '', file_name: '', content: '', description: '', workflow_type: 'custom' });
      loadWorkflows();
    } catch (err) {
      alert('创建工作流失败: ' + err.message);
    } finally {
      setSubmitting(false);
    }
  };

  const handleDelete = async (workflowId) => {
    if (window.confirm('确定要删除这个工作流吗？')) {
      try {
        await DigitalTwinAPI.deleteWorkflow(selectedEnvironment.environment_id, workflowId);
        loadWorkflows();
      } catch (err) {
        alert('删除工作流失败: ' + err.message);
      }
    }
  };

  const handleViewDetails = async (workflow) => {
    try {
      const fullWorkflow = await DigitalTwinAPI.getWorkflow(selectedEnvironment.environment_id, workflow.workflow_id);
      setSelectedWorkflow(fullWorkflow);
      setShowDetailModal(true);
    } catch (err) {
      alert('获取工作流详情失败: ' + err.message);
    }
  };

  const getBadgeStyle = (status) => {
    const baseStyle = styles.badge;
    switch (status) {
      case 'active':
        return { ...baseStyle, ...styles.badgeGreen };
      case 'inactive':
        return { ...baseStyle, ...styles.badgeGray };
      case 'error':
        return { ...baseStyle, ...styles.badgeRed };
      default:
        return { ...baseStyle, ...styles.badgeYellow };
    }
  };

  if (!selectedEnvironment) {
    return (
      <div style={styles.emptyState}>
        <Activity style={styles.emptyStateIcon} />
        <p>请先选择一个环境</p>
      </div>
    );
  }

  return (
    <div>
      <div style={styles.contentHeader}>
        <h2 style={styles.contentTitle}>工作流管理</h2>
        <button 
          style={styles.primaryButton}
          onClick={() => setShowCreateForm(true)}
          disabled={submitting}
          onMouseEnter={(e) => e.target.style.backgroundColor = '#1d4ed8'}
          onMouseLeave={(e) => e.target.style.backgroundColor = '#2563eb'}
        >
          <Upload style={styles.buttonIcon} />
          创建工作流
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
            <span style={{marginLeft: '8px', color: '#6b7280'}}>加载中...</span>
          </div>
        ) : workflows.length === 0 ? (
          <div style={styles.emptyState}>
            <Activity style={styles.emptyStateIcon} />
            <p>暂无工作流，点击上方按钮创建第一个工作流</p>
          </div>
        ) : (
          <div>
            {workflows.map(workflow => (
              <div key={workflow.workflow_id} style={styles.listItem}>
                <div style={styles.itemContent}>
                  <div style={styles.itemInfo}>
                    <h3 style={styles.itemTitle}>{workflow.file_name}</h3>
                    <p style={styles.itemSubtitle}>ID: {workflow.workflow_id}</p>
                    <div style={{display: 'flex', alignItems: 'center', gap: '16px', marginTop: '8px'}}>
                      <span style={getBadgeStyle(workflow.status)}>
                        {workflow.status}
                      </span>
                      <span style={{fontSize: '12px', color: '#6b7280'}}>类型: {workflow.workflow_type}</span>
                    </div>
                    {workflow.description && (
                      <p style={styles.itemDescription}>{workflow.description}</p>
                    )}
                    <p style={styles.itemMeta}>
                      创建时间: {formatDate(workflow.created_at)}
                    </p>
                  </div>
                  <div style={styles.itemActions}>
                    <button
                      onClick={() => handleViewDetails(workflow)}
                      style={{...styles.actionButton, ...styles.actionButtonBlue}}
                      title="查看详情"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#dbeafe'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
                    >
                      <Eye style={styles.actionIcon} />
                    </button>
                    <button
                      onClick={() => handleDelete(workflow.workflow_id)}
                      style={{...styles.actionButton, ...styles.actionButtonRed}}
                      title="删除"
                      onMouseEnter={(e) => e.target.style.backgroundColor = '#fee2e2'}
                      onMouseLeave={(e) => e.target.style.backgroundColor = 'transparent'}
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
        title="创建新工作流"
        size="large"
      >
        <div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>工作流ID *</label>
            <input
              type="text"
              value={formData.workflow_id}
              onChange={(e) => setFormData({...formData, workflow_id: e.target.value})}
              style={styles.formInput}
              placeholder="只能包含字母、数字、连字符和下划线"
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>文件名 *</label>
            <input
              type="text"
              value={formData.file_name}
              onChange={(e) => setFormData({...formData, file_name: e.target.value})}
              style={styles.formInput}
              placeholder="例如: workflow.yaml"
              required
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>工作流类型</label>
            <select
              value={formData.workflow_type}
              onChange={(e) => setFormData({...formData, workflow_type: e.target.value})}
              style={styles.formInput}
            >
              <option value="custom">自定义</option>
              <option value="automation">自动化</option>
              <option value="data_processing">数据处理</option>
              <option value="monitoring">监控</option>
            </select>
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>描述</label>
            <textarea
              value={formData.description}
              onChange={(e) => setFormData({...formData, description: e.target.value})}
              style={styles.formTextarea}
              rows="2"
            />
          </div>
          <div style={styles.formGroup}>
            <label style={styles.formLabel}>工作流内容 (YAML格式) *</label>
            <textarea
              value={formData.content}
              onChange={(e) => setFormData({...formData, content: e.target.value})}
              style={{...styles.formTextarea, height: '160px', fontFamily: 'Monaco, Consolas, monospace', fontSize: '12px'}}
              placeholder={`name: "示例工作流"
description: "这是一个示例工作流"
steps:
  - name: "步骤1"
    action: "process_data"
    parameters:
      input: "sensor_data"
  - name: "步骤2"
    action: "send_notification"
    parameters:
      message: "处理完成"`}
              required
            />
          </div>
          <div style={styles.formActions}>
            <button
              type="button"
              onClick={() => setShowCreateForm(false)}
              style={styles.cancelButton}
              disabled={submitting}
            >
              取消
            </button>
            <button
              type="button"
              onClick={handleSubmit}
              style={{
                ...styles.primaryButton,
                opacity: (submitting || !formData.workflow_id || !formData.file_name || !formData.content) ? 0.5 : 1
              }}
              disabled={submitting || !formData.workflow_id || !formData.file_name || !formData.content}
            >
              {submitting ? '创建中...' : '创建'}
            </button>
          </div>
        </div>
      </Modal>

      {/* 工作流详情模态框 */}
      <Modal
        isOpen={showDetailModal}
        onClose={() => setShowDetailModal(false)}
        title="工作流详情"
        size="large"
      >
        {selectedWorkflow && (
          <div>
            <div style={styles.grid2}>
              <div>
                <label style={styles.formLabel}>工作流ID</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedWorkflow.workflow_id}</p>
              </div>
              <div>
                <label style={styles.formLabel}>文件名</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedWorkflow.file_name}</p>
              </div>
              <div>
                <label style={styles.formLabel}>状态</label>
                <span style={getBadgeStyle(selectedWorkflow.status)}>
                  {selectedWorkflow.status}
                </span>
              </div>
              <div>
                <label style={styles.formLabel}>类型</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedWorkflow.workflow_type}</p>
              </div>
              <div>
                <label style={styles.formLabel}>版本</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedWorkflow.version}</p>
              </div>
              <div>
                <label style={styles.formLabel}>执行次数</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedWorkflow.execution_count}</p>
              </div>
            </div>
            
            {selectedWorkflow.description && (
              <div style={{margin: '24px 0'}}>
                <label style={styles.formLabel}>描述</label>
                <p style={{margin: '4px 0 0 0', fontSize: '14px'}}>{selectedWorkflow.description}</p>
              </div>
            )}
            
            <div>
              <label style={{...styles.formLabel, marginBottom: '8px'}}>工作流内容</label>
              <pre style={styles.preCode}>
                {selectedWorkflow.content}
              </pre>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
};

export default DigitalTwinPlatform;