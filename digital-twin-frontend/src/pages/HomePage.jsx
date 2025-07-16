// src/pages/HomePage.jsx
import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import apiService from '../services/apiService';

function HomePage() {
  const navigate = useNavigate();
  const [showCreateModal, setShowCreateModal] = useState(false);

  const [newEnvironmentId, setNewEnvironmentId] = useState('');
  const [newEnvironmentName, setNewEnvironmentName] = useState('');
  const [newEnvironmentDescription, setNewEnvironmentDescription] = useState('');
  const [isCreating, setIsCreating] = useState(false);
  const [creationError, setCreationError] = useState(null);

  const [environments, setEnvironments] = useState([]);
  const [loadingEnvironments, setLoadingEnvironments] = useState(true);
  const [environmentsError, setEnvironmentsError] = useState(null);

  // notification: { type: 'success' | 'error', message: '', isModal?: boolean }
  const [notification, setNotification] = useState(null); 

  useEffect(() => {
    const fetchEnvironments = async () => {
      setLoadingEnvironments(true);
      setEnvironmentsError(null);
      try {
        const data = await apiService.get('/environments', undefined);
        setEnvironments(data);
      } catch (err) {
        console.error('Failed to fetch environments:', err);
        setEnvironmentsError(err.detail || err.message || '无法加载环境列表，请检查网络或后端服务。');
      } finally {
        setLoadingEnvironments(false);
      }
    };
    fetchEnvironments();
  }, []);

  // 显示临时通知（仅用于错误或非模态提示）
  const showTemporaryNotification = (type, message) => {
    setNotification({ type, message });
    // 3秒后自动清除通知
    setTimeout(() => {
      setNotification(null);
    }, 3000);
  };

  // 处理创建新环境
  const handleCreateEnvironment = async (e) => {
    e.preventDefault();
    setCreationError(null); // 清除之前的错误信息
    setNotification(null); // 清除之前的通知

    if (!newEnvironmentId.trim()) {
      const msg = '环境ID不能为空';
      setCreationError(msg);
      showTemporaryNotification('error', msg); // 触发错误通知
      return;
    }
    if (!newEnvironmentName.trim()) {
      const msg = '环境名称不能为空';
      setCreationError(msg);
      showTemporaryNotification('error', msg); // 触发错误通知
      return;
    }

    setIsCreating(true);

    try {
      const createdEnv = await apiService.post('/environments', undefined, {
        environment_id: newEnvironmentId.trim(),
        display_name: newEnvironmentName.trim(),
        description: newEnvironmentDescription.trim(),
      });

      console.log('Environment created successfully:', createdEnv);
      setShowCreateModal(false); // 关闭创建模态框
      setNewEnvironmentId('');
      setNewEnvironmentName('');
      setNewEnvironmentDescription('');
      setCreationError(null);

      setEnvironments(prev => [...prev, createdEnv]); // 乐观更新列表

      // 显示居中成功弹窗，不自动关闭
      setNotification({
        type: 'success',
        message: `环境 "${createdEnv.display_name || createdEnv.environment_id}" 创建成功！`,
        isModal: true, // 标记为模态弹窗
      });

      // navigate(`/environments/${createdEnv.environment_id}/dashboard`); // 已移除跳转

    } catch (err) {
      console.error('Failed to create environment:', err);
      console.log('完整的错误对象:', err);

      let errorMessage = '创建环境失败，请重试。';

      if (err.detail && typeof err.detail === 'string') {
        errorMessage = `创建失败: ${err.detail}`;
      } else if (err.message) {
        errorMessage = `创建失败: ${err.message}`;
      }

      setCreationError(errorMessage);
      showTemporaryNotification('error', errorMessage); // 错误仍然是临时提示
    } finally {
      setIsCreating(false);
    }
  };

  // 导航到现有环境
  const handleSelectExistingEnvironment = (envId) => {
    if (envId) {
      navigate(`/environments/${envId}/dashboard`);
    }
  };

  return (
    <div className="homepage-container">
      <div className="welcome-section">
        <h1 className="welcome-title">欢迎来到数字孪生平台</h1>
        <p className="welcome-subtitle">在这里管理和创建您的数字孪生环境。</p>
      </div>

      <div className="cards-container">
        {/* 卡片1: 创建新环境 */}
        <div className="card">
          <h2 className="card-title">创建新环境</h2>
          <p className="card-text">
            为您的数字孪生项目创建一个全新的隔离环境。
          </p>
          <button
            onClick={() => setShowCreateModal(true)}
            className="action-button"
          >
            创建新环境
          </button>
        </div>

        {/* 卡片2: 选择已有环境 */}
        <div className="card">
          <h2 className="card-title">选择已有环境</h2>
          {loadingEnvironments ? (
            <p className="loading-spinner">加载环境中...</p>
          ) : environmentsError ? (
            <p className="alert-message">错误：{environmentsError}</p>
          ) : environments.length === 0 ? (
            <p className="no-environments-text">您还没有任何环境。请先创建一个！</p>
          ) : (
            <ul className="environment-list" style={{ listStyle: 'none', padding: 0 }}> {/* 确保列表无默认样式 */}
              {environments.map(env => (
                <li key={env.environment_id} className="environment-list-item" style={{ marginBottom: '8px' }}> {/* 条目间距 */}
                  <button
                    onClick={() => handleSelectExistingEnvironment(env.environment_id)}
                    className="environment-select-button"
                    style={{
                      display: 'flex', // 使用 Flexbox 布局
                      justifyContent: 'space-between', // 名称和ID分列两端
                      alignItems: 'center', // 垂直居中
                      width: '100%', // 按钮占据列表项的全部宽度
                      padding: '10px 15px', // 按钮内边距
                      border: '1px solid #007bff', // 边框改为蓝色
                      borderRadius: '5px', // 圆角
                      backgroundColor: '#FFFFFF', // 背景色改为白色
                      color: '#007bff', // 字体颜色改为蓝色
                      fontSize: '1em', // 字体大小
                      textAlign: 'left', // 文本左对齐
                      cursor: 'pointer',
                      transition: 'background-color 0.2s ease, border-color 0.2s ease',
                    }}
                    // 悬停效果：背景变浅蓝，边框变深蓝
                    onMouseOver={(e) => { e.currentTarget.style.backgroundColor = '#e6f7ff'; e.currentTarget.style.borderColor = '#0056b3'; }}
                    onMouseOut={(e) => { e.currentTarget.style.backgroundColor = '#FFFFFF'; e.currentTarget.style.borderColor = '#007bff'; }}
                  >
                    <span className="environment-name" style={{ flexGrow: 1, fontWeight: 'bold', color: '#007bff' }}> {/* 字体颜色改为蓝色 */}
                      {env.display_name || '无名称'} {/* 如果没有名称，显示“无名称” */}
                    </span>
                    <span className="environment-id" style={{ fontSize: '0.9em', color: '#007bff', marginLeft: '20px' }}> {/* 字体颜色改为蓝色 */}
                      ID: {env.environment_id}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>

      {/* 调整后的通知弹窗渲染逻辑 */}
      {notification && (
        <>
          {/* 模态背景覆盖层，仅在 isModal 为 true 时显示 */}
          {notification.isModal && (
            <div style={{
              position: 'fixed',
              top: 0,
              left: 0,
              width: '100%',
              height: '100%',
              backgroundColor: 'rgba(0, 0, 0, 0.5)', // 半透明黑色
              zIndex: 9998, // 在弹窗之下，内容之上
            }} onClick={() => setNotification(null)}></div> // 点击背景可关闭
          )}

          <div style={{
            position: 'fixed',
            padding: notification.isModal ? '20px 30px' : '12px 20px', // 不同 padding
            borderRadius: notification.isModal ? '10px' : '8px', // 不同圆角
            backgroundColor: notification.type === 'success' ? '#4CAF50' : '#F44336',
            color: 'white',
            zIndex: 9999,
            boxShadow: '0 6px 20px rgba(0,0,0,0.3)',
            display: 'flex',
            flexDirection: notification.isModal ? 'column' : 'row',
            alignItems: 'center',
            gap: notification.isModal ? '15px' : '10px', // 不同间距
            opacity: 0.98,
            transition: 'opacity 0.3s ease-in-out',
            top: notification.isModal ? '50%' : '20px',
            left: notification.isModal ? '50%' : 'auto',
            right: notification.isModal ? 'auto' : '20px',
            transform: notification.isModal ? 'translate(-50%, -50%)' : 'none',
            minWidth: notification.isModal ? '300px' : 'auto', // 模态框宽度
            textAlign: notification.isModal ? 'center' : 'left', // 模态框文本居中
          }}>
            <span style={{ fontSize: notification.isModal ? '1.2em' : '1em', fontWeight: 'bold' }}>
              {notification.message}
            </span>
            <button
              onClick={() => setNotification(null)}
              style={{
                color: 'white',
                fontWeight: 'bold',
                cursor: 'pointer',
                // 按钮样式关键修改
                ...(notification.isModal ? { // 成功模态框的“确定”按钮样式
                  background: 'none',
                  border: '1px solid white',
                  fontSize: '1em',
                  padding: '8px 15px',
                  borderRadius: '5px',
                  marginTop: '10px',
                } : { // 临时错误提示的“X”按钮样式
                  background: 'rgba(255, 255, 255, 0.1)', // 轻微透明背景
                  border: '1px solid rgba(255, 255, 255, 0.3)', // 细边框
                  fontSize: '1.2em',
                  padding: '2px 8px', // 增加内边距
                  borderRadius: '4px', // 轻微圆角
                  marginLeft: 'auto', // 推到右侧
                }),
              }}
            >
              {notification.isModal ? '确定' : '&times;'}
            </button>
          </div>
        </>
      )}

      {/* 创建环境模态框 */}
      {showCreateModal && (
        <div className="modal-overlay">
          <div className="modal-content">
            <h2 className="modal-title">创建新数字孪生环境</h2>
            <form onSubmit={handleCreateEnvironment}>
              <div className="form-group">
                <label htmlFor="envId" className="form-label">环境ID:</label>
                <input
                  type="text"
                  id="envId"
                  value={newEnvironmentId}
                  onChange={(e) => setNewEnvironmentId(e.target.value)}
                  className="form-input"
                  placeholder="例如: my-production-env"
                  required
                />
              </div>
              <div className="form-group">
                <label htmlFor="envName" className="form-label">环境名称:</label>
                <input
                  type="text"
                  id="envName"
                  value={newEnvironmentName}
                  onChange={(e) => setNewEnvironmentName(e.target.value)}
                  className="form-input"
                  placeholder="例如: 生产环境, 开发沙箱"
                  required
                />
              </div>
              <div className="form-group">
                <label htmlFor="envDesc" className="form-label">描述 (可选):</label>
                <textarea
                  id="envDesc"
                  value={newEnvironmentDescription}
                  onChange={(e) => setNewEnvironmentDescription(e.target.value)}
                  className="form-textarea"
                  placeholder="环境的简短描述"
                ></textarea>
              </div>

              {creationError && <p className="error-message">{creationError}</p>}

              <div className="modal-actions">
                <button
                  type="button"
                  onClick={() => {
                    setShowCreateModal(false);
                    setNewEnvironmentId('');
                    setNewEnvironmentName('');
                    setNewEnvironmentDescription('');
                    setCreationError(null);
                    setNotification(null); // 关闭模态框时也清除通知
                  }}
                  className="cancel-button"
                  disabled={isCreating}
                >
                  取消
                </button>
                <button
                  type="submit"
                  className="submit-button"
                  disabled={isCreating}
                >
                  {isCreating ? '创建中...' : '创建'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

export default HomePage;