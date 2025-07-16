// src/components/EnvironmentLayout.jsx
import React, { useEffect } from 'react';
import { useParams, Outlet, useNavigate } from 'react-router-dom';
import { EnvironmentProvider } from './EnvironmentContext';
import Navbar from './Navbar'; // 导入 Navbar
import Sidebar from './Sidebar'; // 导入 Sidebar

function EnvironmentLayout() {
  const { environment_id } = useParams(); // 从 URL 参数中获取 environment_id
  const navigate = useNavigate();

  useEffect(() => {
    if (!environment_id) {
      // 如果 URL 中没有 environment_id，重定向到环境选择页面
      navigate('/'); // 重定向到HomePage
      return;
    }
  }, [environment_id, navigate]);

  if (!environment_id) {
    return null;
  }

  return (
    <EnvironmentProvider environmentId={environment_id}>
      <div className="app-container"> {/* 整体容器 */}
        <Navbar /> {/* 顶部导航栏 */}
        <div className="main-content-wrapper"> {/* 主内容区和侧边栏的容器 */}
          <Sidebar currentEnvironmentId={environment_id} /> {/* 侧边栏，传入当前环境ID */}
          <main className="main-content-area"> {/* 页面主要内容区域 */}
            <Outlet /> {/* 路由嵌套的内容会在这里渲染 */}
          </main>
        </div>
      </div>
    </EnvironmentProvider>
  );
}

export default EnvironmentLayout;