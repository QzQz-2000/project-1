// src/components/MainLayout.jsx
import React from 'react';
import { Outlet, useLocation } from 'react-router-dom';
import Navbar from './Navbar';
import Sidebar from './Sidebar';

function MainLayout() {
  const location = useLocation();
  const isHomePage = location.pathname === '/'; // 判断当前是否是首页

  return (
    <div className="app-container"> {/* 整个应用的容器 */}
      <Navbar /> {/* 顶部导航栏 */}
      <div className="main-content-wrapper"> {/* 主内容区和侧边栏的容器 */}
        {/* 根据是否是首页来决定侧边栏显示的内容 */}
        <Sidebar isHomePage={isHomePage} />
        <main className="main-content-area"> {/* 页面主要内容区域 */}
          <Outlet /> {/* 路由嵌套的内容会在这里渲染 */}
        </main>
      </div>
    </div>
  );
}

export default MainLayout;