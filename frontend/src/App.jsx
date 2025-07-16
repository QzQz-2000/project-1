import React from "react";
import { Layout, Menu } from "antd";
import {
  FileOutlined,
  ClusterOutlined,
  LinkOutlined,
  UploadOutlined,
  WifiOutlined,
  EnvironmentOutlined,
  ThunderboltOutlined,
} from "@ant-design/icons";

import ModelsPage from "./pages/ModelsPage";
import TwinsPage from "./pages/TwinsPage";
import RelationshipsPage from "./pages/RelationshipsPage";
import DevicesPage from "./pages/DevicesPage";
import EnvironmentsPage from "./pages/EnvironmentsPage";

const { Header, Content, Footer, Sider } = Layout;

// ✅ 抽出菜单配置，易于维护和扩展
const menuItems = [
  { key: "environments", icon: <EnvironmentOutlined />, label: "环境管理", component: <EnvironmentsPage /> },
  { key: "devices", icon: <WifiOutlined />, label: "设备管理", component: <DevicesPage /> },
  { key: "models", icon: <FileOutlined />, label: "模型管理", component: <ModelsPage /> },
  { key: "twins", icon: <ClusterOutlined />, label: "Twin管理", component: <TwinsPage /> },
  { key: "relationships", icon: <LinkOutlined />, label: "关系管理", component: <RelationshipsPage /> },
];

export default function App() {
  const [collapsed, setCollapsed] = React.useState(false);
  const [selectedKey, setSelectedKey] = React.useState("models");

  const selectedItem = menuItems.find((item) => item.key === selectedKey);

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Sider collapsible collapsed={collapsed} onCollapse={setCollapsed}>
        <div
          style={{
            height: 32,
            margin: 16,
            color: "white",
            fontWeight: "bold",
            fontSize: 18,
            textAlign: "center",
            overflow: "hidden",
            whiteSpace: "nowrap",
            textOverflow: "ellipsis",
          }}
        >
          Digital Twins
        </div>
        <Menu
          theme="dark"
          selectedKeys={[selectedKey]}
          onClick={(e) => setSelectedKey(e.key)}
          items={menuItems.map(({ key, icon, label }) => ({ key, icon, label }))}
        />
      </Sider>
      <Layout>
        <Header
          style={{
            padding: "0 20px",
            background: "#fff",
            fontSize: 20,
            fontWeight: "bold",
            lineHeight: "64px",
          }}
        >
          数字孪生管理平台
        </Header>
        <Content style={{ margin: 16 }}>
          <div style={{ padding: 24, minHeight: 360, background: "#fff" }}>
            {selectedItem?.component}
          </div>
        </Content>
        <Footer style={{ textAlign: "center" }}>
          Digital Twins System
        </Footer>
      </Layout>
    </Layout>
  );
}
