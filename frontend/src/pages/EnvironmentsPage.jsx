import React, { useState, useEffect } from "react";
import { Button, List, Modal, Form, Input, message } from "antd";
import axios from "axios";

// Base URL for your FastAPI backend
const API_BASE_URL = "http://localhost:8000"; // Make sure this matches your FastAPI server

export default function EnvironmentsPage() {
  const [environments, setEnvironments] = useState([]);
  const [modalVisible, setModalVisible] = useState(false);
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false); // Add loading state

  // 初始化加载环境列表
  useEffect(() => {
    fetchEnvironments();
  }, []);

  // 获取环境列表
  const fetchEnvironments = async () => {
    setLoading(true);
    try {
      // 调整API路径以匹配FastAPI后端
      const res = await axios.get(`${API_BASE_URL}/environments/`);
      setEnvironments(res.data);
    } catch (error) {
      console.error("获取环境列表失败:", error);
      message.error("获取环境列表失败");
    } finally {
      setLoading(false);
    }
  };

  // 删除环境
  const deleteEnvironment = async (environment_id) => {
    Modal.confirm({
      title: "确认删除环境",
      content: `确定要删除环境 "${environment_id}" 吗？`,
      okText: "删除",
      okType: "danger",
      cancelText: "取消",
      onOk: async () => {
        try {
          // 调试日志：确认删除时传递的 ID
          console.log("Attempting to delete environment with ID:", environment_id);
          // 调整API路径以匹配FastAPI后端
          const res = await axios.delete(`${API_BASE_URL}/environments/${environment_id}`);
          message.success(res.data.message || "环境删除成功"); // FastAPI可能会返回一个message
          fetchEnvironments();
        } catch (error) {
          console.error("环境删除失败:", error);
          let errorMessage = "环境删除失败";
          if (error.response && error.response.data && error.response.data.detail) {
            errorMessage = error.response.data.detail;
          } else if (error.message) {
            errorMessage = error.message;
          }
          message.error(errorMessage);
        }
      },
    });
  };

  // 创建新环境
  const onFinish = async (values) => {
    try {
      // 调整API路径并确保发送的数据字段与FastAPI的EnvironmentCreateRequest匹配
      // values 已经是 { environment_id: "...", name: "..." }
      const res = await axios.post(`${API_BASE_URL}/environments/`, values);
      message.success(`环境 "${res.data.name || res.data.environment_id}" 创建成功`);
      setModalVisible(false);
      form.resetFields(); // 重置表单字段
      fetchEnvironments(); // 刷新列表
    } catch (error) {
      console.error("环境创建失败:", error);
      let errorMessage = "环境创建失败";
      if (error.response && error.response.data && error.response.data.detail) {
        errorMessage = error.response.data.detail;
      } else if (error.message) {
        errorMessage = error.message;
      }
      message.error(errorMessage);
    }
  };

  return (
    <>
      <Button type="primary" onClick={() => setModalVisible(true)} style={{ marginBottom: 16 }}>
        新建环境
      </Button>

      <List
        bordered
        loading={loading}
        dataSource={environments}
        renderItem={(item) => (
          <List.Item
            key={item.environment_id} // 使用后端返回的 environment_id 作为key
            actions={[
              <Button danger size="small" onClick={() => deleteEnvironment(item.environment_id)}>
                删除
              </Button>,
            ]}
          >
            <List.Item.Meta
              // 适配后端 EnvironmentModel 的字段：name, environment_id, description
              title={item.name || item.environment_id} // 优先显示name，如果没有则显示environment_id
              description={
                <>
                  <p>ID: {item.environment_id}</p>
                  {item.description && <p>描述: {item.description}</p>}
                  {item.location && <p>位置: {item.location}</p>}
                  {item.owner && <p>负责人: {item.owner}</p>}
                </>
              }
            />
          </List.Item>
        )}
      />

      <Modal
        title="新建环境"
        open={modalVisible}
        onCancel={() => {
          setModalVisible(false);
          form.resetFields(); // 取消时重置表单
        }}
        onOk={() => form.submit()}
      >
        <Form form={form} onFinish={onFinish} layout="vertical">
          {/* Form.Item 的 name 属性必须与后端 Pydantic 模型 EnvironmentCreateRequest 的字段名一致 */}
          <Form.Item
            name="environment_id" // 对应后端 EnvironmentCreateRequest 的 environment_id
            label="环境ID"
            rules={[{ required: true, message: "请输入环境ID!" }]}
          >
            <Input />
          </Form.Item>
          <Form.Item
            name="name" // 对应后端 EnvironmentCreateRequest 的 name
            label="环境名称"
            rules={[{ required: true, message: "请输入环境名称!" }]}
          >
            <Input />
          </Form.Item>
          <Form.Item
            name="description" // 可选字段
            label="描述"
          >
            <Input.TextArea />
          </Form.Item>
          <Form.Item
            name="location" // 可选字段
            label="位置"
          >
            <Input />
          </Form.Item>
          <Form.Item
            name="owner" // 可选字段
            label="负责人"
          >
            <Input />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}