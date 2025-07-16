import React, { useEffect, useState } from "react";
import {
  List,
  Button,
  message,
  Modal,
  Form,
  Input,
  Select,
  Space,
  Popconfirm,
} from "antd";
import axios from "axios";

// Base URL for your FastAPI backend
const API_BASE_URL = "http://localhost:8000"; // 确保这与你的FastAPI服务器地址匹配

export default function TwinsPage() {
  const [twins, setTwins] = useState([]);
  const [models, setModels] = useState([]);
  const [loading, setLoading] = useState(false);
  const [modalVisible, setModalVisible] = useState(false);
  const [form] = Form.useForm(); // 增加 form 实例用于表单控制

  const fetchTwins = async () => {
    setLoading(true);
    try {
      // 使用完整的API路径
      const res = await axios.get(`${API_BASE_URL}/twins/`);
      setTwins(res.data);
    } catch (error) {
      console.error("获取 Twins 失败:", error);
      message.error("获取 Twins 失败");
    } finally {
      setLoading(false);
    }
  };

  const fetchModels = async () => {
    try {
      // 使用完整的API路径
      const res = await axios.get(`${API_BASE_URL}/models/`);
      // 确保模型数据结构与后端 ModelOverview 匹配
      // 后端 ModelOverview 有 model_id 和 display_name
      setModels(res.data);
    } catch (error) {
      console.error("获取模型失败:", error);
      message.error("获取模型失败");
    }
  };

  const deleteTwin = async (twin_id) => {
    try {
      // 使用完整的API路径
      await axios.delete(`${API_BASE_URL}/twins/${twin_id}`);
      message.success("删除成功");
      fetchTwins(); // 刷新列表
    } catch (error) {
      console.error("删除 Twin 失败:", error);
      let errorMessage = "删除失败";
      if (error.response && error.response.data && error.response.data.detail) {
        errorMessage = error.response.data.detail;
      } else if (error.message) {
        errorMessage = error.message;
      }
      message.error(errorMessage);
    }
  };

  const onFinish = async (values) => {
    let propertiesParsed = null;
    // 尝试解析 properties 字段为 JSON 对象
    if (values.properties) {
      try {
        propertiesParsed = JSON.parse(values.properties);
      } catch (e) {
        message.error("属性 JSON 格式不正确，请检查！");
        return; // JSON 解析失败，阻止提交
      }
    }

    const payload = {
      twin_id: values.twin_id,
      model_id: values.model_id,
      // 确保 properties 为 null 或有效的 JSON 对象
      properties: propertiesParsed,
    };

    try {
      // 使用完整的API路径
      await axios.post(`${API_BASE_URL}/twins/`, payload);
      message.success("创建成功");
      setModalVisible(false);
      form.resetFields(); // 创建成功后重置表单
      fetchTwins(); // 刷新列表
    } catch (e) {
      console.error("创建 Twin 失败:", e);
      let errorMessage = "创建失败，请检查输入";
      if (e.response && e.response.data && e.response.data.detail) {
        errorMessage = e.response.data.detail;
      } else if (e.message) {
        errorMessage = e.message;
      }
      message.error(errorMessage);
    }
  };

  useEffect(() => {
    fetchTwins();
    fetchModels();
  }, []);

  return (
    <>
      <Space style={{ marginBottom: 16 }}>
        <Button type="primary" onClick={() => {
          setModalVisible(true);
          form.resetFields(); // 打开弹窗时重置表单
        }}>
          新建 Twin
        </Button>
      </Space>

      <List
        loading={loading}
        bordered
        dataSource={twins}
        renderItem={(item) => (
          <List.Item
            key={item.twin_id} // 使用 twin_id 作为 key
            actions={[
              <Popconfirm
                title="确认删除？"
                onConfirm={() => deleteTwin(item.twin_id)}
                okText="是"
                cancelText="否"
              >
                <Button danger size="small">
                  删除
                </Button>
              </Popconfirm>,
            ]}
          >
            <List.Item.Meta
              // 根据后端 TwinModel 的字段显示
              title={item.twin_id} // 通常 twin_id 是唯一的标识符
              description={
                <>
                  {item.display_name && <p>名称: {item.display_name}</p>} {/* 如果 TwinModel 有 display_name 字段 */}
                  <p>模型 ID: {item.model_id}</p>
                  {/* 如果有 properties 且不为空，则显示 */}
                  {item.properties && Object.keys(item.properties).length > 0 && (
                    <p>属性: {JSON.stringify(item.properties)}</p>
                  )}
                  {item.last_telemetry_timestamp && (
                    <p>最新遥测时间: {new Date(item.last_telemetry_timestamp).toLocaleString()}</p>
                  )}
                </>
              }
            />
          </List.Item>
        )}
      />

      <Modal
        title="创建 Twin"
        open={modalVisible} // Ant Design 5.x 使用 'open' 代替 'visible'
        onCancel={() => setModalVisible(false)}
        footer={null} // 隐藏默认的 Footer 按钮
      >
        <Form form={form} layout="vertical" onFinish={onFinish}> {/* 绑定 form 实例 */}
          <Form.Item
            label="Twin ID"
            name="twin_id"
            rules={[{ required: true, message: "请输入 Twin ID" }]}
          >
            <Input />
          </Form.Item>
          <Form.Item
            label="选择模型"
            name="model_id"
            rules={[{ required: true, message: "请选择模型" }]}
          >
            <Select placeholder="请选择">
                {models.map((m) => (
                // 后端 ModelOverview 有 model_id 和 display_name
                <Select.Option key={m.model_id} value={m.model_id}>
                    {m.display_name || m.model_id} {/* 优先显示 display_name */}
                </Select.Option>
                ))}
            </Select>
            </Form.Item>
          <Form.Item label="属性 JSON (可选)" name="properties">
            <Input.TextArea
              rows={4}
              placeholder='如：{"prop1": "value1", "temp": 25}'
              // onChange 时不做强校验，校验留给 onFinish 中的 JSON.parse
            />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit">
              创建
            </Button>
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}