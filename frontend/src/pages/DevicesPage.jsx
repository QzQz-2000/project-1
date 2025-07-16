import React, { useEffect, useState } from "react";
import { Select, Button, List, Modal, Form, Input, message, Spin, Typography, Space } from "antd";
import axios from "axios";

const { Option } = Select;
const { Text } = Typography;

export default function DevicesPage() {
  const [envs, setEnvs] = useState([]);
  const [selectedEnv, setSelectedEnv] = useState(null);
  const [devices, setDevices] = useState([]);
  const [modalVisible, setModalVisible] = useState(false);
  const [loadingEnvironments, setLoadingEnvironments] = useState(false);
  const [loadingDevices, setLoadingDevices] = useState(false);
  const [form] = Form.useForm();

  // Fetch environments on component mount
  useEffect(() => {
    fetchEnvironments();
  }, []);

  // Fetch devices when selectedEnv changes
  useEffect(() => {
    if (selectedEnv) {
      fetchDevices(selectedEnv);
    } else {
      setDevices([]); // Clear devices if no environment is selected
    }
  }, [selectedEnv]);

  /**
   * Fetches the list of environments from the backend.
   */
  const fetchEnvironments = async () => {
    setLoadingEnvironments(true);
    try {
      const res = await axios.get("/environments/");
      setEnvs(res.data);
    } catch (error) {
      console.error("Failed to fetch environments:", error);
      message.error("获取环境列表失败");
    } finally {
      setLoadingEnvironments(false);
    }
  };

  /**
   * Fetches the list of devices for a specific environment.
   * @param {string} env_id The ID of the selected environment.
   */
  const fetchDevices = async (env_id) => {
    setLoadingDevices(true);
    try {
      // CORRECTED: Use query parameter for environment_id
      const res = await axios.get(`/devices/?environment_id=${env_id}`); //
      setDevices(res.data);
    } catch (error) {
      console.error(`Failed to fetch devices for environment ${env_id}:`, error);
      message.error("获取设备列表失败");
    } finally {
      setLoadingDevices(false);
    }
  };

  /**
   * Deletes a specific device.
   * IMPORTANT: This frontend assumes a DELETE /devices/{device_id} endpoint exists.
   * If your backend only supports cascading deletion via /environments/{environment_id},
   * you will need to adjust this logic or implement the backend endpoint.
   * @param {string} device_id The ID of the device to delete.
   */
  const deleteDevice = async (device_id) => { // Modified to only take device_id
    Modal.confirm({
      title: '确认删除设备',
      content: `确定要删除设备 ${device_id} 吗？`,
      okText: '删除',
      cancelText: '取消',
      onOk: async () => {
        try {
          // ASSUMPTION: Backend has a DELETE /devices/{device_id} endpoint.
          // If not, this will fail. See backend suggestions below.
          await axios.delete(`/devices/${device_id}`); //
          message.success("设备删除成功");
          fetchDevices(selectedEnv); // Refresh the device list
        } catch (error) {
          console.error(`Failed to delete device ${device_id}:`, error);
          message.error(error.response?.data?.detail || "设备删除失败");
        }
      },
    });
  };

  /**
   * Handles the submission of the new device registration form.
   * @param {object} values Form values including device_id and environment_id.
   */
  const onFinish = async (values) => {
    try {
      await axios.post("/devices/", values); //
      message.success("设备注册成功");
      setModalVisible(false);
      form.resetFields();
      fetchDevices(selectedEnv); // Refresh the device list after registration
    } catch (error) {
      console.error("Failed to register device:", error);
      message.error(error.response?.data?.detail || "设备注册失败");
    }
  };

  return (
    <>
      <Space style={{ marginBottom: 16 }}>
        <Select
          placeholder="选择环境"
          style={{ width: 300 }}
          value={selectedEnv}
          onChange={setSelectedEnv}
          loading={loadingEnvironments}
          disabled={loadingEnvironments}
        >
          {envs.map((env) => (
            <Option key={env.environment_id} value={env.environment_id}>
              {env.name || env.environment_id} {/* Display name if available, otherwise ID */}
            </Option>
          ))}
        </Select>
        <Button
          type="primary"
          disabled={!selectedEnv || loadingDevices}
          onClick={() => {
            setModalVisible(true);
            form.setFieldsValue({ env_id: selectedEnv }); // Pre-fill environment ID in modal
          }}
        >
          注册设备
        </Button>
      </Space>

      <Spin spinning={loadingDevices} tip="加载设备中...">
        <List
          bordered
          dataSource={devices}
          locale={{
            emptyText: (
              <Text type="secondary">
                {selectedEnv
                  ? "当前环境下没有设备。"
                  : "请选择一个环境来查看设备。"}
              </Text>
            ),
          }}
          renderItem={(item) => (
            <List.Item
              actions={[
                <Button
                  danger
                  size="small"
                  onClick={() => deleteDevice(item.device_id)} // Pass only device_id
                >
                  删除
                </Button>,
              ]}
            >
              <List.Item.Meta
              // Display name if available, otherwise ID
                title={item.name || item.device_id}
                description={`ID: ${item.device_id}, 类型: ${item.device_type || 'N/A'}, 环境ID: ${item.environment_id}`}
              />
            </List.Item>
          )}
        />
      </Spin>

      <Modal
        title="注册设备"
        open={modalVisible}
        onCancel={() => {
          setModalVisible(false);
          form.resetFields(); // Reset form when modal is closed
        }}
        onOk={() => form.submit()}
        confirmLoading={false} // You might want to add a state for form submission loading
      >
        <Form form={form} onFinish={onFinish} layout="vertical" initialValues={{ env_id: selectedEnv }}>
          <Form.Item
            name="env_id"
            label="环境ID"
            rules={[{ required: true, message: "请选择环境" }]}
          >
            {/* Display selected environment name, not just ID */}
            <Select disabled>
              {selectedEnv && (
                <Option value={selectedEnv}>
                  {envs.find(env => env.environment_id === selectedEnv)?.name || selectedEnv}
                </Option>
              )}
            </Select>
          </Form.Item>
          <Form.Item
            name="device_id"
            label="设备ID"
            rules={[{ required: true, message: "请输入设备ID" }]}
          >
            <Input placeholder="请输入设备ID" />
          </Form.Item>
          <Form.Item
            name="name"
            label="设备名称"
          >
            <Input placeholder="请输入设备名称 (可选)" />
          </Form.Item>
          <Form.Item
            name="device_type"
            label="设备类型"
          >
            <Input placeholder="请输入设备类型 (可选, 例如: sensor, actuator)" />
          </Form.Item>
          {/* You can add more fields from DeviceCreateRequest here if needed, e.g., location, properties */}
        </Form>
      </Modal>
    </>
  );
}