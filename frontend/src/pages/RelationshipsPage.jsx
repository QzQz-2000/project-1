import React, { useEffect, useState } from "react";
import { Form, Input, Button, Select, message } from "antd";
import axios from "axios";

export default function RelationshipsPage() {
  const [twins, setTwins] = useState([]);

  const fetchTwins = async () => {
    try {
      const res = await axios.get("/twins/");
      setTwins(res.data);
    } catch {
      message.error("获取 Twins 失败");
    }
  };

  useEffect(() => {
    fetchTwins();
  }, []);

  const onFinish = async (values) => {
    try {
      await axios.post("/relationships/", {
        source_id: values.source_id,
        target_id: values.target_id,
        relationship_name: values.relationship_name,
        properties: {},
      });
      message.success("关系创建成功");
    } catch (e) {
      message.error(e.response?.data?.detail || "创建失败");
    }
  };

  return (
    <>
      <h2>关系管理</h2>
      <Form layout="vertical" onFinish={onFinish} style={{ maxWidth: 400 }}>
        <Form.Item
          label="来源 Twin"
          name="source_id"
          rules={[{ required: true, message: "请选择来源 Twin" }]}
        >
          <Select placeholder="请选择来源 Twin">
            {twins.map((t) => (
              <Select.Option key={t.twin_id} value={t.twin_id}>
                {t.twin_id}
              </Select.Option>
            ))}
          </Select>
        </Form.Item>
        <Form.Item
          label="目标 Twin"
          name="target_id"
          rules={[{ required: true, message: "请选择目标 Twin" }]}
        >
          <Select placeholder="请选择目标 Twin">
            {twins.map((t) => (
              <Select.Option key={t.twin_id} value={t.twin_id}>
                {t.twin_id}
              </Select.Option>
            ))}
          </Select>
        </Form.Item>
        <Form.Item
          label="关系名称"
          name="relationship_name"
          rules={[{ required: true, message: "请输入关系名称" }]}
        >
          <Input placeholder="关系名称，如关联、依赖" />
        </Form.Item>
        <Form.Item>
          <Button type="primary" htmlType="submit">
            创建关系
          </Button>
        </Form.Item>
      </Form>
    </>
  );
}
