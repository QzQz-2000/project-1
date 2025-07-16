import React, { useEffect, useState } from "react";
import {
  List,
  Button,
  message,
  Modal,
  Upload,
  Typography,
  Input,
  Space,
  Form,
  Select, // For environment selection
} from "antd";
import { UploadOutlined, PlusOutlined, EditOutlined } from "@ant-design/icons";
import axios from "axios";

const { Paragraph } = Typography;
const { Option } = Select; // Destructure Option from Select

// Base URL for your FastAPI backend
const API_BASE_URL = "http://localhost:8000"; // Make sure this matches your FastAPI server

export default function ModelsPage() {
  const [models, setModels] = useState([]);
  const [environments, setEnvironments] = useState([]); // New state for environments
  const [loading, setLoading] = useState(false);
  const [uploadVisible, setUploadVisible] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [file, setFile] = useState(null);
  const [preview, setPreview] = useState("");
  const [selectedEnvironmentId, setSelectedEnvironmentId] = useState(""); // New state for selected environment

  const [modelDetailVisible, setModelDetailVisible] = useState(false);
  const [selectedModelJson, setSelectedModelJson] = useState("");
  const [searchText, setSearchText] = useState("");

  const [editModalVisible, setEditModalVisible] = useState(false);
  const [editingModel, setEditingModel] = useState(null);
  const [editForm] = Form.useForm(); // Ant Design Form instance

  // --- API Calls ---

  const fetchModels = async () => {
    setLoading(true);
    try {
      const res = await axios.get(`${API_BASE_URL}/models/`);
      setModels(res.data);
    } catch (e) {
      message.error("获取模型列表失败");
      console.error("获取模型列表失败:", e);
    } finally {
      setLoading(false);
    }
  };

  // New: Fetch environments for upload dropdown
  const fetchEnvironments = async () => {
    try {
      // Assuming you have an /environments/ endpoint in FastAPI
      const res = await axios.get(`${API_BASE_URL}/environments/`);
      setEnvironments(res.data);
      if (res.data.length > 0) {
        setSelectedEnvironmentId(res.data[0].environment_id); // Set first environment as default
      }
    } catch (e) {
      message.error("获取环境列表失败");
      console.error("获取环境列表失败:", e);
    }
  };

  const confirmDeleteModel = (modelId) => {
    Modal.confirm({
      title: "确定要删除这个模型吗？",
      content: `模型 ID：${modelId}`,
      okText: "确认删除",
      okType: "danger",
      cancelText: "取消",
      onOk: () => deleteModel(modelId),
    });
  };

  const deleteModel = async (modelId) => {
    try {
      const res = await axios.delete(`${API_BASE_URL}/models/${modelId}`);
      message.success(res.data.message || "删除成功"); // FastAPI returns a message
      fetchModels();
    } catch (error) {
      console.error("删除失败:", error);
      let content = "删除失败。";
      if (error.response && error.response.data) {
        content = error.response.data.detail || error.response.data.message || JSON.stringify(error.response.data);
      } else if (error.message) {
        content = error.message;
      }
      Modal.error({
        title: "删除失败",
        content,
      });
    }
  };

  const readFileContent = (file) => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target.result;
        resolve(text);
      };
      reader.onerror = () => reject("读取文件失败");
      reader.readAsText(file);
    });
  };

  const beforeUpload = async (file) => {
    if (!file.name.endsWith(".json")) {
      message.error("请选择 JSON 文件上传");
      return Upload.LIST_IGNORE;
    }
    try {
      const content = await readFileContent(file);
      // Basic check if it's valid JSON
      JSON.parse(content);
      setPreview(content);
      setFile(file);
      return false; // Prevent default upload behavior
    } catch (err) {
      console.error("读取或解析文件失败:", err);
      message.error(`文件内容无效或无法解析: ${String(err)}`);
      return Upload.LIST_IGNORE;
    }
  };

  const handleUpload = async () => {
    if (!file) {
      message.warning("请先选择文件");
      return;
    }
    if (!selectedEnvironmentId) {
      message.warning("请选择模型所属的环境");
      return;
    }

    const formData = new FormData();
    formData.append("file", file);

    setUploading(true);
    try {
      const res = await axios.post(
        `${API_BASE_URL}/models/upload-json/?environment_id=${selectedEnvironmentId}`,
        formData,
        {
          headers: {
            "Content-Type": "multipart/form-data", // axios handles this automatically with FormData
          },
        }
      );
      message.success(`模型 "${res.data.model_id}" 上传成功！`);
      setUploadVisible(false);
      setFile(null);
      setPreview("");
      setSelectedEnvironmentId(
        environments.length > 0 ? environments[0].environment_id : ""
      ); // Reset environment
      fetchModels(); // Refresh the list
    } catch (error) {
      console.error("上传失败:", error);
      let content = "上传过程中发生错误。";
      if (error.response && error.response.data) {
        content = error.response.data.detail || error.response.data.message || JSON.stringify(error.response.data);
      } else if (error.message) {
        content = error.message;
      }
      Modal.error({
        title: "上传失败",
        content,
      });
    } finally {
      setUploading(false);
    }
  };

  // --- Edit Model Functionality ---
  const showEditModal = (model) => {
    setEditingModel(model);
    setEditModalVisible(true);
    editForm.setFieldsValue({
      name: model.name,
      description: model.description,
      environment_id: model.environment_id, // Display environment ID, but keep it disabled for editing
    });
  };

  const handleEditSubmit = async (values) => {
    if (!editingModel) return;

    setUploading(true); // Using uploading state for consistency
    try {
      const updatedModelData = {
        ...editingModel, // Keep existing fields
        name: values.name,
        description: values.description,
        // environment_id is not changed here as per your FastAPI PUT endpoint logic
        // If your PUT allowed changing env_id, you'd include it here.
      };
      // FastAPI PUT expects the model_id from the path and the full ModelSchema in the body
      const res = await axios.put(
        `${API_BASE_URL}/models/${editingModel.model_id}`,
        updatedModelData
      );
      message.success(`模型 "${res.data.model_id}" 更新成功！`);
      setEditModalVisible(false);
      setEditingModel(null);
      fetchModels();
    } catch (error) {
      console.error("更新失败:", error);
      let content = "更新过程中发生错误。";
      if (error.response && error.response.data) {
        content = error.response.data.detail || error.response.data.message || JSON.stringify(error.response.data);
      } else if (error.message) {
        content = error.message;
      }
      Modal.error({
        title: "更新失败",
        content,
      });
    } finally {
      setUploading(false);
    }
  };

  const handleEditCancel = () => {
    setEditModalVisible(false);
    setEditingModel(null);
    editForm.resetFields();
  };

  const showModelDetail = (item) => {
    let jsonStr = "";
    try {
      // Assuming 'item' is now the ModelSchema directly from FastAPI
      jsonStr = JSON.stringify(item, null, 2);
    } catch (e) {
      jsonStr = "无法解析模型 JSON 内容";
    }
    setSelectedModelJson(jsonStr);
    setModelDetailVisible(true);
  };

  const filteredModels = models.filter((item) => {
    const text = searchText.trim().toLowerCase();
    if (!text) return true;
    // Adapt to ModelSchema properties: name, model_id
    const name = (item.name || "").toLowerCase();
    const id = (item.model_id || "").toLowerCase();
    return name.includes(text) || id.includes(text);
  });

  useEffect(() => {
    fetchModels();
    fetchEnvironments(); // Fetch environments on component mount
  }, []);

  return (
    <>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          marginBottom: 16,
          alignItems: "center",
        }}
      >
        <h2>模型列表</h2>
        <Space>
          <Input.Search
            placeholder="搜索模型名称或ID"
            allowClear
            onChange={(e) => setSearchText(e.target.value)}
            style={{ width: 200 }}
            value={searchText}
          />
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setUploadVisible(true)}
          >
            上传模型
          </Button>
        </Space>
      </div>

      <List
        loading={loading}
        bordered
        dataSource={filteredModels}
        renderItem={(item) => (
          <List.Item
            key={item.model_id} // Use model_id as key
            actions={[
              <Button
                type="text"
                icon={<EditOutlined />}
                size="small"
                onClick={(e) => {
                  e.stopPropagation(); // Prevent List.Item onClick
                  showEditModal(item);
                }}
              >
                编辑
              </Button>,
              <Button
                danger
                size="small"
                onClick={(e) => {
                  e.stopPropagation(); // Prevent List.Item onClick
                  confirmDeleteModel(item.model_id);
                }}
              >
                删除
              </Button>,
            ]}
            onClick={() => showModelDetail(item)} // Click on item to show full JSON
            style={{ cursor: "pointer" }}
          >
            <List.Item.Meta
              // Adapt to ModelSchema properties: name, description, model_id
              title={item.name || item.model_id || "未命名模型"}
              description={
                <small style={{ color: "#888" }}>
                  ID: {item.model_id || "无 ID"}{" "}
                  {item.environment_id && `(环境: ${item.environment_id})`}
                  <br />
                  {item.description || "无描述"}
                </small>
              }
            />
          </List.Item>
        )}
      />

      {/* 上传模型弹窗 */}
      <Modal
        open={uploadVisible}
        title="上传模型文件"
        onCancel={() => {
          setUploadVisible(false);
          setFile(null);
          setPreview("");
        }}
        footer={[
          <Button
            key="cancel"
            onClick={() => {
              setUploadVisible(false);
              setFile(null);
              setPreview("");
            }}
            disabled={uploading}
          >
            取消
          </Button>,
          <Button
            key="upload"
            type="primary"
            onClick={handleUpload}
            loading={uploading}
            disabled={!file || !selectedEnvironmentId}
          >
            上传
          </Button>,
        ]}
      >
        <Form layout="vertical">
          <Form.Item label="选择模型文件">
            <Upload
              accept=".json"
              beforeUpload={beforeUpload}
              showUploadList={file ? [{ uid: "-1", name: file.name }] : false}
              onRemove={() => {
                setFile(null);
                setPreview("");
              }}
              maxCount={1}
              disabled={uploading}
            >
              <Button icon={<UploadOutlined />} disabled={uploading}>
                选择模型文件
              </Button>
            </Upload>
          </Form.Item>

          {file && (
            <Form.Item label="文件内容预览">
              <div
                style={{
                  padding: 12,
                  border: "1px solid #eee",
                  borderRadius: 4,
                  backgroundColor: "#fafafa",
                  whiteSpace: "pre-wrap",
                  fontFamily: "monospace",
                  maxHeight: 200,
                  overflowY: "auto",
                }}
              >
                <Paragraph>{preview}</Paragraph>
              </div>
            </Form.Item>
          )}

          <Form.Item
            label="选择所属环境"
            rules={[{ required: true, message: "请选择模型所属环境!" }]}
          >
            <Select
              placeholder="请选择环境"
              value={selectedEnvironmentId}
              onChange={(value) => setSelectedEnvironmentId(value)}
              disabled={uploading}
            >
              {environments.map((env) => (
                <Option key={env.environment_id} value={env.environment_id}>
                  {env.name || env.environment_id}
                </Option>
              ))}
            </Select>
          </Form.Item>
        </Form>
      </Modal>

      {/* 查看模型详情弹窗 */}
      <Modal
        open={modelDetailVisible}
        title="模型详情 JSON"
        footer={[
          <Button key="close" onClick={() => setModelDetailVisible(false)}>
            关闭
          </Button>,
        ]}
        width={700}
        onCancel={() => setModelDetailVisible(false)}
      >
        <pre
          style={{
            whiteSpace: "pre-wrap",
            wordBreak: "break-word",
            maxHeight: 400,
            overflowY: "auto",
            backgroundColor: "#f6f8fa",
            padding: 12,
            borderRadius: 4,
            fontFamily: "monospace",
          }}
        >
          {selectedModelJson}
        </pre>
      </Modal>

      {/* 编辑模型弹窗 */}
      <Modal
        open={editModalVisible}
        title={`编辑模型: ${editingModel?.model_id}`}
        onCancel={handleEditCancel}
        footer={[
          <Button key="cancel" onClick={handleEditCancel} disabled={uploading}>
            取消
          </Button>,
          <Button
            key="submit"
            type="primary"
            onClick={() => editForm.submit()} // Trigger form submission
            loading={uploading}
          >
            保存
          </Button>,
        ]}
      >
        <Form
          form={editForm}
          layout="vertical"
          onFinish={handleEditSubmit}
          initialValues={editingModel}
        >
          <Form.Item label="模型 ID">
            <Input value={editingModel?.model_id} disabled />
          </Form.Item>
          <Form.Item
            label="名称"
            name="name"
            rules={[{ required: true, message: "请输入模型名称!" }]}
          >
            <Input />
          </Form.Item>
          <Form.Item label="描述" name="description">
            <Input.TextArea rows={4} />
          </Form.Item>
          <Form.Item label="所属环境 ID">
            <Input value={editingModel?.environment_id} disabled />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}