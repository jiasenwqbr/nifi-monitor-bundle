package com.ztgx.nifi.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.TypeElement;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @Description:
 * @author: 贾森
 * @date: 2021年12月29日 下午4:45
 */
@Tags({"monitor数据分流", "监控", "数据质量检测"})
@CapabilityDescription("monitor数据分流")
public class ZtgxSaveErrorDataAsFileProcessor extends AbstractProcessor {
    private final Set<Relationship> relationships;
    public static final Relationship REL_MONITOR = new Relationship.Builder()
            .description("所有已成功处理的Monitor流程文件都在这里进行了路由。")
            .name("Monitor")
            .build();

    public static final Relationship REL_DIRTY = new Relationship.Builder()
            .name("DirtyData")
            .description("所有已成功处理的DirtyData流程文件都在这里进行了路由。 ")
            .build();
    public static final Relationship REL_ERRORDATA = new Relationship.Builder()
            .name("ErrorDataInfo")
            .description("所有已成功处理的ErrorDataInfo流程文件都在这里进行了路由。 ")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("exception")
            .description("当流文件因无法设置状态而失败时，它将在这里路由。 ")
            .build();


    public ZtgxSaveErrorDataAsFileProcessor() {
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_MONITOR);
        relationshipSet.add(REL_DIRTY);
        relationshipSet.add(REL_ERRORDATA);
        relationshipSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            // 将FlowFile 读进字节数组
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });
            final Charset charset = Charset.forName(StandardCharsets.UTF_8.name());
            String data = new String(content, charset);
            JSONObject jsonObject = JSON.parseObject(data);
            int errorCount = jsonObject.getInteger("errorCount");
            int rightCount = jsonObject.getInteger("rightCount");
            Date excuteTime = jsonObject.getDate("excuteTime");
            Date excuteEndTime = jsonObject.getDate("excuteEndTime");
            String exception = jsonObject.getString("exception");
            String policyId = jsonObject.getString("policyId");
            String policyType = jsonObject.getString("policyType");
            String databaseName = jsonObject.getString("databaseName");
            String sourceTableName = jsonObject.getString("sourceTableName");
            String policyName = jsonObject.getString("policyName");
            String destinationTableName = jsonObject.getString("destinationTableName");
            String processorType = jsonObject.getString("processorType");
            String sessionId = jsonObject.getString("sessionId");
            String runNifiId = jsonObject.getString("runNifiId");
            String[] strings = runNifiId.split("NiFi");
            String nifiGroupId = strings[0];
            String mid = jsonObject.getString("id");
            ;
            JSONObject monitorJson = new JSONObject();
            monitorJson.put("errorCount", errorCount);
            monitorJson.put("rightCount", rightCount);
            monitorJson.put("excuteTime", excuteTime);
            monitorJson.put("excuteEndTime", excuteEndTime);
            monitorJson.put("exception", exception);
            monitorJson.put("policyId", policyId);
            monitorJson.put("policyType", policyType);
            monitorJson.put("databaseName", databaseName);
            monitorJson.put("sourceTableName", sourceTableName);
            monitorJson.put("policyName", policyName);
            monitorJson.put("destinationTableName", destinationTableName);
            monitorJson.put("processorType", processorType);
            monitorJson.put("sessionId", sessionId);
            monitorJson.put("runNifiId", runNifiId);
            monitorJson.put("nifiGroupId", nifiGroupId);
            monitorJson.put("id", mid);
            JSONArray dirtyDataJsonArr = new JSONArray();
            JSONArray errorDataInfoJsonArr = new JSONArray();
            JSONArray arr = jsonObject.getJSONArray("dirtyDataList");
            for (int i = 0; i < arr.size(); i++) {
                JSONObject ddjo = new JSONObject();
                JSONObject jo = arr.getJSONObject(i);
                String ddid = UUID.randomUUID().toString();
                String rowData = jo.getString("rowData");
                String sId = jo.getString("sessionId");
                String datapri = jo.getString("datapri");
                ddjo.put("id", ddid);
                ddjo.put("roData", rowData);
                ddjo.put("sessionId", sId);
                ddjo.put("datapri", datapri);
                ddjo.put("monitorId", mid);
                dirtyDataJsonArr.add(ddjo);
                JSONArray arr1 = jo.getJSONArray("errorDataList");
                for (int j = 0; j < arr1.size(); j++) {
                    JSONObject jo1 = arr1.getJSONObject(j);
                    JSONObject edijo = new JSONObject();
                    String columnName = jo1.getString("columnName");
                    String columnValue = jo1.getString("columnValue");
                    String description = jo1.getString("description");
                    String errorType = jo1.getString("errorType");
                    String errorGrade = jo1.getString("errorGrade");
                    Date executeTIme = jo1.getDate("executeTIme");
                    edijo.put("id", UUID.randomUUID().toString());
                    edijo.put("columnName", columnName);
                    edijo.put("columnValue", columnValue);
                    edijo.put("description", description);
                    edijo.put("errorType", errorType);
                    edijo.put("errorGrade", errorGrade);
                    edijo.put("executeTime", executeTIme);
                    edijo.put("dirtyDataId", ddid);
                    errorDataInfoJsonArr.add(edijo);
                }

            }

            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    out.write(JSONArray.toJSONString(monitorJson, SerializerFeature.WriteMapNullValue)
                            .getBytes(StandardCharsets.UTF_8));
                }
            });
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_MONITOR);

            if (dirtyDataJsonArr.size() > 0) {
                flowFile = session.create();
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException {
                        out.write(JSONArray.toJSONString(dirtyDataJsonArr, SerializerFeature.WriteMapNullValue)
                                .getBytes(StandardCharsets.UTF_8));
                    }
                });
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_DIRTY);
            }

            if (errorDataInfoJsonArr.size() > 0) {
                flowFile = session.create();
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException {
                        out.write(JSONArray.toJSONString(errorDataInfoJsonArr, SerializerFeature.WriteMapNullValue)
                                .getBytes(StandardCharsets.UTF_8));
                    }
                });
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                flowFile = session.putAllAttributes(flowFile, attributes);
                session.transfer(flowFile, REL_ERRORDATA);
            }

        } catch (Exception e) {
            e.printStackTrace();
            session.transfer(flowFile, REL_FAILURE);

        }
    }
}