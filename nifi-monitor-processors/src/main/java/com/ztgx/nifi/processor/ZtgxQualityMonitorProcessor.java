package com.ztgx.nifi.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ztgx.nifi.processor.entity.DirtyData;
import com.ztgx.nifi.processor.entity.ErrorDataInfo;
import com.ztgx.nifi.processor.entity.MonitorInfo;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

@SideEffectFree
@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "monitor", "监控", "数据质量检测" })
@CapabilityDescription("监控数据")
public class ZtgxQualityMonitorProcessor extends AbstractProcessor {
    private final Set<Relationship> relationships;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("所有已成功处理的流程文件都在这里进行了路由。")
            .name("error")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("exception")
            .description("当流文件因无法设置状态而失败时，它将在这里路由。 ")
            .build();
    public static final Relationship RIGHT_SUCCESS = new Relationship.Builder()
            .description("所有已成功处理的流程文件都在这里进行了路由。")
            .name("right")
            .build();


    public  ZtgxQualityMonitorProcessor(){
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILURE);
        relationshipSet.add(RIGHT_SUCCESS);
        relationships = Collections.unmodifiableSet(relationshipSet);
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException{
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try{
            Date excuteTime = new Date();
            // 将FlowFile 读进字节数组
            final byte[] content = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    StreamUtils.fillBuffer(in, content, true);
                }
            });
            // 转String
            final Charset charset = Charset.forName(StandardCharsets.UTF_8.name());
            String data = new String(content, charset);
            // 如何只有一条数据, 转换成数组
            if (!data.startsWith("[")) {
                data = "[" + data + "]";
            }
            // 抽出来的数据
            JSONArray extractData = JSON.parseArray(data);
            // 错误的结果
            JSONArray errorData = new JSONArray();
            JSONArray rightData = new JSONArray();
            String sessionId = UUID.randomUUID().toString();
            MonitorInfo monitorInfo = new  MonitorInfo();
            List<DirtyData> dirtyDataList = new ArrayList<DirtyData>();
            int rightCount = 0;
            for (int i = 0 ;i<extractData.size();i++){
                JSONObject jsonObject  = extractData.getJSONObject(i);
                //数据校验
                String tyshxydm = jsonObject.getString("TYSHXYDM");
                if(tyshxydm==null||"".equals(tyshxydm)){
                    DirtyData dd = new DirtyData();
                    dd.setDatapri(jsonObject.getString("ID"));
                    List<ErrorDataInfo> errorDataList = new ArrayList<>();
                    ErrorDataInfo edi = new ErrorDataInfo();
                    edi.setColumnName("TYSHXYDM");
                    edi.setColumnValue(tyshxydm);
                    edi.setDescription("统一社会信用代码不能为空！");
                    edi.setExecuteTIme(new Date());
                    errorDataList.add(edi);
                    dd.setErrorDataList(errorDataList);
                    dd.setRowData(jsonObject.toString());
                    dirtyDataList.add(dd);

                }else{
                    rightCount+=1;
                    rightData.add(jsonObject);
                }
            }
            monitorInfo.setDirtyDataList(dirtyDataList);
            getLogger().info(".............................................dirtyDataList:"+dirtyDataList.size());
            getLogger().info(".............................................rightData:"+rightData.size());
            monitorInfo.setRightCount(rightCount);
            monitorInfo.setErrorCount(dirtyDataList.size());
            monitorInfo.setSessionId(sessionId);
            monitorInfo.setDestinationTableName("quality_monitor");
            monitorInfo.setDatabaseName("127.0.0.1:3306/error");
            monitorInfo.setExcuteTime(excuteTime);
            monitorInfo.setExcuteEndTime(new Date());
            monitorInfo.setSourceTableName("ztgx_qyjbxx");
            monitorInfo.setRunNifiId("质量管理005");

            boolean isRight = !rightData.isEmpty()&&rightData.size()>0;
            boolean isError = !monitorInfo.getDirtyDataList().isEmpty()&&monitorInfo.getDirtyDataList().size()>0;

            if (isRight){
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException
                    {
                        out.write(JSONArray.toJSONString(rightData, SerializerFeature.WriteMapNullValue)
                                .getBytes(StandardCharsets.UTF_8));
                    }
                });
                Map<String, String> attributes = new HashMap<String, String>();
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                flowFile = session.putAllAttributes(flowFile,attributes);
                session.transfer(flowFile, RIGHT_SUCCESS);

            }else{
                session.remove(flowFile);
            }
            if (isError){
                flowFile = session.create();
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException
                    {
                        out.write(JSONArray.toJSONString(monitorInfo, SerializerFeature.WriteMapNullValue)
                                .getBytes(StandardCharsets.UTF_8));
                    }
                });
                Map<String, String> attributes = new HashMap<String, String>();
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                flowFile = session.putAllAttributes(flowFile,attributes);
                session.transfer(flowFile, REL_SUCCESS);
            }
        }catch (Exception e){
            e.printStackTrace();

        }



    }

    private void transferFlowFile(ProcessSession session, Object monitorInfo, Map<String, String> attributes,
                                  Relationship relationShip)
    {
        getLogger().info("监控数据：" + JSONArray.toJSONString(monitorInfo, SerializerFeature.WriteMapNullValue));
        FlowFile flowFile = session.create();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        getLogger().info(".............................................3");
        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream in, OutputStream out) throws IOException
            {
                out.write(JSONArray.toJSONString(monitorInfo, SerializerFeature.WriteMapNullValue)
                        .getBytes(StandardCharsets.UTF_8));
                getLogger().info(".............................................4");
            }
        });
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, relationShip);
        getLogger().info("............................................5");
    }


    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }


}
