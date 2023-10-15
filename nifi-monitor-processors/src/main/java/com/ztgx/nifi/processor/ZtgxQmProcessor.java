package com.ztgx.nifi.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ztgx.nifi.processor.entity.CTResponse;
import com.ztgx.nifi.processor.entity.DirtyData;
import com.ztgx.nifi.processor.entity.ErrorDataInfo;
import com.ztgx.nifi.processor.entity.MonitorInfo;
import com.ztgx.nifi.util.MultiThread;
import com.ztgx.nifi.util.QualidateValidate;
import com.ztgx.nifi.util.RuleUtil;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Tags({"monitor", "监控", "数据质量检测"})
@CapabilityDescription("监控数据")
public class ZtgxQmProcessor extends AbstractProcessor {
    private final Set<Relationship> relationships;
    protected List<PropertyDescriptor> propDescriptors;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("所有已成功处理的流程文件都在这里进行了路由。")
            .name("errorData")
            .build();
    public static final Relationship REL_RIGHT = new Relationship.Builder()
            .description("所有正确的数据都在这里进行了路由。")
            .name("rightData")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("exception")
            .description("当流文件因无法设置状态而失败时，它将在这里路由。 ")
            .build();
    public static final PropertyDescriptor RUN_NIFI_ID = new PropertyDescriptor.Builder()
            .name("RUN_NIFI_ID")
            .displayName("NIFI运行ID")
            .description("标识任务每次调度的ID.")
            .defaultValue("ZLGL_" + UUID.randomUUID().toString())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor VALIDATE_RULE = new PropertyDescriptor.Builder()
            .name("VALIDATE_RULE")
            .displayName("验证规则")
            .description("验证规则.")
            .defaultValue("[{\"fieldname\":\"TYSHXYDM\",\"fieldnameCN\":\"统一社会信用代码\",\"rule\":\"http://192.168.2.86:9013/rs/rs/v0031\",\"trueStr\":\"统一社会信用代码符合规范\",\"falseStr\":\"统一社会信用代码不符合规范\",\"emptyStr\":\"统一社会信用代码不能为空\",\"parameter\":\"{\\\"parameter1\\\":\\\"{TYSHXYDM}\\\"}\"}]\n")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor VALIDATE_MULI_RULE = new PropertyDescriptor.Builder()
            .name("VALIDATE_MULI_RULE")
            .displayName("多字段验证规则")
            .description("多字段验证规则")
            .defaultValue("[]")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor SOURCE_DATA_TYPE_MAP = new PropertyDescriptor.Builder()
            .name("SOURCE_DATA_TYPE_MAP")
            .displayName("源数据字段类型")
            .description("源数据字段类型")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("{}")
            .required(true)
            .build();
    public static final PropertyDescriptor SOURCE_DATA_IS_NOT_BLANK_MAP = new PropertyDescriptor.Builder()
            .name("SOURCE_DATA_IS_NOT_BLANK_MAP")
            .displayName("源数据不可为空字段")
            .description("源数据不可为空字段")
            .defaultValue("{}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();


    public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("DATABASE_NAME")
            .displayName("数据源名称")
            .description("数据源名称.")
            .defaultValue("127.0.0.1:3306/error")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor SOURCE_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("SOURCE_TABLE_NAME")
            .displayName("数据源表名称")
            .description("数据源表名称.")
            .defaultValue("ztgx_qyjbxx")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor DESTINATION_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("DESTINATION_TABLE_NAME")
            .displayName("目标表名称")
            .description("目标表名称.")
            .defaultValue("quality_monitor")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor TABLE_PRI = new PropertyDescriptor.Builder()
            .name("TABLE_PRI")
            .displayName("表主键或唯一标识字段")
            .description("目标表名称.")
            .defaultValue("ID")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor THREAD_NUM = new PropertyDescriptor.Builder()
            .name("THREAD_NUM")
            .displayName("线程数")
            .description("线程数")
            .defaultValue("10")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();


    public ZtgxQmProcessor() {
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_RIGHT);
        relationshipSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationshipSet);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(RUN_NIFI_ID);
        pds.add(VALIDATE_RULE);
        pds.add(DATABASE_NAME);
        pds.add(SOURCE_TABLE_NAME);
        pds.add(DESTINATION_TABLE_NAME);
        pds.add(TABLE_PRI);
        pds.add(VALIDATE_MULI_RULE);
        pds.add(SOURCE_DATA_TYPE_MAP);
        pds.add(SOURCE_DATA_IS_NOT_BLANK_MAP);
        pds.add(THREAD_NUM);

        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {


        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
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
            JSONArray extractData = JSON.parseArray(data);

            getLogger().info("extractData.size():"+extractData.size());

            String sessionId = UUID.randomUUID().toString();
            String runNifiId = context.getProperty("RUN_NIFI_ID").getValue();

            JSONArray rightData = new JSONArray();
            MonitorInfo monitorInfo = new MonitorInfo();

            int rightCount = 0;
            String tablePri = context.getProperty("TABLE_PRI").getValue();
            String threadNum = context.getProperty("THREAD_NUM").getValue();
            int num = 10;//默认线程数
            if (threadNum!=null&&!"".equals(threadNum)&&!"0".equals(threadNum)){
                num = Integer.valueOf(threadNum);
            }
            int length = extractData.size();
            // 启动多线程
            if (num > length) {
                num = length;
            }
            int baseNum = length / num;
            int remainderNum = length % num;
            int end = 0;
            List<List<JSONObject>> jsonObjects = new ArrayList<>();
            for (int i=0;i<num;i++){
                List<JSONObject> jsonObjects1 = new ArrayList<>();
                int start = end;
                end = start + baseNum;
                if (i == (num - 1)) {
                    end = length;
                } else if (i < remainderNum) {
                    end = end + 1;
                }
                List<Object> list =  extractData.subList(start,end);
                for (Object obj : list){
                    jsonObjects1.add((JSONObject)obj);
                }
                jsonObjects.add(jsonObjects1);
            }

            String validateRuleStr = context.getProperty("VALIDATE_RULE").getValue();
            String validateMuliRuleStr = context.getProperty("VALIDATE_MULI_RULE").getValue();

            MultiThread<JSONObject,DirtyData> multiThread = new MultiThread<JSONObject, DirtyData>(jsonObjects) {

                @Override
                public List<DirtyData> outExecute(int currentThread, List<JSONObject> data) {
                    List<DirtyData> dirtyDataList1 = new ArrayList<DirtyData>();
                    for (JSONObject jsonObject : data){
                        DirtyData dd = QualidateValidate.validateRow(validateRuleStr, validateMuliRuleStr,jsonObject);
                        dd.setDatapri(jsonObject.getString(tablePri));
                        if(jsonObject.getString(tablePri)==null){
                            dd.setDatapri(jsonObject.getString(tablePri.toLowerCase()));
                        }
                        if (dd.getErrorDataList().size() > 0) {
                            dd.setSessionId(sessionId);
                            dirtyDataList1.add(dd);
                        }
                    }
                    return dirtyDataList1;
                }
            };
            List<DirtyData> dirtyDataList = multiThread.getResult();
            getLogger().info(".............................................rightData:" + (extractData.size()-dirtyDataList.size()));
            getLogger().info(".............................................dirtyDataList:" + dirtyDataList.size());
            monitorInfo.setDirtyDataList(dirtyDataList);
            monitorInfo.setRightCount(extractData.size()-dirtyDataList.size());
            monitorInfo.setErrorCount(dirtyDataList.size());
            monitorInfo.setSessionId(sessionId);
            String databaseName = context.getProperty("DATABASE_NAME").getValue();
            String sourceTableName = context.getProperty("SOURCE_TABLE_NAME").getValue();
            String dtn = context.getProperty("DESTINATION_TABLE_NAME").getValue();
            monitorInfo.setDestinationTableName(dtn);
            monitorInfo.setDatabaseName(databaseName);
            monitorInfo.setExcuteTime(excuteTime);
            monitorInfo.setExcuteEndTime(new Date());
            monitorInfo.setSourceTableName(sourceTableName);
            monitorInfo.setRunNifiId(runNifiId);
            monitorInfo.setId(sessionId);

            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    out.write(JSONArray.toJSONString(monitorInfo, SerializerFeature.WriteMapNullValue)
                            .getBytes(StandardCharsets.UTF_8));
                }
            });
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);

            flowFile = session.create();
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    out.write(JSONArray.toJSONString(rightData, SerializerFeature.WriteMapNullValue)
                            .getBytes(StandardCharsets.UTF_8));
                }
            });

            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_RIGHT);

        } catch (Exception e) {
            e.printStackTrace();
            session.transfer(flowFile, REL_FAILURE);
        }
    }


}

