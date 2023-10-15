package com.ztgx.nifi.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ztgx.nifi.processor.entity.DirtyData;
import com.ztgx.nifi.processor.entity.ErrorDataInfo;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
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
import java.sql.*;
import java.util.*;
import java.util.Date;

@SideEffectFree
@Tags({"回写源表的正确数据", "数据质量检测"})
@CapabilityDescription("回写源表的正确数据")
public class ZtgxModifiedRightSourceProcessor extends AbstractProcessor {

    private final Set<Relationship> relationships;
    protected List<PropertyDescriptor> propDescriptors;
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("所有已成功处理的Monitor流程文件都在这里进行了路由。")
            .name("成功")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().description("所有失败数据在此路由").name("失败").build();

    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    public static final PropertyDescriptor SOURCE_TABLE_NAME = new PropertyDescriptor.Builder()
            .name("SOURCE_TABLE_NAME")
            .displayName("数据源表名称")
            .description("数据源表名称.")
            .defaultValue("ztgx_qyjbxx")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor SOURCE_TABLE_PRI_COL = new PropertyDescriptor.Builder()
            .name("SOURCE_TABLE_PRI_COL")
            .displayName("表主键或唯一标识字段")
            .description("表主键或唯一标识字段.")
            .defaultValue("ID")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor SOURCE_TABLE_STATUS_COL = new PropertyDescriptor.Builder()
            .name("SOURCE_TABLE_STATUS_COL")
            .displayName("数据源表状态字段名称")
            .description("数据源表状态字段名称.")
            .defaultValue("status")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor SOURCE_TABLE_STATUS_RIGHT_VALUE = new PropertyDescriptor.Builder()
            .name("SOURCE_TABLE_STATUS_RIGHT_VALUE")
            .displayName("正确状态值")
            .description("正确状态值.")
            .defaultValue("1")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    public static final PropertyDescriptor SOURCE_TABLE_UPDATETIME_COL = new PropertyDescriptor.Builder()
            .name("SOURCE_TABLE_UPDATETIME_COL")
            .displayName("数据源表更新时间字段名称")
            .description("数据源表更新时间字段名称.")
            .defaultValue("update_time")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .displayName("数据源")
            .description("The Controller Service that is used to obtain connection to database").required(true)
            .identifiesControllerService(DBCPService.class).build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    public ZtgxModifiedRightSourceProcessor() {
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationshipSet);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SERVICE);
        pds.add(SOURCE_TABLE_NAME);
        pds.add(SOURCE_TABLE_UPDATETIME_COL);
        pds.add(SOURCE_TABLE_STATUS_RIGHT_VALUE);
        pds.add(SOURCE_TABLE_STATUS_COL);
        pds.add(SOURCE_TABLE_PRI_COL);
        propDescriptors = Collections.unmodifiableList(pds);
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
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

            if (!data.startsWith("[")) {
                data = "[" + data + "]";
            }
            getLogger().info("rightdata:"+data);
            JSONArray extractData = JSON.parseArray(data);
            modifiedSource(context, extractData);
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    out.write(JSONArray.toJSONString(extractData, SerializerFeature.WriteMapNullValue)
                            .getBytes(StandardCharsets.UTF_8));
                }
            });
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error(e.getMessage());
            e.printStackTrace();
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    private void modifiedSource(ProcessContext context, JSONArray extractData) {
        Connection conn = null;
        PreparedStatement stmt = null;
        Statement stmts = null;
        ResultSet rs = null;
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        String tableName = context.getProperty("SOURCE_TABLE_NAME").getValue();
        String priCol = context.getProperty("SOURCE_TABLE_PRI_COL").getValue();
        String statusCol = context.getProperty("SOURCE_TABLE_STATUS_COL").getValue();
        String updateTimeCol = context.getProperty("SOURCE_TABLE_UPDATETIME_COL").getValue();
        String rightValue = context.getProperty("SOURCE_TABLE_STATUS_RIGHT_VALUE").getValue();
        String sql = "update " + tableName + " set " + statusCol + " =? ," + updateTimeCol + " =?  where " + priCol + " =?";

        try {
            conn = dbcpService.getConnection();
            stmt = conn.prepareStatement(sql);
            final int batchSize = 1000;
            int count = 0;
            for (int i = 0; i < extractData.size(); i++) {

                String priValue = extractData.getJSONObject(i).getString(priCol);
                getLogger().info("rightsql:"+sql);
                getLogger().info("praram:"+rightValue+"......"+priValue);
                stmt.setString(1, rightValue);
                stmt.setTimestamp(2, new java.sql.Timestamp(new Date().getTime()));
                stmt.setString(3, priValue);
                stmt.addBatch();
                if (++count % batchSize == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();

        } catch (SQLException throwables) {
            getLogger().error(throwables.getMessage());
            throwables.printStackTrace();
        } finally {
            closeConn(conn, stmt, stmts, rs);
        }

    }

    private void closeConn(Connection conn, PreparedStatement stmt, Statement stmts, ResultSet rs) {
        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (null != stmts) {
            try {
                stmts.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
