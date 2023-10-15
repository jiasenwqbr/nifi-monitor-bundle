package com.ztgx.nifi.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ztgx.nifi.processor.entity.DirtyData;
import com.ztgx.nifi.processor.entity.ErrorDataInfo;
import com.ztgx.nifi.processor.entity.MonitorInfo;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.Date;

@SideEffectFree
@Tags({"错误数据批次保存至关系型数据库", "错误数据保存", "数据质量检测", "中投国信"})
@CapabilityDescription("错误数据批次保存至关系型数据库")
public class ZtgxErrorDataBatchSaveToDBProcessor extends AbstractProcessor {

    private final Set<Relationship> relationships;
    private final List<PropertyDescriptor> propDescriptors;
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("保存成功。")
            .name("保存成功")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("异常")
            .description("当流文件因无法设置状态而失败时，它将在这里路由。 ")
            .build();
    static final PropertyDescriptor DEST_SERVICE = new PropertyDescriptor.Builder()
            .name("DEST_SERVICE")
            .description("目标数据源").required(true)
            .identifiesControllerService(DBCPService.class).build();

    public ZtgxErrorDataBatchSaveToDBProcessor() {
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationshipSet);
        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DEST_SERVICE);
        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
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
        MonitorInfo monitorInfo = new MonitorInfo();
        String mid = jsonObject.getString("id");
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
        monitorInfo.setId(mid);
        monitorInfo.setRightCount(rightCount);
        monitorInfo.setErrorCount(errorCount);
        monitorInfo.setExcuteTime(excuteTime);
        monitorInfo.setExcuteEndTime(excuteEndTime);
        monitorInfo.setException(exception);
        monitorInfo.setPolicyId(policyId);
        monitorInfo.setPolicyName(policyName);
        monitorInfo.setPolicyType(policyType);
        monitorInfo.setDatabaseName(databaseName);
        monitorInfo.setSourceTableName(sourceTableName);
        monitorInfo.setPolicyName(policyName);
        monitorInfo.setDestinationTableName(destinationTableName);
        monitorInfo.setProcessorType(processorType);
        monitorInfo.setSessionId(sessionId);
        monitorInfo.setRunNifiId(runNifiId);
        monitorInfo.setNifiGroupId(nifiGroupId);

        List<DirtyData> dirtyDataList = new ArrayList<>();
        JSONArray arr = jsonObject.getJSONArray("dirtyDataList");
        for (int i = 0; i < arr.size(); i++) {
            DirtyData dd = new DirtyData();
            JSONObject jo = arr.getJSONObject(i);
            String id = UUID.randomUUID().toString();
            String rowData = jo.getString("rowData");
            String sId = jo.getString("sessionId");
            String datapri = jo.getString("datapri");
            dd.setDatapri(datapri);
            dd.setId(id);
            dd.setRowData(rowData);
            dd.setSessionId(sId);

            List<ErrorDataInfo> errorDataList = new ArrayList<>();
            JSONArray arr1 = jo.getJSONArray("errorDataList");
            for (int j = 0; j < arr1.size(); j++) {
                JSONObject jo1 = arr1.getJSONObject(j);
                ErrorDataInfo edi = new ErrorDataInfo();
                String columnName = jo1.getString("columnName");
                String columnValue = jo1.getString("columnValue");
                String description = jo1.getString("description");
                String errorType = jo1.getString("errorType");
                String errorGrade = jo1.getString("errorGrade");
                Date executeTIme = jo1.getDate("executeTIme");
                edi.setDescription(description);
                edi.setColumnName(columnName);
                edi.setColumnValue(columnValue);
                edi.setErrorGrade(errorGrade);
                edi.setErrorType(errorType);
                edi.setExecuteTIme(executeTIme);
                edi.setId(UUID.randomUUID().toString());
                errorDataList.add(edi);
            }
            dd.setErrorDataList(errorDataList);
            dirtyDataList.add(dd);

        }
        monitorInfo.setDirtyDataList(dirtyDataList);
        monitorInfo.setId(mid);

        try {
            saveMonitor(monitorInfo, context);
            saveDirtyDataList(monitorInfo, context);
            saveErrorDataList(monitorInfo, context);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            session.transfer(flowFile, REL_FAILURE);
        }

    }

    private void saveMonitor(MonitorInfo monitorInfo, ProcessContext context) {
        Connection conn = null;
        PreparedStatement stmt = null;
        Statement stmts = null;
        ResultSet rs = null;
        final DBCPService dbcpService = context.getProperty(DEST_SERVICE).asControllerService(DBCPService.class);
        String sql = "insert into quality_monitor_info(id,error_count,right_count,excute_time,excute_endtime," +
                "exception,policyId,policy_type,database_name,source_table_name," +
                "policy_name,destination_tablename,processor_type,session_id,run_nifi_id,nifi_group_id) " +
                " values(?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?,?," +
                "?)";
        try {
            conn = dbcpService.getConnection();
            //conn.setAutoCommit(false);
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, monitorInfo.getId());
            stmt.setInt(2, monitorInfo.getErrorCount());
            stmt.setInt(3, monitorInfo.getRightCount());
            stmt.setTimestamp(4, new java.sql.Timestamp(monitorInfo.getExcuteTime().getTime()));
            stmt.setTimestamp(5, new java.sql.Timestamp(monitorInfo.getExcuteEndTime().getTime()));
            stmt.setString(6, monitorInfo.getException());
            stmt.setString(7, monitorInfo.getPolicyId());
            stmt.setString(8, monitorInfo.getPolicyType());
            stmt.setString(9, monitorInfo.getDatabaseName());
            stmt.setString(10, monitorInfo.getSourceTableName());
            stmt.setString(11, monitorInfo.getPolicyName());
            stmt.setString(12, monitorInfo.getDestinationTableName());
            stmt.setString(13, monitorInfo.getProcessorType());
            stmt.setString(14, monitorInfo.getSessionId());
            stmt.setString(15, monitorInfo.getRunNifiId());
            stmt.setString(16, monitorInfo.getNifiGroupId());
            stmt.executeUpdate();
            //conn.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            closeConn(conn, stmt, stmts, rs);
        }
    }

    private void saveDirtyDataList(MonitorInfo monitorInfo, ProcessContext context) {
        Connection conn = null;
        PreparedStatement stmt = null;
        Statement stmts = null;
        ResultSet rs = null;
        final DBCPService dbcpService = context.getProperty(DEST_SERVICE).asControllerService(DBCPService.class);
        String sql = "insert into quality_dirty_data(id,row_data,session_id,data_pri,monitor_id,error_count) values (?,?,?,?,?,?)";
        String mid = monitorInfo.getId();
        try {
            conn = dbcpService.getConnection();
            stmt = conn.prepareStatement(sql);
            final int batchSize = 1000;
            int count = 0;
            for (DirtyData dd : monitorInfo.getDirtyDataList()) {
                stmt.setString(1, dd.getId());
                stmt.setString(2, dd.getRowData());
                stmt.setString(3, dd.getSessionId());
                stmt.setString(4, dd.getDatapri());
                stmt.setString(5, mid);
                stmt.setInt(6, dd.getErrorDataList().size());
                stmt.addBatch();
                if (++count % batchSize == 0) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            closeConn(conn, stmt, stmts, rs);
        }
    }

    private void saveErrorDataList(MonitorInfo monitorInfo, ProcessContext context) {
        Connection conn = null;
        PreparedStatement stmt = null;
        Statement stmts = null;
        ResultSet rs = null;
        final DBCPService dbcpService = context.getProperty(DEST_SERVICE).asControllerService(DBCPService.class);
        String sql = "insert into quality_error_data_info(id,column_name,description,execute_time,dirty_data_id,error_type,error_grade) values (?,?,?,?,?,?,?) ";

        try {
            conn = dbcpService.getConnection();
            stmt = conn.prepareStatement(sql);
            final int batchSize = 1000;
            int count = 0;
            for (DirtyData dd : monitorInfo.getDirtyDataList()) {
                List<ErrorDataInfo> edlist = dd.getErrorDataList();
                for (ErrorDataInfo edi : edlist) {
                    stmt.setString(1, edi.getId());
                    stmt.setString(2, edi.getColumnName());
                    stmt.setString(3, edi.getDescription());
                    stmt.setTimestamp(4, new java.sql.Timestamp(edi.getExecuteTIme().getTime()));
                    //stmt.set
                    stmt.setString(5, dd.getId());
                    stmt.setString(6, edi.getErrorType());
                    stmt.setString(7, edi.getErrorGrade());
                    stmt.addBatch();
                    if (++count % batchSize == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
            }
            stmt.executeBatch();
        } catch (SQLException throwables) {
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
