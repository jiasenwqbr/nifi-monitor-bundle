package com.ztgx.nifi.processor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
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
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

@SideEffectFree
@Tags({ "中投国信", "错误数据", "数据质量检测" })
@CapabilityDescription("错误数据处理")
public class ZtgxErrorDataHandleProcessor extends AbstractProcessor {
    private  final Set<Relationship> relationships;
    private final List<PropertyDescriptor> propDescriptors;
    public static final Relationship REL_INSERT = new Relationship.Builder()
            .description("新增的数据在此路由。")
            .name("insert")
            .build();
    public static final Relationship REL_UPDATE = new Relationship.Builder()
            .description("更新的文件都在这里进行了路由。")
            .name("update")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("exception")
            .description("当流文件因无法设置状态而失败时，它将在这里路由。 ")
            .build();
    static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database").required(true)
            .identifiesControllerService(DBCPService.class).build();
    public ZtgxErrorDataHandleProcessor (){
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_INSERT);
        relationshipSet.add(REL_UPDATE);
        relationshipSet.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(relationshipSet);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SERVICE);
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
        try{

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
            JSONObject jsonObject  = JSON.parseObject(data);

            // 抽出来的数据
            JSONObject jo = validateInsertOrUpdate( context, jsonObject, "ZTGX_QYJBXX_ERROR");
            JSONArray updateData = jo.getJSONArray("updateData");
            JSONArray insertData = jo.getJSONArray("insertData");

            getLogger().info("##############################################错误数据updateData："+updateData.size());
            getLogger().info("##############################################错误数据insertData："+insertData.size());
            boolean isupdate = !updateData.isEmpty()&&updateData.size()>0;
            boolean isinsert = !insertData.isEmpty()&&insertData.size()>0;
            if (isupdate){
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException
                    {
                        out.write(JSONArray.toJSONString(updateData, SerializerFeature.WriteMapNullValue)
                                .getBytes(StandardCharsets.UTF_8));
                    }
                });
                Map<String, String> attributes = new HashMap<String, String>();
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                flowFile = session.putAllAttributes(flowFile,attributes);
                session.transfer(flowFile, REL_UPDATE);
            }else{
                session.remove(flowFile);

            }
            if (isinsert){
                flowFile = session.create();
                flowFile = session.write(flowFile, new StreamCallback() {
                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException
                    {
                        out.write(JSONArray.toJSONString(insertData, SerializerFeature.WriteMapNullValue)
                                .getBytes(StandardCharsets.UTF_8));
                    }
                });
                Map<String, String> attributes = new HashMap<String, String>();
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                flowFile = session.putAllAttributes(flowFile,attributes);
                session.transfer(flowFile, REL_INSERT);
            }

        }catch (Exception e) {
            session.rollback();
            session.transfer(flowFile, REL_FAILURE);
            e.printStackTrace();
        }

    }


    private boolean isExit(String id, ProcessContext context, String tablename) {
        boolean flag = false;
        Connection conn = null;
        PreparedStatement stmt = null;
        Statement stmts = null;
        ResultSet rs = null;
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

        try {
            conn = dbcpService.getConnection();
            stmts = conn.createStatement();
            String sql = "select * from "+tablename+" where id='"+id+"' ";
            rs = stmts.executeQuery(sql);
            while (rs.next()){
                getLogger().info("..............错误ID："+rs.getString("ID")+"存在");
                flag = true;
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
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
            }if (null != stmts) {
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
        return flag;
    }

    private JSONObject validateInsertOrUpdate(ProcessContext context,JSONObject jsonObject,String tablename){
        Connection conn = null;
        PreparedStatement stmt = null;
        Statement stmts = null;
        ResultSet rs = null;
        JSONObject jo = new JSONObject();
        JSONArray updateData = new JSONArray();
        JSONArray insertData = new JSONArray();
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        JSONArray arr = jsonObject.getJSONArray("dirtyDataList");
        try {
        conn = dbcpService.getConnection();
        for (int j = 0;j<arr.size();j++){
            JSONObject jsonObject1  = arr.getJSONObject(j);
            String id = jsonObject1.getString("id");
            String errors = jsonObject1.getJSONArray("errorDataList").toString();
            JSONObject jsonObjectNew = new JSONObject();
            jsonObjectNew.put("ID",id);
            jsonObjectNew.put("ERRORS",errors);
            boolean falg = false;
            //查询id是否存在
            stmts = conn.createStatement();
            String sql = "select * from "+tablename+" where id='"+id+"' ";
            rs = stmts.executeQuery(sql);
            while (rs.next()){
                falg = true;
            }
            if (falg){
                updateData.add(jsonObjectNew);
            }else{
                insertData.add(jsonObjectNew);
            }
        }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }finally {
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
            }if (null != stmts) {
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
        jo.put("updateData",updateData);
        jo.put("insertData",insertData);

        return jo;
    }



}
