package com.zoho.sde.app.controller;

import java.sql.DriverManager;
import java.sql.ResultSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.WildcardQuery;
import javax.servlet.http.HttpServletRequest;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;

import org.apache.lucene.document.Field;


import org.apache.lucene.document.TextField; 
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.FSDirectory;
import org.json.JSONArray;
import org.json.JSONObject;
import org.quartz.JobDataMap;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.healthmarketscience.sqlbuilder.BinaryCondition;
import com.healthmarketscience.sqlbuilder.CustomSql;
import com.healthmarketscience.sqlbuilder.OrderObject;
import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.UnaryCondition;
import com.healthmarketscience.sqlbuilder.custom.mysql.MysLimitClause;
import com.weddini.throttling.Throttling;
import com.weddini.throttling.ThrottlingType;
import com.zoho.sde.app.constants.SDEAppConstants;
import com.zoho.sde.app.constants.TicketTransistion;
import com.zoho.sde.app.dataTables.CRMBUILDUPDATEINFO;

import com.zoho.sde.app.dataTables.STOREUPDATETEDTICKETS;
import com.zoho.sde.app.services.SchedulerService;
import com.zoho.sde.app.utils.DataBaseConnectorUtil;

//--------------------------------------------------------//
import com.zoho.sde.app.utils.TicketsSchedulerInfoUtil;
import com.zoho.sde.app.utils.PropertiesUtil;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;

import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.DirectoryReader;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import org.apache.lucene.search.TopDocs;

import org.apache.lucene.store.Directory;


import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

import java.nio.file.Paths;

import java.io.File;
import java.io.IOException;


@RestController
@RequestMapping({"/", "/api/"})
public class TicketsSchedulerInfoController {
    private static final Logger LOGGER = Logger.getLogger(TicketsSchedulerInfoController.class.getName());

    @RequestMapping(value = "/ticket/scheduler/info", method = RequestMethod.GET, produces = "application/json;charset=UTF-8") //No I18N
    @Throttling(type = ThrottlingType.RemoteAddr, limit = 5, timeUnit = TimeUnit.SECONDS)
    public String fetchschedulerDetails(HttpServletRequest request, AbstractAuthenticationToken authentication) {
            try {
                JSONArray schedulerDetailArray = TicketsSchedulerInfoUtil.getallschedulardetails();
                if (schedulerDetailArray != null) {
                      return schedulerDetailArray.toString();
                } else {
                    LOGGER.log(Level.SEVERE, "Failed to retrieve scheduler details.");
                    return new JSONObject().put("error", "Failed to retrieve scheduler details.").toString();
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error in fetchschedulerDetails", e);
                return new JSONObject().put("error", "An error occurred while processing the request.").toString();
            }
       
    }

    @RequestMapping(value = "mi/pagination/dc/build", method = RequestMethod.GET, produces = "application/json;charset=UTF-8")
    @Throttling(type = ThrottlingType.RemoteAddr, limit = 5, timeUnit = TimeUnit.SECONDS)
    public String buildDetailsBasedOnDcVsPageCritieria(
        HttpServletRequest request, 
        AbstractAuthenticationToken authentication
    ) {
        try {
            // Retrieve the 'action' parameter
            String action = request.getParameter("action");
            
            // Check if 'action' equals "Dc" and no other parameters are present
            if (request.getParameterMap().size() == 1) {
                SelectQuery sq = new SelectQuery().addAllColumns();
                sq.addCustomJoin(SelectQuery.JoinType.INNER, 
                        new CustomSql(STOREUPDATETEDTICKETS.TABLE), 
                        new CustomSql(CRMBUILDUPDATEINFO.TABLE), 
                        BinaryCondition.equalTo(new CustomSql(STOREUPDATETEDTICKETS.TABLE + "." + STOREUPDATETEDTICKETS.BUILDID), 
                        new CustomSql(CRMBUILDUPDATEINFO.TABLE + "." + CRMBUILDUPDATEINFO.BUILDID)));
                sq.addCondition(UnaryCondition.isNotNull(new CustomSql(STOREUPDATETEDTICKETS.SCHEDULERSTATUS)));
                if (!"ALL".equals(action)) {
                sq.addCondition(BinaryCondition.equalTo(new CustomSql(STOREUPDATETEDTICKETS.DC),action ));
                }
                sq.addCustomOrdering(STOREUPDATETEDTICKETS.SCHEDULERID, OrderObject.Dir.DESCENDING);
                sq.addCustomization(new MysLimitClause(0, 24));
                List<Map<String, Object>> particularsSchedulerInfo = DataBaseConnectorUtil.getResultSetAsListFromSelectQuery(sq.toString(), new Object[]{});
                JSONArray schedulerDetailArray = TicketsSchedulerInfoUtil.buildDetailsBasedOnDcVsPageCritieria(particularsSchedulerInfo);
                return schedulerDetailArray.toString();
            } 
            
            // Check for the presence of three specific parameters
            String from = request.getParameter("from");
            String to = request.getParameter("to");
            int fromInt = Integer.parseInt(from);
            int toInt = Integer.parseInt(to);
            String currentdcselected = request.getParameter("currentdcselected");
            if (from != null && to != null && currentdcselected != null) {
            SelectQuery sq = new SelectQuery().addAllColumns();
            sq.addCustomJoin(SelectQuery.JoinType.INNER, 
                        new CustomSql(STOREUPDATETEDTICKETS.TABLE), 
                        new CustomSql(CRMBUILDUPDATEINFO.TABLE), 
                        BinaryCondition.equalTo(new CustomSql(STOREUPDATETEDTICKETS.TABLE + "." + STOREUPDATETEDTICKETS.BUILDID), 
                        new CustomSql(CRMBUILDUPDATEINFO.TABLE + "." + CRMBUILDUPDATEINFO.BUILDID)));
            if (!"ALL".equals(currentdcselected)) {           
            sq.addCondition(BinaryCondition.equalTo(new CustomSql(STOREUPDATETEDTICKETS.DC), currentdcselected ));
            }
            sq.addCondition(UnaryCondition.isNotNull(new CustomSql(STOREUPDATETEDTICKETS.SCHEDULERSTATUS)));
            sq.addCustomOrdering(STOREUPDATETEDTICKETS.SCHEDULERID, OrderObject.Dir.DESCENDING);
            sq.addCustomization(new MysLimitClause(fromInt, 24));
            List<Map<String, Object>> particularsSchedulerInfo = DataBaseConnectorUtil.getResultSetAsListFromSelectQuery(sq.toString(), new Object[]{});  
                JSONArray anotherDetailArray = TicketsSchedulerInfoUtil.buildDetailsBasedOnDcVsPageCritieria(particularsSchedulerInfo);
                return anotherDetailArray.toString();
            }
            
            // Handle invalid cases
            return "Invalid parameters";
            
        } catch (Exception e) {
          
            LOGGER.log(Level.SEVERE, "Error processing scheduler details", e);//No I18N
            return"error occured";
            
        }
    }

    @RequestMapping(value = "/ticket/scheduler/specificBuild", method = RequestMethod.GET, produces = "application/json;charset=UTF-8") //No I18N
    @Throttling(type = ThrottlingType.RemoteAddr, limit = 5, timeUnit = TimeUnit.SECONDS)
    public String fetchSchedulerDetailsOnSpecificBuild(HttpServletRequest request, AbstractAuthenticationToken authentication) {
        String buildId = request.getParameter("buildId"); //No I18N

        SelectQuery sq = new SelectQuery().addAllColumns();
        sq.addCustomJoin(SelectQuery.JoinType.INNER, new CustomSql(STOREUPDATETEDTICKETS.TABLE), new CustomSql(CRMBUILDUPDATEINFO.TABLE),
                BinaryCondition.equalTo(new CustomSql(STOREUPDATETEDTICKETS.TABLE + "." + STOREUPDATETEDTICKETS.BUILDID),
                        new CustomSql(CRMBUILDUPDATEINFO.TABLE + "." + CRMBUILDUPDATEINFO.BUILDID)));
        sq.addCondition(BinaryCondition.equalTo(new CustomSql(CRMBUILDUPDATEINFO.TABLE + "." + CRMBUILDUPDATEINFO.BUILDID),
                SDEAppConstants.PREPARED_STATEMENT_QUESTION_MARK));

        List<Map<String, Object>> particularsSchedulerInfo = DataBaseConnectorUtil.getResultSetAsListFromSelectQuery(sq.toString(), new Object[]{buildId});
       
        if (particularsSchedulerInfo != null && !particularsSchedulerInfo.isEmpty()) {
            JSONArray schedulerDetailArray = new JSONArray();
            for (Map<String, Object> tempHashMap : particularsSchedulerInfo) {
                try {
                    JSONObject schedulerDetailObj = new JSONObject();
    
                    // CRMBUILDUPDATEINFO details
                    putIfExists(schedulerDetailObj, "BUILDLABEL", tempHashMap.get(CRMBUILDUPDATEINFO.BUILDLABEL)); //No I18N
                    putIfExists(schedulerDetailObj, "BUILDID", tempHashMap.get(CRMBUILDUPDATEINFO.BUILDID));//No I18N
                    putIfExists(schedulerDetailObj, "BUILDTYPE", tempHashMap.get(CRMBUILDUPDATEINFO.BUILDTYPE));//No I18N
                    putIfExists(schedulerDetailObj, "COMMENT", tempHashMap.get(CRMBUILDUPDATEINFO.COMMENT));//No I18N
                    putIfExists(schedulerDetailObj, "BUILDURL", tempHashMap.get(CRMBUILDUPDATEINFO.BUILDURL));//No I18N
                    putIfExists(schedulerDetailObj, "BRANCHNAME", tempHashMap.get(CRMBUILDUPDATEINFO.BRANCHNAME));//No I18N
                    putIfExists(schedulerDetailObj, "CHANGESET", tempHashMap.get(CRMBUILDUPDATEINFO.CHANGESET));//No I18N
                    putIfExists(schedulerDetailObj, "STATUS", tempHashMap.get(CRMBUILDUPDATEINFO.STATUS));//No I18N
                    putIfExists(schedulerDetailObj, "ISINCREAMENTAL", tempHashMap.get(CRMBUILDUPDATEINFO.ISINCREAMENTAL));//No I18N
                    putIfExists(schedulerDetailObj, "EDITION", tempHashMap.get(CRMBUILDUPDATEINFO.EDITION));//No I18N
                    putIfExists(schedulerDetailObj, "STARTTIME", tempHashMap.get(CRMBUILDUPDATEINFO.STARTTIME));//No I18N
                    putIfExists(schedulerDetailObj, "ENDTIME", tempHashMap.get(CRMBUILDUPDATEINFO.ENDTIME));//No I18N
                    putIfExists(schedulerDetailObj, "CREATEDTIME", tempHashMap.get(CRMBUILDUPDATEINFO.CREATEDTIME));//No I18N
                    putIfExists(schedulerDetailObj, "ITERATION", tempHashMap.get(CRMBUILDUPDATEINFO.ITERATION));//No I18N
                    putIfExists(schedulerDetailObj, "SERVICE", tempHashMap.get(CRMBUILDUPDATEINFO.SERVICE));//No I18N
                    putIfExists(schedulerDetailObj, "ISPATCH", tempHashMap.get(CRMBUILDUPDATEINFO.ISPATCH));//No I18N
    
                    // STOREUPDATETEDTICKETS details
                    putIfExists(schedulerDetailObj, "SCHEDULERID", tempHashMap.get(STOREUPDATETEDTICKETS.SCHEDULERID));//No I18N
                    putIfExists(schedulerDetailObj, "DC", tempHashMap.get(STOREUPDATETEDTICKETS.DC));//No I18N
                    putIfExists(schedulerDetailObj, "SCHEDULERSTATUS", tempHashMap.get(STOREUPDATETEDTICKETS.SCHEDULERSTATUS));//No I18N
                    putIfExists(schedulerDetailObj, "TOTALCREATORENTRIES", tempHashMap.get(STOREUPDATETEDTICKETS.TOTALCREATORENTRIES));//No I18N
                    putIfExists(schedulerDetailObj, "SUCCEEDSUPPORTIDS", tempHashMap.get(STOREUPDATETEDTICKETS.SUCCEEDSUPPORTIDS));//No I18N
                    putIfExists(schedulerDetailObj, "CREATORENTRIESONBUILD", tempHashMap.get(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD));//No I18N
    
                    schedulerDetailArray.put(schedulerDetailObj);
                    return schedulerDetailArray.toString();
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error processing scheduler details", e);//No I18N
                }
            }
        } else {
            return "noDataIntheDB";//No I18N
        }
        return  "";//No I18N
    
        
    }
    private void putIfExists(JSONObject obj, String key, Object value) {
        if (value != null) {
            obj.put(key, value.toString());
        } else {
            obj.put(key, "");//No I18N
        }
    }

    @RequestMapping(value = "scheduler/retry/mechanism", method = RequestMethod.GET, produces = "application/json;charset=UTF-8") //No I18N
    @Throttling(type = ThrottlingType.RemoteAddr, limit = 5, timeUnit = TimeUnit.SECONDS)
    public String retryScheduler(HttpServletRequest request, AbstractAuthenticationToken authentication) {
        String buildId = request.getParameter("BuildID"); //No I18N
        
        SelectQuery sq = new SelectQuery().addAllColumns();
        sq.addCustomJoin(SelectQuery.JoinType.INNER, new CustomSql(STOREUPDATETEDTICKETS.TABLE), new CustomSql(CRMBUILDUPDATEINFO.TABLE),
                BinaryCondition.equalTo(new CustomSql(STOREUPDATETEDTICKETS.TABLE + "." + STOREUPDATETEDTICKETS.BUILDID),
                        new CustomSql(CRMBUILDUPDATEINFO.TABLE + "." + CRMBUILDUPDATEINFO.BUILDID)));
        sq.addCondition(BinaryCondition.equalTo(new CustomSql(CRMBUILDUPDATEINFO.TABLE + "." + CRMBUILDUPDATEINFO.BUILDID),
                SDEAppConstants.PREPARED_STATEMENT_QUESTION_MARK));

        List<Map<String, Object>> particularsSchedulerInfo = DataBaseConnectorUtil.getResultSetAsListFromSelectQuery(sq.toString(), new Object[]{buildId});

        JobDataMap jobDataMap = new JobDataMap();

        try {
            JSONArray schedulerDetailArray = new JSONArray();
            if (particularsSchedulerInfo != null && !particularsSchedulerInfo.isEmpty()) {
                HashMap<String, Object> tempHashMap = (HashMap<String, Object>) particularsSchedulerInfo.get(0);

                jobDataMap.put("buildName", getStringValue(tempHashMap.get(CRMBUILDUPDATEINFO.COMMENT)));//No I18N
                jobDataMap.put("buildLabel", getStringValue(tempHashMap.get(CRMBUILDUPDATEINFO.BUILDLABEL)));//No I18N
                jobDataMap.put("comment", getStringValue(tempHashMap.get(CRMBUILDUPDATEINFO.COMMENT)));//No I18N
                jobDataMap.put("buildurl", getStringValue(tempHashMap.get(CRMBUILDUPDATEINFO.BUILDURL)));//No I18N
                jobDataMap.put("changeSet", getStringValue(tempHashMap.get(CRMBUILDUPDATEINFO.CHANGESET)));//No I18N
                jobDataMap.put("service", getStringValue(tempHashMap.get(CRMBUILDUPDATEINFO.SERVICE)));//No I18N
                jobDataMap.put("isPatch", getStringValue(tempHashMap.get(CRMBUILDUPDATEINFO.ISPATCH)));//No I18N
                jobDataMap.put("buildType", getStringValue(tempHashMap.get(CRMBUILDUPDATEINFO.BUILDTYPE)));//No I18N
                jobDataMap.put("dc", getStringValue(tempHashMap.get(STOREUPDATETEDTICKETS.DC)));// No I18N
                jobDataMap.put("schedularId", getStringValue(tempHashMap.get(STOREUPDATETEDTICKETS.SCHEDULERID)));// No I18N
                jobDataMap.put("edition", getStringValue(tempHashMap.get(CRMBUILDUPDATEINFO.EDITION)));// No I18N

                jobDataMap.put("BuildgeneratedTime", getStringValue(tempHashMap.get(CRMBUILDUPDATEINFO.STARTTIME)));// No I18N
                jobDataMap.put("buildReleasedTime", getStringValue(tempHashMap.get(CRMBUILDUPDATEINFO.ENDTIME)));// No I18N

                schedulerDetailArray.put(jobDataMap);

                SchedulerService schedulerService = new SchedulerService();
                String service = jobDataMap.getString("service");//No I18N
                String schedulerId = jobDataMap.getString("schedularId");//No I18N
                String edition = jobDataMap.getString("edition");//No I18N
                String buildName = jobDataMap.getString("buildName");//No I18N
                markAsSchedulerStatusPending(schedulerId);

          

                TicketTransistion ticketTransition = TicketTransistion.getTransition(service, null, null);


                // if ("CRMVERSION".equalsIgnoreCase(service)) { //No I18N
                //     if ("All Editions".equalsIgnoreCase(edition)) { //No I18N
                //         markAsSchedulerStatusPending(schedulerId);
                //         TicketsSchedulerInfoUtil.retryCreateSchedulers(buildName, schedulerService, jobDataMap, ticketTransition, 0);
                //         return "success"; //No I18N
                //     } else {
                //         markAsSchedulerStatusPending(schedulerId);
                //         TicketsSchedulerInfoUtil.retryCreateSchedulers(buildName, schedulerService, jobDataMap, ticketTransition, 1);
                //         return "success"; //No I18N
                //     }
                // }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "An error occurred while processing retryScheduler.", e); //No I18N
        }

        return "";
    }
                    

    private String getStringValue(Object value) {
        return value != null ? value.toString() : "";
    }

    private void markAsSchedulerStatusPending(String schedulerId){
        String schedulerStatus = "pending"; //No I18N
        SelectQuery selectQuery = new SelectQuery().addAllColumns().addCustomFromTable(STOREUPDATETEDTICKETS.TABLE);
        selectQuery.addCondition(BinaryCondition.equalTo(new CustomSql(STOREUPDATETEDTICKETS.SCHEDULERID), SDEAppConstants.PREPARED_STATEMENT_QUESTION_MARK));
        List<Map<String, Object>> ResultList = DataBaseConnectorUtil.getResultSetAsListFromSelectQuery(selectQuery.toString(), new Object[]{schedulerId});

        if (ResultList != null) {
            Boolean result = DataBaseConnectorUtil.updateRowToDataBase(
                    STOREUPDATETEDTICKETS.TABLE,
                    new String[]{STOREUPDATETEDTICKETS.SCHEDULERSTATUS},
                    new String[]{schedulerStatus},
                    new String[]{STOREUPDATETEDTICKETS.SCHEDULERID},
                    new String[]{schedulerId}
            );
    
            if (result) {
                LOGGER.log(Level.SEVERE,"Scheduler status marked as {0} in SCHEDULERSTATUS Column in STOREUPDATETEDTICKETS Table for Scheduler ID {1}", new Object[]{schedulerStatus,schedulerId}); //No I18N
            } else {
                LOGGER.log(Level.SEVERE,"Failed to update Scheduler status in STOREUPDATETEDTICKETS Table for Scheduler ID {0}", new Object[]{schedulerId}); //No I18N
            }
        }


    }
// -------------------------------------------------------------------------------------------------------------------------------------------------//
//                        Below Code resposnible to index Data in the Table 
// -------------------------------------------------------------------------------------------------------------------------------------------------//

@RequestMapping(value="mi/reindex", method = RequestMethod.POST) // No I18N
@Throttling(type = ThrottlingType.RemoteAddr, limit = 60, timeUnit = TimeUnit.SECONDS)
public static String reIndexOngoingIssuesForSearch(HttpServletRequest request, AbstractAuthenticationToken authentication) {
    String action = request.getParameter("action"); //No I18N
    try {
        reIndexCheckInDetails(action);
        return "reindexedSuccessfully"; //No I18N
    } catch (Exception e) {
        LOGGER.log(Level.WARNING, "Exception occurred while reindexing Lucene", e); //No I18N
        return "error"; //No I18N
    }
}

public static void reIndexThroughSchedulers(JobDataMap params) throws Exception {
    String dailyIndex = params.get("dailyIndex").toString();//No I18N
    reIndexCheckInDetails(dailyIndex);
}


private static void reIndexCheckInDetails(String actionString) throws IOException {
    File indexDirectoryFile = new File(PropertiesUtil.getProperty("lucene.index.path")); //No I18N
    Directory indexDirectory = FSDirectory.open(Paths.get(indexDirectoryFile.getAbsolutePath()));
    Analyzer analyzer = new SimpleAnalyzer();
    IndexWriterConfig iwConfig = new IndexWriterConfig(analyzer);
    IndexWriter indexWriter = new IndexWriter(indexDirectory, iwConfig);
     String querySql;
        if ("dailyIndex".equals(actionString)) {  //No I18N
            Query query = getDeleteQuery();
            indexWriter.deleteDocuments(query);
            querySql = "SELECT * FROM StoreUpdatedTickets INNER JOIN CrmBuildUpdateInfo ON StoreUpdatedTickets.BUILDID = CrmBuildUpdateInfo.BUILDID WHERE CrmBuildUpdateInfo.CREATEDTIME >= DATE_SUB(NOW(), INTERVAL 2 DAY) and StoreUpdatedTickets.SCHEDULERSTATUS is not null"; //No I18N
        } else {
            Query query  = getDeleteQueryforallDocument();
            indexWriter.deleteDocuments(query);
            querySql = "SELECT * FROM StoreUpdatedTickets INNER JOIN CrmBuildUpdateInfo ON StoreUpdatedTickets.BUILDID = CrmBuildUpdateInfo.BUILDID where StoreUpdatedTickets.SCHEDULERSTATUS is not null";  //No I18N
        }
    try (Connection connection =  (Connection) DriverManager.getConnection(SDEAppConstants.MYSQL_CONNECTOR_URL, SDEAppConstants.db_username, SDEAppConstants.db_password);
        Statement statement = (Statement) connection.createStatement();
        ResultSet resultSet = statement.executeQuery(querySql)) {   
        LOGGER.log(Level.INFO, "Result set found");  //No I18N
        while (resultSet.next()) {
            JSONObject jsonObject = new JSONObject();
            
            jsonObject.put(CRMBUILDUPDATEINFO.BUILDID, resultSet.getObject(CRMBUILDUPDATEINFO.BUILDID) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.BUILDID)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.BUILDID));
            jsonObject.put(CRMBUILDUPDATEINFO.BUILDURL, resultSet.getObject(CRMBUILDUPDATEINFO.BUILDURL) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.BUILDURL)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.BUILDURL));

            jsonObject.put(CRMBUILDUPDATEINFO.BUILDLABEL, resultSet.getObject(CRMBUILDUPDATEINFO.BUILDLABEL) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.BUILDLABEL)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.BUILDLABEL));
            jsonObject.put(CRMBUILDUPDATEINFO.ENDTIME, resultSet.getObject(CRMBUILDUPDATEINFO.ENDTIME) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.ENDTIME)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.ENDTIME));
            jsonObject.put(CRMBUILDUPDATEINFO.BUILDTYPE, resultSet.getObject(CRMBUILDUPDATEINFO.BUILDTYPE) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.BUILDTYPE)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.BUILDTYPE));
            jsonObject.put(CRMBUILDUPDATEINFO.COMMENT, resultSet.getObject(CRMBUILDUPDATEINFO.COMMENT) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.COMMENT)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.COMMENT));
            jsonObject.put(CRMBUILDUPDATEINFO.BRANCHNAME, resultSet.getObject(CRMBUILDUPDATEINFO.BRANCHNAME) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.BRANCHNAME)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.BRANCHNAME));
            jsonObject.put(CRMBUILDUPDATEINFO.CHANGESET, resultSet.getObject(CRMBUILDUPDATEINFO.CHANGESET) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.CHANGESET)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.CHANGESET));
            jsonObject.put(CRMBUILDUPDATEINFO.ISPATCH, resultSet.getObject(CRMBUILDUPDATEINFO.ISPATCH) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.ISPATCH)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.ISPATCH));
            jsonObject.put(CRMBUILDUPDATEINFO.SERVICE, resultSet.getObject(CRMBUILDUPDATEINFO.SERVICE) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.SERVICE)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.SERVICE));
            jsonObject.put(STOREUPDATETEDTICKETS.DC, resultSet.getObject(STOREUPDATETEDTICKETS.DC) == null || "null".equals(resultSet.getObject(STOREUPDATETEDTICKETS.DC)) ? "" : resultSet.getString(STOREUPDATETEDTICKETS.DC));
            jsonObject.put(CRMBUILDUPDATEINFO.STARTTIME, resultSet.getObject(CRMBUILDUPDATEINFO.STARTTIME) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.STARTTIME)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.STARTTIME));
            jsonObject.put(CRMBUILDUPDATEINFO.CREATEDTIME, resultSet.getObject(CRMBUILDUPDATEINFO.CREATEDTIME) == null || "null".equals(resultSet.getObject(CRMBUILDUPDATEINFO.CREATEDTIME)) ? "" : resultSet.getString(CRMBUILDUPDATEINFO.CREATEDTIME));
            jsonObject.put(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD, resultSet.getObject(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD) == null || "null".equals(resultSet.getObject(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD)) ? "" : resultSet.getString(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD));

            String value = resultSet.getObject(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD) == null || "null".equals(resultSet.getObject(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD)) ? "" : resultSet.getString(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD);
            byte[] utf8Bytes = value.getBytes("UTF-8");
            if (utf8Bytes.length > 29988) {
                // Split data into chunks of 29988 bytes
                int chunkSize = 29988;
                int offset = 0;
                while (offset < utf8Bytes.length) {
                    int end = Math.min(utf8Bytes.length, offset + chunkSize);
                    String chunk = new String(utf8Bytes, offset, end - offset, "UTF-8");
        
                    JSONObject chunkJsonObject = new JSONObject(jsonObject.toString()); // Clone original JSON object
                    chunkJsonObject.put(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD, chunk);
        
                    Document document = getLuceneDocForMi(chunkJsonObject);
                    indexWriter.addDocument(document);
        
                    offset += chunkSize;
                }
            } else {
                Document document = getLuceneDocForMi(jsonObject);
                indexWriter.addDocument(document);
            }

        }
    } catch (Exception e) {
        LOGGER.log(Level.SEVERE, "Error occurred in forming Lucene Document", e); //No I18N
    } finally {
        indexWriter.flush();
        indexWriter.close();
        LOGGER.log(Level.INFO, "Creator entry Data  Reindexed Successfully"); //No I18N
    }
}

public static Document getLuceneDocForMi(JSONObject jsonObj) throws Exception {
   
        
   
    FacetsConfig config = new FacetsConfig();    
    Document document = new Document();
    document.add(new StringField(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD, jsonObj.get(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD).toString(), Store.YES));


    document.add(new TextField(CRMBUILDUPDATEINFO.BUILDID,jsonObj.get(CRMBUILDUPDATEINFO.BUILDID).toString(),Store.YES));

    document.add(new TextField(CRMBUILDUPDATEINFO.BUILDLABEL, jsonObj.get(CRMBUILDUPDATEINFO.BUILDLABEL).toString(), Store.YES));
    document.add(new TextField(CRMBUILDUPDATEINFO.BUILDTYPE,jsonObj.get(CRMBUILDUPDATEINFO.BUILDTYPE).toString(),Store.YES));

    document.add(new TextField(CRMBUILDUPDATEINFO.ENDTIME,jsonObj.get(CRMBUILDUPDATEINFO.ENDTIME).toString(),Store.YES));
    document.add(new StringField(CRMBUILDUPDATEINFO.BUILDURL,jsonObj.get(CRMBUILDUPDATEINFO.BUILDURL).toString(),Store.YES));


    document.add(new TextField(CRMBUILDUPDATEINFO.COMMENT, jsonObj.get(CRMBUILDUPDATEINFO.COMMENT).toString(), Field.Store.YES));

    document.add(new TextField(CRMBUILDUPDATEINFO.BRANCHNAME,jsonObj.get(CRMBUILDUPDATEINFO.BRANCHNAME).toString(),Store.YES));
    document.add(new TextField(CRMBUILDUPDATEINFO.CHANGESET, jsonObj.get(CRMBUILDUPDATEINFO.CHANGESET).toString(),Store.YES));
    document.add(new TextField(CRMBUILDUPDATEINFO.ISPATCH, jsonObj.get(CRMBUILDUPDATEINFO.ISPATCH).toString(),Store.YES));
    document.add(new TextField(CRMBUILDUPDATEINFO.SERVICE, jsonObj.get(CRMBUILDUPDATEINFO.SERVICE).toString(),Store.YES));
    document.add(new TextField(STOREUPDATETEDTICKETS.DC, jsonObj.get(STOREUPDATETEDTICKETS.DC).toString(),Store.YES));
    document.add(new TextField(CRMBUILDUPDATEINFO.STARTTIME, jsonObj.get(CRMBUILDUPDATEINFO.STARTTIME).toString(),Store.YES));
    
    // Indexing with the Created time , Hence while deleting the Document based on that Field Value 
    String timestamp =jsonObj.get(CRMBUILDUPDATEINFO.CREATEDTIME).toString();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S"); //No I18N
    Date date = sdf.parse(timestamp);
    long milliseconds = date.getTime();

    // Add the current time to the document when indexing
    document.add(new LongPoint("indexedDate", milliseconds)); //No I18N
    return config.build(document);

}

private static Query getDeleteQuery() {
    // Construct a BooleanQuery to combine the existing query with the range query
    BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder(); 

    // Add the existing query (e.g., WildcardQuery) to the BooleanQuery
    Query existingQuery = new WildcardQuery(new Term(CRMBUILDUPDATEINFO.COMMENT, "*")); //No I18N
    booleanQueryBuilder.add(existingQuery, BooleanClause.Occur.MUST);

    // Calculate Date Two Days Ago
    Calendar cal = Calendar.getInstance();
    cal.add(Calendar.DATE, -2);
    Date twoDaysAgo = cal.getTime();

    // Convert the twoDaysAgo date to milliseconds since epoch
    long twoDaysAgoMillis = twoDaysAgo.getTime();

    // Construct a range query for documents indexed within the last two days up to now (inclusive)
    Query rangeQuery = LongPoint.newRangeQuery("indexedDate", twoDaysAgoMillis, System.currentTimeMillis()); //No I18N
    booleanQueryBuilder.add(rangeQuery, BooleanClause.Occur.MUST);

    // Return the combined query
    return booleanQueryBuilder.build();
}
private static Query getDeleteQueryforallDocument() {

    Query existingQuery = new WildcardQuery(new Term(CRMBUILDUPDATEINFO.COMMENT, "*")); //No I18N
   
    return existingQuery;
}

// -------------------------------------------------------------------------------------------------------------------------------------------------//
//                                    Need to Build the Query to search the Data in the Index Directory                                             //
// -------------------------------------------------------------------------------------------------------------------------------------------------//

@RequestMapping(value="mi/fetchCheckinData", method = RequestMethod.POST) // No I18N
@Throttling(type = ThrottlingType.RemoteAddr, limit = 60, timeUnit = TimeUnit.SECONDS)
private static String toFindCheckIn(HttpServletRequest request) throws ParseException, IOException  {

    String checkin = request.getParameter("CheckIn"); //No I18N
    QueryParser parser = new QueryParser("CREATORENTRIESONBUILD", new StandardAnalyzer()); //No I18N
    parser.setAllowLeadingWildcard(true); // Enable  wildcard
    Query wildcardQuery = parser.parse("*" + checkin + "*");    //No I18N
    return resultForTheQuery(wildcardQuery).toString();
}

private static JSONArray resultForTheQuery(Query checkinQuery) {
    JSONArray jsonArray = new JSONArray();
    try {
        File indexDirectoryFile = new File(PropertiesUtil.getProperty("lucene.index.path")); //No I18N

        Directory indexDirectory = FSDirectory.open(Paths.get(indexDirectoryFile.getAbsolutePath()));

        IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(indexDirectory));

        TopDocs hits = indexSearcher.search(checkinQuery, 10);
        
        if (hits.scoreDocs.length == 0) {
            LOGGER.log(Level.INFO, "No data in Lucene for query"); //No I18N
        } else {
            for (ScoreDoc scoreDoc : hits.scoreDocs) {
                Document doc = indexSearcher.doc(scoreDoc.doc);
                JSONObject jsonObj = new JSONObject();
                jsonObj.put(CRMBUILDUPDATEINFO.COMMENT, doc.get(CRMBUILDUPDATEINFO.COMMENT));

                jsonObj.put(CRMBUILDUPDATEINFO.BUILDLABEL,doc.get(CRMBUILDUPDATEINFO.BUILDLABEL));
                jsonObj.put(CRMBUILDUPDATEINFO.BUILDURL,doc.get(CRMBUILDUPDATEINFO.BUILDURL));
                jsonObj.put(CRMBUILDUPDATEINFO.CHANGESET,doc.get(CRMBUILDUPDATEINFO.CHANGESET));
                jsonObj.put(CRMBUILDUPDATEINFO.SERVICE,doc.get(CRMBUILDUPDATEINFO.SERVICE));
                jsonObj.put(CRMBUILDUPDATEINFO.STARTTIME,doc.get(CRMBUILDUPDATEINFO.STARTTIME));
                jsonObj.put(CRMBUILDUPDATEINFO.ENDTIME,doc.get(CRMBUILDUPDATEINFO.ENDTIME));
                jsonObj.put(CRMBUILDUPDATEINFO.ISPATCH,doc.get(CRMBUILDUPDATEINFO.ISPATCH));
                jsonObj.put(CRMBUILDUPDATEINFO.BUILDURL,doc.get(CRMBUILDUPDATEINFO.BUILDURL));
                jsonObj.put(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD,doc.get(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD));
                jsonObj.put(STOREUPDATETEDTICKETS.DC,doc.get(STOREUPDATETEDTICKETS.DC));
                jsonObj.put(STOREUPDATETEDTICKETS.BUILDID,doc.get(STOREUPDATETEDTICKETS.BUILDID));

                jsonArray.put(jsonObj);   
            }
        }
        indexDirectory.close();
        LOGGER.log(Level.INFO, "Search Result with length :: {0}", jsonArray.length());//No I18N
    } catch (IOException e) {
        LOGGER.log(Level.SEVERE, "Error occurred during search", e);//No I18N
    }
    return jsonArray;
}
// For Revamp As of Now Not In Use
private static Query Buildquery(String keyword) throws ParseException {
    QueryParser parser = new QueryParser(STOREUPDATETEDTICKETS.CREATORENTRIESONBUILD, new SimpleAnalyzer());
    Query query = parser.parse(keyword);
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(query, BooleanClause.Occur.MUST);
    Query booleanQuery = builder.build();
    LOGGER.log(Level.INFO, "Final Query :: {0}", booleanQuery.toString());//No I18N
    return booleanQuery;
}

// For Revamp As of Now Not In Use
private static Query toTestQuerys(String keyword) throws ParseException{
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    Query booleanQuery;
    QueryParser queryParser = new QueryParser("COMMENT", new KeywordAnalyzer()); //No I18N
    queryParser.setAllowLeadingWildcard(true); // If you want to allow leading wildcards
     booleanQuery = queryParser.parse(QueryParserBase.escape(keyword));

    BooleanClause bClause = new BooleanClause(booleanQuery, Occur.SHOULD);
    builder.add(bClause);

    return new BooleanQuery.Builder().add(builder.build(), Occur.MUST).build();
    }

}   


 