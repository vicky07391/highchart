package com.crucialbits.cy.custom.service;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.crucialbits.chart.highchart.Chart;
import com.crucialbits.chart.highchart.Column;
import com.crucialbits.chart.highchart.Data;
import com.crucialbits.chart.highchart.Events;
import com.crucialbits.chart.highchart.Highchart;
import com.crucialbits.chart.highchart.Labels;
import com.crucialbits.chart.highchart.Options3d;
import com.crucialbits.chart.highchart.PlotOptions;
import com.crucialbits.chart.highchart.Point;
import com.crucialbits.chart.highchart.Series;
import com.crucialbits.chart.highchart.Title;
import com.crucialbits.chart.highchart.XAxis;
import com.crucialbits.chart.highchart.YAxis;
import com.crucialbits.cy.app.Constants;
import com.crucialbits.cy.app.Utility;
import com.crucialbits.cy.dao.CustomerDAO;
import com.crucialbits.cy.dao.PodDataCacheDAO;
import com.crucialbits.cy.dao.SimpleeCustomerDataDAO;
import com.crucialbits.cy.dao.SimpleeEligibilityDAO;
import com.crucialbits.cy.dao.SimpleeInformationDAO;
import com.crucialbits.cy.dao.SimpleeTransactionDAO;
import com.crucialbits.cy.model.Account;
import com.crucialbits.cy.model.Customer;
import com.crucialbits.cy.model.Information;
import com.crucialbits.cy.model.PodConfiguration;
import com.crucialbits.cy.model.PodDataCache;
import com.crucialbits.cy.model.Properties;
import com.crucialbits.cy.model.SimpleeCustomerData;
import com.crucialbits.cy.model.SimpleeEligibility;
import com.crucialbits.cy.model.SimpleeInformation;
import com.crucialbits.cy.model.SimpleeTransaction;
import com.crucialbits.util.CryptoHelper;
import com.crucialbits.util.StringHelper;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SimpleePodService implements Constants {

	private final static Logger logger = Logger.getLogger(SimpleePodService.class);
	
	public void cdetailsmarketemailcalculations(Account account, String mmyy) throws Exception {
		
		CustomerDAO customerDAO = new CustomerDAO();
		SimpleeCustomerDataDAO simpleeCustomerDataDAO = new SimpleeCustomerDataDAO();
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();
		DecimalFormat df = new DecimalFormat("#.#");
		
		logger.debug("mmyy: " + mmyy);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		
		int skip = 0;
		int limit = 1000;
		
		Double allSend = 0D;
		Double allBounce = 0D;
		Double allOpen = 0D;
		Double allClick = 0D;
		Double allOpenRate = 0D;
		Double allClickThroughRate = 0D;
		
		Double billRelatedSend = 0D;
		Double billRelatedBounce = 0D;
		Double billRelatedOpen = 0D;
		Double billRelatedClick = 0D;
		Double billRelatedDrivePay = 0D;
		Double billRelatedOpenRate = 0D;
		Double billRelatedClickThroughRate = 0D; 
		Double billRelatedDrivePayRate = 0D;
		
		Double welcomeSend = 0D;
		Double welcomeBounce = 0D;
		Double welcomeOpen = 0D;
		Double welcomeClick = 0D;
		Double welcomeOpenRate = 0D;
		Double welcomeClickThroughRate = 0D; 

		Double newBillReadySend = 0D;
		Double newBillReadyBounce = 0D;
		Double newBillReadyOpen = 0D;
		Double newBillReadyClick = 0D;
		Double newBillReadyDrivePay = 0D;
		Double newBillReadyOpenRate = 0D;
		Double newBillReadyClickThroughRate = 0D; 
		Double newBillReadyDrivePayRate = 0D; 
		
		Double billReminder1Send = 0D;
		Double billReminder1Bounce = 0D;
		Double billReminder1Open = 0D;
		Double billReminder1Click = 0D;
		Double billReminder1DrivePay = 0D;
		Double billReminder1OpenRate = 0D;
		Double billReminder1ClickThroughRate = 0D; 
		Double billReminder1DrivePayRate = 0D; 
		
		Double billReminder2Send = 0D;
		Double billReminder2Bounce = 0D;
		Double billReminder2Open = 0D;
		Double billReminder2Click = 0D;
		Double billReminder2DrivePay = 0D;
		Double billReminder2OpenRate = 0D;
		Double billReminder2ClickThroughRate = 0D; 
		Double billReminder2DrivePayRate = 0D; 
		
		Double collectionsWarning1Send = 0D;
		Double collectionsWarning1Bounce = 0D;
		Double collectionsWarning1Open = 0D;
		Double collectionsWarning1Click = 0D;
		Double collectionsWarning1DrivePay = 0D;
		Double collectionsWarning1OpenRate = 0D;
		Double collectionsWarning1ClickThroughRate = 0D; 
		Double collectionsWarning1DrivePayRate = 0D; 
		
		Double collectionsWarning2Send = 0D;
		Double collectionsWarning2Bounce = 0D;
		Double collectionsWarning2Open = 0D;
		Double collectionsWarning2Click = 0D;
		Double collectionsWarning2DrivePay = 0D;
		Double collectionsWarning2OpenRate = 0D;
		Double collectionsWarning2ClickThroughRate = 0D; 
		Double collectionsWarning2DrivePayRate = 0D; 
		
		while(true) {
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
			if(customers.size() == 0) {
				break;
			}
			
			for(Customer customer : customers) {
				List<String> markets = simpleeCustomerDataDAO.getDistinctMarkets(customer.getId(), mmyy);
				if(markets.size() > 0) {
					
					for(String market : markets) {
						allSend = 0D;
						allBounce = 0D;
						allOpen = 0D;
						allClick = 0D;
						allOpenRate = 0D;
						allClickThroughRate = 0D;
						
						billRelatedSend = 0D;
						billRelatedBounce = 0D;
						billRelatedOpen = 0D;
						billRelatedClick = 0D;
						billRelatedDrivePay = 0D;
						billRelatedOpenRate = 0D;
						billRelatedClickThroughRate = 0D; 
						billRelatedDrivePayRate = 0D; 
						
						welcomeSend = 0D;
						welcomeBounce = 0D;
						welcomeOpen = 0D;
						welcomeClick = 0D;
						welcomeOpenRate = 0D;
						welcomeClickThroughRate = 0D; 
						
						newBillReadySend = 0D;
						newBillReadyBounce = 0D;
						newBillReadyOpen = 0D;
						newBillReadyClick = 0D;
						newBillReadyDrivePay = 0D;
						newBillReadyOpenRate = 0D;
						newBillReadyClickThroughRate = 0D; 
						newBillReadyDrivePayRate = 0D; 
						
						billReminder1Send = 0D;
						billReminder1Bounce = 0D;
						billReminder1Open = 0D;
						billReminder1Click = 0D;
						billReminder1DrivePay = 0D;
						billReminder1OpenRate = 0D;
						billReminder1ClickThroughRate = 0D; 
						billReminder1DrivePayRate = 0D; 
						
						billReminder2Send = 0D;
						billReminder2Bounce = 0D;
						billReminder2Open = 0D;
						billReminder2Click = 0D;
						billReminder2DrivePay = 0D;
						billReminder2OpenRate = 0D;
						billReminder2ClickThroughRate = 0D; 
						billReminder2DrivePayRate = 0D; 
						
						collectionsWarning1Send = 0D;
						collectionsWarning1Bounce = 0D;
						collectionsWarning1Open = 0D;
						collectionsWarning1Click = 0D;
						collectionsWarning1DrivePay = 0D;
						collectionsWarning1OpenRate = 0D;
						collectionsWarning1ClickThroughRate = 0D; 
						collectionsWarning1DrivePayRate = 0D; 
						
						collectionsWarning2Send = 0D;
						collectionsWarning2Bounce = 0D;
						collectionsWarning2Open = 0D;
						collectionsWarning2Click = 0D;
						collectionsWarning2DrivePay = 0D;
						collectionsWarning2OpenRate = 0D;
						collectionsWarning2ClickThroughRate = 0D; 
						collectionsWarning2DrivePayRate = 0D; 
						
						fieldMap.clear();
						fieldMap.put("customerId", customer.getId());
						fieldMap.put("market", market);
						fieldMap.put("mmyy", mmyy);
						
						List<SimpleeCustomerData> mailStats = simpleeCustomerDataDAO.findAll(fieldMap);
						for(SimpleeCustomerData mailStat : mailStats) {
							allSend += (mailStat.getAll_Emails_Send() != null) ? mailStat.getAll_Emails_Send() : 0;
							allBounce += (mailStat.getAll_Emails_Bounce() != null) ? mailStat.getAll_Emails_Bounce() : 0;
							allOpen += (mailStat.getAll_Emails_Open() != null) ? mailStat.getAll_Emails_Open() : 0;
							allClick += (mailStat.getAll_Emails_Click() != null) ? mailStat.getAll_Emails_Click() : 0;
							
							billRelatedSend += (mailStat.getBills_Related_Send() != null) ? mailStat.getBills_Related_Send() : 0;
							billRelatedBounce += (mailStat.getBills_Related_Bounce() != null) ? mailStat.getBills_Related_Bounce() : 0;
							billRelatedOpen += (mailStat.getBills_Related_Open() != null) ? mailStat.getBills_Related_Open() : 0;
							billRelatedClick += (mailStat.getBills_Related_Click() != null) ? mailStat.getBills_Related_Click() : 0;
							billRelatedDrivePay += (mailStat.getBills_Related_Drive_Pay() != null) ? mailStat.getBills_Related_Drive_Pay() : 0;
							
							welcomeSend += (mailStat.getWelcome_Send() != null) ? mailStat.getWelcome_Send() : 0;
							welcomeBounce += (mailStat.getWelcome_Bounce() != null) ? mailStat.getWelcome_Bounce() : 0;
							welcomeOpen += (mailStat.getWelcome_Open() != null) ? mailStat.getWelcome_Open() : 0;
							welcomeClick += (mailStat.getWelcome_Click() != null) ? mailStat.getWelcome_Click() : 0;
						
							newBillReadySend += (mailStat.getNew_Bill_Ready_Send() != null) ? mailStat.getNew_Bill_Ready_Send() : 0;
							newBillReadyBounce += (mailStat.getNew_Bill_Ready_Bounce() != null) ? mailStat.getNew_Bill_Ready_Bounce() : 0;
							newBillReadyOpen += (mailStat.getNew_Bill_Ready_Open() != null) ? mailStat.getNew_Bill_Ready_Open() : 0;
							newBillReadyClick += (mailStat.getNew_Bill_Ready_Open() != null) ? mailStat.getNew_Bill_Ready_Open() : 0;
							newBillReadyDrivePay += (mailStat.getNew_Bill_Ready_Drive_Pay() != null) ? mailStat.getNew_Bill_Ready_Drive_Pay() : 0;
												
							billReminder1Send += (mailStat.getBill_Reminder1_Send() != null) ? mailStat.getBill_Reminder1_Send() : 0;
							billReminder1Bounce += (mailStat.getBill_Reminder1_Bounce() != null) ? mailStat.getBill_Reminder1_Bounce() : 0;
							billReminder1Open += (mailStat.getBill_Reminder1_Open() != null) ? mailStat.getBill_Reminder1_Open() : 0;
							billReminder1Click += (mailStat.getBill_Reminder1_Click() != null) ? mailStat.getBill_Reminder1_Click() : 0;
							billReminder1DrivePay += (mailStat.getBill_Reminder1_Drive_Pay() != null) ? mailStat.getBill_Reminder1_Drive_Pay() : 0;

							billReminder2Send += (mailStat.getBill_Reminder2_Send() != null) ? mailStat.getBill_Reminder2_Send() : 0;
							billReminder2Bounce += (mailStat.getBill_Reminder2_Bounce() != null) ? mailStat.getBill_Reminder2_Bounce() : 0;
							billReminder2Open += (mailStat.getBill_Reminder2_Open() != null) ? mailStat.getBill_Reminder2_Open() : 0;
							billReminder2Click += (mailStat.getBill_Reminder2_Click() != null) ? mailStat.getBill_Reminder2_Click() : 0;
							billReminder2DrivePay += (mailStat.getBill_Reminder2_Drive_Pay() != null) ? mailStat.getBill_Reminder2_Drive_Pay() : 0;

							collectionsWarning1Send += (mailStat.getCollections_Warning1_Send() != null) ? mailStat.getCollections_Warning1_Send() : 0;
							collectionsWarning1Bounce += (mailStat.getCollections_Warning1_Bounce() != null) ? mailStat.getCollections_Warning1_Bounce() : 0;
							collectionsWarning1Open += (mailStat.getCollections_Warning1_Open() != null) ? mailStat.getCollections_Warning1_Open() : 0;
							collectionsWarning1Click += (mailStat.getCollections_Warning1_Click() != null) ? mailStat.getCollections_Warning1_Click() : 0;
							collectionsWarning1DrivePay += (mailStat.getCollections_Warning1_Drive_Pay() != null) ? mailStat.getCollections_Warning1_Drive_Pay() : 0;

							collectionsWarning2Send += (mailStat.getCollections_Warning2_Send() != null) ? mailStat.getCollections_Warning2_Send() : 0;
							collectionsWarning2Bounce += (mailStat.getCollections_Warning2_Bounce() != null) ? mailStat.getCollections_Warning2_Bounce() : 0;
							collectionsWarning2Open += (mailStat.getCollections_Warning2_Open() != null) ? mailStat.getCollections_Warning2_Open() : 0;
							collectionsWarning2Click += (mailStat.getCollections_Warning2_Click() != null) ? mailStat.getCollections_Warning2_Click() : 0;
							collectionsWarning2DrivePay += (mailStat.getCollections_Warning2_Drive_Pay() != null) ? mailStat.getCollections_Warning2_Drive_Pay() : 0;

						}
						
						allOpenRate = allOpen * 100;
						allOpenRate = allOpenRate / (allSend - allBounce);
						
						allClickThroughRate = allClick * 100;
						allClickThroughRate = allClickThroughRate / (allSend - allBounce);
						
						billRelatedOpenRate = billRelatedOpen * 100;
						billRelatedOpenRate = billRelatedOpenRate / (billRelatedSend - billRelatedBounce);
						
						billRelatedClickThroughRate = billRelatedClick * 100;
						billRelatedClickThroughRate = billRelatedClickThroughRate / (billRelatedSend - billRelatedBounce);
						
						billRelatedDrivePayRate = billRelatedDrivePay * 100;
						billRelatedDrivePayRate = billRelatedDrivePayRate / (billRelatedSend - billRelatedBounce);
						
						welcomeOpenRate = welcomeOpen * 100;
						welcomeOpenRate = welcomeOpenRate / (welcomeSend - welcomeBounce);
						
						welcomeClickThroughRate = welcomeClick * 100;
						welcomeClickThroughRate = welcomeClickThroughRate / (welcomeSend - welcomeBounce);
						
						
						newBillReadyOpenRate = newBillReadyOpen * 100;
						newBillReadyOpenRate = newBillReadyOpenRate / (newBillReadySend - newBillReadyBounce);
						
						newBillReadyClickThroughRate = newBillReadyClick * 100;
						newBillReadyClickThroughRate = newBillReadyClickThroughRate / (newBillReadySend - newBillReadyBounce);
						
						newBillReadyDrivePayRate = newBillReadyDrivePay * 100;
						newBillReadyDrivePayRate = newBillReadyDrivePayRate / (newBillReadySend - newBillReadyBounce);;
						
						billReminder1OpenRate = billReminder1Open * 100;
						billReminder1OpenRate = billReminder1OpenRate / (billReminder1Send - billReminder1Bounce);
						
						billReminder1ClickThroughRate = billReminder1Click * 100;
						billReminder1ClickThroughRate = billReminder1ClickThroughRate / (billReminder1Send - billReminder1Bounce);
						
						billReminder1DrivePayRate = billReminder1DrivePay * 100;
						billReminder1DrivePayRate = billReminder1DrivePayRate / (billReminder1Send - billReminder1Bounce);
						
						billReminder2OpenRate = billReminder2Open * 100;
						billReminder2OpenRate = billReminder2OpenRate / (billReminder2Send - billReminder2Bounce);
						
						billReminder2ClickThroughRate = billReminder2Click * 100;
						billReminder2ClickThroughRate = billReminder2ClickThroughRate / (billReminder2Send - billReminder2Bounce);
						
						billReminder2DrivePayRate = billReminder2DrivePay * 100;
						billReminder2DrivePayRate = billReminder2DrivePayRate / (billReminder2Send - billReminder2Bounce);
						
						
						collectionsWarning1OpenRate = collectionsWarning1Open * 100;
						collectionsWarning1OpenRate = collectionsWarning1OpenRate / (collectionsWarning1Send - collectionsWarning1Bounce);
						
						collectionsWarning1ClickThroughRate = collectionsWarning1Click * 100;
						collectionsWarning1ClickThroughRate = collectionsWarning1ClickThroughRate / (collectionsWarning1Send - collectionsWarning1Bounce);
						
						collectionsWarning1DrivePayRate = collectionsWarning1DrivePay * 100;
						collectionsWarning1DrivePayRate = collectionsWarning1DrivePayRate / (collectionsWarning1Send - collectionsWarning1Bounce);
						
						collectionsWarning2OpenRate = collectionsWarning2Open * 100;
						collectionsWarning2OpenRate = collectionsWarning2OpenRate / (collectionsWarning2Send - collectionsWarning2Bounce);
						
						collectionsWarning2ClickThroughRate = collectionsWarning2Click * 100;
						collectionsWarning2ClickThroughRate = collectionsWarning2ClickThroughRate / (collectionsWarning2Send - collectionsWarning2Bounce);
						
						collectionsWarning2DrivePayRate = collectionsWarning2DrivePay * 100;
						collectionsWarning2DrivePayRate = collectionsWarning2DrivePayRate / (collectionsWarning2Send - collectionsWarning2Bounce);
						
						SimpleeInformation si = new SimpleeInformation();
						
						si.setCustomerId(customer.getId());
						si.setMarket(market);
						si.setType("MARKETANALYSIS");
						si.setDate(new Date());
						si.setMmyy(mmyy);
						
						si.setAllSend((!Double.isNaN(allSend)) ? Double.parseDouble(df.format(allSend)) : 0);
						si.setAllOpenRate((!Double.isNaN(allOpenRate)) ? Double.parseDouble(df.format(allOpenRate)) : 0);
						si.setAllClickThroughRate((!Double.isNaN(allClickThroughRate)) ? Double.parseDouble(df.format(allClickThroughRate)) : 0);
						
						si.setBillRelatedSend((!Double.isNaN(billRelatedSend)) ? Double.parseDouble(df.format(billRelatedSend)) : 0);
						si.setBillRelatedOpenRate((!Double.isNaN(billRelatedOpenRate)) ? Double.parseDouble(df.format(billRelatedOpenRate)) : 0);
						si.setBillRelatedClickThroughRate((!Double.isNaN(billRelatedClickThroughRate)) ? Double.parseDouble(df.format(billRelatedClickThroughRate)) : 0);
						si.setBillRelatedDrivePayRate((!Double.isNaN(billRelatedDrivePayRate)) ? Double.parseDouble(df.format(billRelatedDrivePayRate)) : 0);
						
						si.setWelcomeSend((!Double.isNaN(welcomeSend)) ? Double.parseDouble(df.format(welcomeSend)) : 0);
						si.setWelcomeOpenRate((!Double.isNaN(welcomeOpenRate)) ? Double.parseDouble(df.format(welcomeOpenRate)) : 0);
						si.setWelcomeClickThroughRate((!Double.isNaN(welcomeClickThroughRate)) ? Double.parseDouble(df.format(welcomeClickThroughRate)) : 0);
						
						si.setNewBillReadySend((!Double.isNaN(newBillReadySend)) ? Double.parseDouble(df.format(newBillReadySend)) : 0);						
						si.setNewBillReadyOpenRate((!Double.isNaN(newBillReadyOpenRate)) ? Double.parseDouble(df.format(newBillReadyOpenRate)) : 0);
						si.setNewBillReadyClickThroughRate((!Double.isNaN(newBillReadyClickThroughRate)) ? Double.parseDouble(df.format(newBillReadyClickThroughRate)) : 0);
						si.setNewBillReadyDrivePayRate((!Double.isNaN(newBillReadyDrivePayRate)) ? Double.parseDouble(df.format(newBillReadyDrivePayRate)) : 0);
						
						si.setBillReminder1Send((!Double.isNaN(billReminder1Send)) ? Double.parseDouble(df.format(billReminder1Send)) : 0);
						si.setBillReminder1OpenRate((!Double.isNaN(billReminder1OpenRate)) ? Double.parseDouble(df.format(billReminder1OpenRate)) : 0);
						si.setBillReminder1ClickThroughRate((!Double.isNaN(billReminder1ClickThroughRate)) ? Double.parseDouble(df.format(billReminder1ClickThroughRate)) : 0);
						si.setBillReminder1DrivePayRate((!Double.isNaN(billReminder1DrivePayRate)) ? Double.parseDouble(df.format(billReminder1DrivePayRate)) : 0);
						
						si.setBillReminder2Send((!Double.isNaN(billReminder2Send)) ? Double.parseDouble(df.format(billReminder2Send)) : 0);
						si.setBillReminder2OpenRate((!Double.isNaN(billReminder2OpenRate)) ? Double.parseDouble(df.format(billReminder2OpenRate)) : 0);
						si.setBillReminder2ClickThroughRate((!Double.isNaN(billReminder2ClickThroughRate)) ? Double.parseDouble(df.format(billReminder2ClickThroughRate)) : 0);
						si.setBillReminder2DrivePayRate((!Double.isNaN(billReminder2DrivePayRate)) ? Double.parseDouble(df.format(billReminder2DrivePayRate)) : 0);
						
						
						si.setCollectionsWarning1Send((!Double.isNaN(collectionsWarning1Send)) ? Double.parseDouble(df.format(collectionsWarning1Send)) : 0);
						si.setCollectionsWarning1OpenRate((!Double.isNaN(collectionsWarning1OpenRate)) ? Double.parseDouble(df.format(collectionsWarning1OpenRate)) : 0);
						si.setCollectionsWarning1ClickThroughRate((!Double.isNaN(collectionsWarning1ClickThroughRate)) ? Double.parseDouble(df.format(collectionsWarning1ClickThroughRate)) : 0);
						si.setCollectionsWarning1DrivePayRate((!Double.isNaN(collectionsWarning1DrivePayRate)) ? Double.parseDouble(df.format(collectionsWarning1DrivePayRate)) : 0);
						
						
						si.setCollectionsWarning2Send((!Double.isNaN(collectionsWarning2Send)) ? Double.parseDouble(df.format(collectionsWarning2Send)) : 0);
						si.setCollectionsWarning2OpenRate((!Double.isNaN(collectionsWarning2OpenRate)) ? Double.parseDouble(df.format(collectionsWarning2OpenRate)) : 0);
						si.setCollectionsWarning2ClickThroughRate((!Double.isNaN(collectionsWarning2ClickThroughRate)) ? Double.parseDouble(df.format(collectionsWarning2ClickThroughRate)) : 0);
						si.setCollectionsWarning2DrivePayRate((!Double.isNaN(collectionsWarning2DrivePayRate)) ? Double.parseDouble(df.format(collectionsWarning2DrivePayRate)) : 0);
			
						simpleeInformationDAO.insert(si);
					}
				}
			}
			
			skip += limit;
		}
		logger.debug("cdetailsmarketemailcalculations : done");
	}
	
	public void cdetailspaperstatementcalculations(Account account, String mmyy) throws Exception {
		
		CustomerDAO customerDAO = new CustomerDAO();
		SimpleeCustomerDataDAO simpleeCustomDataDAO = new SimpleeCustomerDataDAO();
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		
		int skip = 0;
		int limit = 1000;
		
		Long papers = 0L;
		
		while(true) {
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
			if(customers.size() == 0) {
				break;
			}
			
			for(Customer customer : customers) {
				List<String> markets = simpleeCustomDataDAO.getDistinctMarkets(customer.getId(), mmyy);
				if(markets.size() > 0) {
					
					for(String market : markets) {
						papers = 0L;
						
						fieldMap.clear();
						fieldMap.put("customerId", customer.getId());
						fieldMap.put("market", market);
						fieldMap.put("mmyy", mmyy);
						
						List<SimpleeCustomerData> paperStats = simpleeCustomDataDAO.findAll(fieldMap);
						for(SimpleeCustomerData paperStat : paperStats) {
							papers += (paperStat.getPapers() != null) ? paperStat.getPapers() : 0;
						}
						
						SimpleeInformation si = new SimpleeInformation();
						
						si.setCustomerId(customer.getId());
						si.setMarket(market);
						si.setType("PAPERANALYSIS");
						si.setDate(new Date());
						si.setPapers(papers);
						si.setMmyy(mmyy);
						
						simpleeInformationDAO.insert(si);
					}
				}
			}
			
			skip += limit;
		}
		logger.debug("cdetailspaperstatementcalculations : done");
	}
	
	public void cdetailsdunningcyclecalculations(Account account, String mmyy) throws Exception {
		
		CustomerDAO customerDAO = new CustomerDAO();
		SimpleeCustomerDataDAO simpleeCustomDataDAO = new SimpleeCustomerDataDAO();
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		
		int skip = 0;
		int limit = 1000;
		
		Long totalBills = 0L;
		Long processedBills = 0L;
		Long noDunning = 0L;
		Long s1 = 0L;
		Long s2 = 0L;
		Long c1= 0L;
		Long c2 = 0L;
		Long p1 = 0L;
		Long p2 = 0L;
		Long p1m = 0L;
		Long p2m = 0L;
		Long c1m = 0L;
		Long c2m = 0L;
		
		while(true) {
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
			if(customers.size() == 0) {
				break;
			}
			
			for(Customer customer : customers) {
				List<String> markets = simpleeCustomDataDAO.getDistinctMarkets(customer.getId(), mmyy);
				if(markets.size() > 0) {
					
					for(String market : markets) {
						totalBills = 0L;
						processedBills = 0L;
						noDunning = 0L;
						s1 = 0L;
						s2 = 0L;
						c1= 0L;
						c2 = 0L;
						p1 = 0L;
						p2 = 0L;
						p1m = 0L;
						p2m = 0L;
						c1m = 0L;
						c2m = 0L;
						
						fieldMap.clear();
						fieldMap.put("customerId", customer.getId());
						fieldMap.put("market", market);
						fieldMap.put("mmyy", mmyy);
						
						List<SimpleeCustomerData> stats = simpleeCustomDataDAO.findAll(fieldMap);
						for(SimpleeCustomerData stat : stats) {
							totalBills += (stat.getTotalBills() != null) ? stat.getTotalBills().longValue() : 0;
							processedBills += (stat.getProcessedBills() != null) ? stat.getProcessedBills().longValue() : 0;
							noDunning += (stat.getNoDunningCode() != null) ? stat.getNoDunningCode().longValue() : 0;
							s1 += (stat.getS1() != null) ? stat.getS1().longValue() : 0;
							s2 += (stat.getS2() != null) ? stat.getS2().longValue() : 0;
							c1 += (stat.getC1() != null) ? stat.getC1().longValue() : 0;
							c2 += (stat.getC2() != null) ? stat.getC2().longValue() : 0;
							p1 += (stat.getP1() != null) ? stat.getP1().longValue() : 0;
							p2 += (stat.getP2() != null) ? stat.getP2().longValue() : 0;
							p1m += (stat.getP1m() != null) ? stat.getP1m().longValue() : 0;
							p2m += (stat.getP2m() != null) ? stat.getP2m().longValue() : 0;
							c1m += (stat.getC1m() != null) ? stat.getC1m().longValue() : 0;
							c2m += (stat.getC2m() != null) ? stat.getC2m().longValue() : 0;
							
						}
						
						SimpleeInformation si = new SimpleeInformation();
						
						si.setCustomerId(customer.getId());
						si.setMarket(market);
						si.setType("DUNNINGANALYSIS");
						si.setDate(new Date());
						si.setMmyy(mmyy);
						
						si.setTotalBills(totalBills);
						si.setProcessedBills(processedBills);
						si.setNoDunning(noDunning);
						si.setS1(s1);
						si.setS2(s2);
						si.setC1(c1);
						si.setC2(c2);
						si.setP1(p1);
						si.setP2(p2);
						si.setP1m(p1m);
						si.setP2m(p2m);
						si.setC1m(c1m);
						si.setC2m(c2m);
						
						simpleeInformationDAO.insert(si);
					}
				}
			}
			
			skip += limit;
		}
		logger.debug("cdetailsdunningcyclecalculations : done");
	}
	
	public void cdetailsmarketcomparison$calculations(Account account, String mmyy) throws Exception {
		
		CustomerDAO customerDAO = new CustomerDAO();
		SimpleeTransactionDAO simpleeTransactionDAO = new SimpleeTransactionDAO();
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();
		DecimalFormat df = new DecimalFormat("#.#");
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		
		int skip = 0;
		int limit = 1000;
		
		List<Properties> headers = Utility.getInstance().monthHeaders(mmyy);
		
		Double avg = 0D;
		Double month3Sum = 0D;
		Long kpi3MonthsAverage = 0L;
		Long kpiMonthVsMonthGrowthAmount = 0L;
		Double kpiMonthVsMonthGrowthPercent = 0D;
		
		Long month3SumTransaction = 0L;
		Long kpi3MonthsAverageTransaction = 0L;
		Long kpiMonthVsMonthGrowthTransaction = 0L;
		Double kpiMonthVsMonthGrowthTransactionPercent = 0D;
		
		Map<String, Double> amounts = new LinkedHashMap<String, Double>();
		Map<String, Long> transactions = new LinkedHashMap<String, Long>();
		Map<String, Double> averages = new LinkedHashMap<String, Double>();
		
		while(true) {
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
			if(customers.size() == 0) {
				break;
			}
			
			for(Customer customer : customers) {
				List<String> markets = simpleeTransactionDAO.getDistinctMarkets(customer.getId(), mmyy);
				Collections.sort(markets);
				if(markets.size() > 0) {
				
					for(String market : markets) {
						logger.debug("Market: " + market);
						
						for(Properties header : headers) {
							amounts.put(header.getKey(), getFilteredTransactions(mmyy, customer.getId(), market, header.getIndex(), fieldMap, simpleeTransactionDAO));
							transactions.put(header.getVertical(), getFilteredTransactionCounts(mmyy, customer.getId(), market, header.getIndex(), fieldMap, simpleeTransactionDAO));
						}
						
						for(Properties header : headers) {
							avg = 0D;
							try {
								avg = (amounts.get(header.getKey()) != null && transactions.get(header.getVertical()) != null) ? amounts.get(header.getKey()) / transactions.get(header.getVertical()) : 0;
							} catch(ArithmeticException ae) {}
							averages.put(header.getCategory(), (!Double.isInfinite(avg) && !Double.isNaN(avg)) ? Double.parseDouble(df.format(avg)) : 0);
						}
						
						fieldMap.clear();
						fieldMap.put("customerId", customer.getId());
						fieldMap.put("market", market);
						fieldMap.put("mmyy", mmyy);
						SimpleeTransaction simpleeTransaction = simpleeTransactionDAO.findOne(fieldMap);
						if(simpleeTransaction != null) {
						
							SimpleeInformation si = new SimpleeInformation();
							
							si.setCustomerId(customer.getId());
							si.setMarket(market);
							si.setType("MARKET$COMPARISONANALYSIS");
							si.setDate(new Date());
							si.setMmyy(simpleeTransaction.getMmyy());
							
							List<Object> columns = new ArrayList<Object>();
							List<Object> transactionColumns = new ArrayList<Object>();
							List<Object> averageColumns = new ArrayList<Object>();
							
							si.setJanAmount((amounts.get("janAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("janAmount")))) : 0);
							si.setFebAmount((amounts.get("febAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("febAmount")))) : 0);
							si.setMarAmount((amounts.get("marAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("marAmount")))) : 0);
							si.setAprAmount((amounts.get("aprAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("aprAmount")))) : 0);
							si.setMayAmount((amounts.get("mayAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("mayAmount")))) : 0);
							si.setJunAmount((amounts.get("junAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("junAmount")))) : 0);
							si.setJulAmount((amounts.get("julAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("julAmount")))) : 0);
							si.setAugAmount((amounts.get("augAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("augAmount")))) : 0);
							si.setSepAmount((amounts.get("sepAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("sepAmount")))) : 0);
							si.setOctAmount((amounts.get("octAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("octAmount")))) : 0);
							si.setNovAmount((amounts.get("novAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("novAmount")))) : 0);
							si.setDecAmount((amounts.get("decAmount") != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get("decAmount")))) : 0);
							
							si.setJanTransaction((transactions.get("janTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("janTransaction")))) : 0);
							si.setFebTransaction((transactions.get("febTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("febTransaction")))) : 0);
							si.setMarTransaction((transactions.get("marTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("marTransaction")))) : 0);
							si.setAprTransaction((transactions.get("aprTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("aprTransaction")))) : 0);
							si.setMayTransaction((transactions.get("mayTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("mayTransaction")))) : 0);
							si.setJunTransaction((transactions.get("junTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("junTransaction")))) : 0);
							si.setJulTransaction((transactions.get("julTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("julTransaction")))) : 0);
							si.setAugTransaction((transactions.get("augTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("augTransaction")))) : 0);
							si.setSepTransaction((transactions.get("sepTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("sepTransaction")))) : 0);
							si.setOctTransaction((transactions.get("octTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("octTransaction")))) : 0);
							si.setNovTransaction((transactions.get("novTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("novTransaction")))) : 0);
							si.setDecTransaction((transactions.get("decTransaction") != null) ? Long.parseLong(String.valueOf(Math.round(transactions.get("decTransaction")))) : 0);
							
							si.setJanAVG(averages.get("janAVG"));
							si.setFebAVG(averages.get("febAVG"));
							si.setMarAVG(averages.get("marAVG"));
							si.setAprAVG(averages.get("aprAVG"));
							si.setMayAVG(averages.get("mayAVG"));
							si.setJunAVG(averages.get("junAVG"));
							si.setJulAVG(averages.get("julAVG"));
							si.setAugAVG(averages.get("augAVG"));
							si.setSepAVG(averages.get("sepAVG"));
							si.setOctAVG(averages.get("octAVG"));
							si.setNovAVG(averages.get("novAVG"));
							si.setDecAVG(averages.get("decAVG"));
							
							for(Properties header : headers) {
								columns.add((amounts.get(header.getKey()) != null) ? Double.parseDouble(String.valueOf(Math.round(amounts.get(header.getKey())))) : 0);
							}
							for(Properties header : headers) {
								transactionColumns.add((transactions.get(header.getVertical()) != null) ? Math.round(transactions.get(header.getVertical())) : 0);
							}
							for(Properties header : headers) {
								averageColumns.add((averages.get(header.getCategory()) != null) ? averages.get(header.getCategory()) : 0);
							}
							si.setColumns(columns);
							si.setTransactionColumns(transactionColumns);
							si.setAvgColumns(averageColumns);
							si.setHeaders(headers);
							
							month3Sum = 0D;
							kpi3MonthsAverage = 0L;
							kpiMonthVsMonthGrowthAmount = 0L;
							kpiMonthVsMonthGrowthPercent = 0D;
							
							month3Sum += Double.parseDouble(columns.get(columns.size() - 1).toString());
							month3Sum += Double.parseDouble(columns.get(columns.size() - 2).toString());
							month3Sum += Double.parseDouble(columns.get(columns.size() - 3).toString());
							
							kpi3MonthsAverage = Math.round(month3Sum / 3);
							kpiMonthVsMonthGrowthAmount = Math.round(Double.parseDouble(columns.get(columns.size() - 1).toString())) - Math.round(Double.parseDouble(columns.get(columns.size() - 2).toString()));
							try {
								kpiMonthVsMonthGrowthPercent = kpiMonthVsMonthGrowthAmount / Double.parseDouble(columns.get(columns.size() - 2).toString());
//								logger.debug(kpiMonthVsMonthGrowthPercent + "\t" + kpiMonthVsMonthGrowthAmount + "\t" + Double.parseDouble(columns.get(columns.size() - 2).toString()));
							} catch(ArithmeticException ae){}
							kpiMonthVsMonthGrowthPercent = kpiMonthVsMonthGrowthPercent * 100;
							
							si.setKpi3MonthAverage(Double.parseDouble(String.valueOf(kpi3MonthsAverage)));
							si.setKpiMonthVsMonthGrowthAmount(Double.parseDouble(String.valueOf(kpiMonthVsMonthGrowthAmount)));
							si.setKpiMonthVsMonthGrowthPercent((!Double.isInfinite(kpiMonthVsMonthGrowthPercent) && !Double.isNaN(kpiMonthVsMonthGrowthPercent)) ? Double.parseDouble(String.valueOf(Math.round(kpiMonthVsMonthGrowthPercent))) : 0);
							
							month3SumTransaction = 0L;
							kpi3MonthsAverageTransaction = 0L;
							kpiMonthVsMonthGrowthTransaction = 0L;
							kpiMonthVsMonthGrowthTransactionPercent = 0D;
							
							month3SumTransaction += Long.parseLong(transactionColumns.get(transactionColumns.size() - 1).toString());
							month3SumTransaction += Long.parseLong(transactionColumns.get(transactionColumns.size() - 2).toString());
							month3SumTransaction += Long.parseLong(transactionColumns.get(transactionColumns.size() - 3).toString());
							
							kpi3MonthsAverageTransaction = Long.parseLong(String.valueOf(Math.round(month3SumTransaction / 3)));
							kpiMonthVsMonthGrowthTransaction = Math.round(Double.parseDouble(transactionColumns.get(transactionColumns.size() - 1).toString())) - Math.round(Double.parseDouble(transactionColumns.get(transactionColumns.size() - 2).toString()));
							try {
								kpiMonthVsMonthGrowthTransactionPercent = kpiMonthVsMonthGrowthTransaction / Double.parseDouble(transactionColumns.get(transactionColumns.size() - 2).toString());
							} catch(ArithmeticException ae){}
							kpiMonthVsMonthGrowthTransactionPercent = kpiMonthVsMonthGrowthTransactionPercent * 100;
							
							si.setKpi3MonthAverageTransaction(kpi3MonthsAverageTransaction);
							si.setKpiMonthVsMonthGrowthTransaction(kpiMonthVsMonthGrowthTransaction);
							si.setKpiMonthVsMonthGrowthTransactionPercent((!Double.isInfinite(kpiMonthVsMonthGrowthTransactionPercent) && !Double.isNaN(kpiMonthVsMonthGrowthTransactionPercent)) ? Long.parseLong(String.valueOf(Math.round(kpiMonthVsMonthGrowthTransactionPercent))) : 0);
							
							simpleeInformationDAO.insert(si);
							
						}
					}
				}
			}
			
			skip += limit;
		}
		logger.debug("cdetailsmarketcomparison$calculations : done");
	}
	
	public void cdetailseligibilityusagecalculations(Account account, String mmyy) throws Exception {
		
		CustomerDAO customerDAO = new CustomerDAO();
		SimpleeEligibilityDAO simpleeEligibilityDAO = new SimpleeEligibilityDAO();
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		
		int skip = 0;
		int limit = 1000;
		
		Long count = 0L;
		
		while(true) {
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
			if(customers.size() == 0) {
				break;
			}
			
			for(Customer customer : customers) {
				List<String> markets = simpleeEligibilityDAO.getDistinctMarkets(customer.getId(), mmyy);
				if(markets.size() > 0) {
					
					for(String market : markets) {
						count = 0L;
						
						count = simpleeEligibilityDAO.countByFilters(customer.getId(), market, mmyy);
						
						fieldMap.clear();
						fieldMap.put("customerId", customer.getId());
						fieldMap.put("market", market);
						fieldMap.put("mmyy", mmyy);
						SimpleeEligibility eligibility = simpleeEligibilityDAO.findOne(fieldMap);
						
						if(eligibility != null) {
							SimpleeInformation si = new SimpleeInformation();
							
							si.setCustomerId(customer.getId());
							si.setMarket(market);
							si.setType("ELIGIBILITYUSAGENANALYSIS");
							si.setDate(new Date());
							si.setEligibilityTransactions(count);
							si.setMmyy(eligibility.getMmyy());
							
							simpleeInformationDAO.insert(si);
						}
						
					}
				}
			}
			
			skip += limit;
		}
		logger.debug("cdetailseligibilityusagecalculations : done");
	}
	
	public Double getFilteredTransactions(String mmyy, String customerId, String market, Integer month, Map<String, Object> fieldMap, SimpleeTransactionDAO simpleeTransactionDAO) {
		Double sum = 0D;
		int tSkip = 0;
		int tLimit = 1000;
		while(true) {
			fieldMap.clear();
			fieldMap.put("customerId", customerId);
			fieldMap.put("market", market);
			fieldMap.put("month", month);
			fieldMap.put("mmyy", mmyy);
			List<SimpleeTransaction> transactions = simpleeTransactionDAO.findAll(fieldMap, "date", true, tSkip, tLimit);
			if(transactions.size() == 0) {
				break;
			}
			for(SimpleeTransaction transaction : transactions) {
				sum += (transaction.getAmount() != null) ? transaction.getAmount() : 0;
			}
			tSkip += tLimit;
		}
		return sum;
	}
	
	public long getFilteredTransactionCounts(String mmyy, String customerId, String market, Integer month, Map<String, Object> fieldMap, SimpleeTransactionDAO simpleeTransactionDAO) {
		Long counts = 0L;
		int tSkip = 0;
		int tLimit = 1000;
		while(true) {
			fieldMap.clear();
			fieldMap.put("customerId", customerId);
			fieldMap.put("market", market);
			fieldMap.put("month", month);
			fieldMap.put("mmyy", mmyy);
			List<SimpleeTransaction> transactions = simpleeTransactionDAO.findAll(fieldMap, "date", true, tSkip, tLimit);
			if(transactions.size() == 0) {
				break;
			}
			counts += transactions.size();
			tSkip += tLimit;
		}
		return counts;
	}
	
	public Map<String, Object> cdetailsallemailssimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		fieldMap.put("type", "MARKETANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKETANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsallemailssimplee");
		cachedData.put("apiName", "cdetailsallemailssimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "All Emails");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_ALL_EMAILS_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsbillrelatedemailssimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		fieldMap.put("type", "MARKETANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKETANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsbillrelatedemailssimplee");
		cachedData.put("apiName", "cdetailsbillrelatedemailssimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Bill Related Emails");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_BILL_RELATED_EMAILS_SIMPLEE", "yes");
		
		return cachedData;
	}

	public Map<String, Object> cdetailswelcomeemailssimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();

		fieldMap.put("type", "MARKETANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKETANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailswelcomeemailssimplee");
		cachedData.put("apiName", "cdetailswelcomeemailssimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Welcome Emails");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_WELCOME_EMAILS_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsnewbillsreadysimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();

		fieldMap.put("type", "MARKETANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKETANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsnewbillsreadysimplee");
		cachedData.put("apiName", "cdetailsnewbillsreadysimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "New Bills Ready");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_NEW_BILLS_READY_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsbillreminderiemailssimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		fieldMap.put("type", "MARKETANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKETANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsbillreminderiemailssimplee");
		cachedData.put("apiName", "cdetailsbillreminderiemailssimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Bill Reminder I");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_BILL_REMINDER_I_EMAILS_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsbillreminderiiemailssimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		fieldMap.put("type", "MARKETANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKETANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsbillreminderiiemailssimplee");
		cachedData.put("apiName", "cdetailsbillreminderiiemailssimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Bill Reminder II");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_BILL_REMINDER_II_EMAILS_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailscollectionswarningiemailssimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		fieldMap.put("type", "MARKETANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKETANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailscollectionswarningiemailssimplee");
		cachedData.put("apiName", "cdetailscollectionswarningiemailssimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Collections Warning I");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_COLLECTIONS_WARNING_I_EMAILS_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailscollectionswarningiiemailssimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();

		fieldMap.put("type", "MARKETANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKETANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailscollectionswarningiiemailssimplee");
		cachedData.put("apiName", "cdetailscollectionswarningiiemailssimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Collections Warning II");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_COLLECTIONS_WARNING_II_EMAILS_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailspaperstatementssimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		fieldMap.put("type", "PAPERANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "PAPERANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailspaperstatementssimplee");
		cachedData.put("apiName", "cdetailspaperstatementssimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Papers Sent");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_PAPER_STATEMENTS_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsbillsdunningcyclesimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		fieldMap.put("type", "DUNNINGANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "DUNNINGANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsbillsdunningcyclesimplee");
		cachedData.put("apiName", "cdetailsbillsdunningcyclesimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Bills Dunning Cycle - Status");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_BILLS_DUNNING_CYCLE_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsmarketamountcomparisonsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		fieldMap.put("type", "MARKET$COMPARISONANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKET$COMPARISONANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		List<Properties> headers = Utility.getInstance().monthHeaders(monthFilter);
		
		cachedData.put("headers", headers);
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsmarketamountcomparisonsimplee");
		cachedData.put("apiName", "cdetailsmarketamountcomparisonsimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Patient Collections Growth - $ Market Comparison");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_MARKET_AMOUNT_COMPARISON_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailskpiscollectedamountbymarketsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		fieldMap.put("type", "MARKET$COMPARISONANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKET$COMPARISONANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		List<Properties> headers = Utility.getInstance().monthHeaders(monthFilter);
		
		cachedData.put("headers", headers);
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailskpiscollectedamountbymarketsimplee");
		cachedData.put("apiName", "cdetailskpiscollectedamountbymarketsimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "KPI's - $ Collected By Market");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_KPIS_COLLECTED_AMOUNT_BY_MARKET_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsmarketviewbydepartmentamountsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, String market) throws Exception {
		
		SimpleeTransactionDAO simpleeTransactionDAO = new SimpleeTransactionDAO();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		String[] splittedMonthFilter = monthFilter.split(" ");
		
		List<String> markets = simpleeTransactionDAO.getDistinctMarkets(customerId, monthFilter);
		Collections.sort(markets);
		
		if(StringHelper.isEmpty(market)) {
			market = markets.get(0);
		}
		
		List<String> departments = simpleeTransactionDAO.getDistinctDepartments(customerId, market, monthFilter);
		Collections.sort(departments);
		
		List<Properties> months = Utility.getInstance().monthHeaders(splittedMonthFilter[0]);
		
		for(Properties month : months) {
			List<Object> values = new ArrayList<Object>();
			Long sum = 0L;
			for(String department : departments) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, department, null, String.valueOf(month.getIndex()), monthFilter));
				sum += value;
				values.add(value);
			}
			values.add(0, month.getValue());
			values.add(sum);
			month.setValues(values);
		}
		departments.add("Total Payments");
		
		List<String> payments = simpleeTransactionDAO.getDistinctPaymentMethods(customerId, market, monthFilter);
		Collections.sort(payments);
		
		List<Properties> paymentMonths = Utility.getInstance().monthHeaders(splittedMonthFilter[0]);
		
		for(Properties month : paymentMonths) {
			List<Object> values = new ArrayList<Object>();
			Long sum = 0L;
			for(String payment : payments) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, null, payment, String.valueOf(month.getIndex()), monthFilter));
				sum += value;
				values.add(value);
			}
			values.add(0, month.getValue());
			values.add(sum);
			month.setValues(values);
		}
		payments.add("Total Payments");
		
		List<Properties> kpiDepartmentInformationns = new ArrayList<Properties>();
		List<String> kpiDepartmentHeaders = new ArrayList<String>();
		
		Long sum = 0L;
		List<Object> months3AvgValues = new ArrayList<Object>();
		for(String department : departments) {
			if(!kpiDepartmentHeaders.contains(department) && !department.toLowerCase().contains("return")) {
				kpiDepartmentHeaders.add(department);
			}
			if(!department.toLowerCase().contains("return") && !department.equals("Total Payments")) {
				
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter));
				value += Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				value += Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 3).getIndex()), monthFilter));
				value = Math.round(value / 3);
				sum += value;
				months3AvgValues.add(value);
			}
		}
		Properties prop = new Properties();
		months3AvgValues.add(0, "3 Month Avg $");
		months3AvgValues.add(Math.round(sum));
		prop.setValues(months3AvgValues);
		kpiDepartmentInformationns.add(prop);
		
		sum = 0L;
		Double val = 0D;
		List<Object> months3AvgPercent = new ArrayList<Object>();
		for(int x = 1; x < months3AvgValues.size() - 1; x++) {
			val = 0D;
			try {
				val = Double.parseDouble(months3AvgValues.get(x).toString()) / Double.parseDouble(months3AvgValues.get(months3AvgValues.size() - 1).toString());
				val = val * 100;
			} catch(ArithmeticException ae){}
			months3AvgPercent.add(Math.round(val));
			sum += Math.round(val);
		}
		prop = new Properties();
		months3AvgPercent.add(0, "3 Month Avg %");
		months3AvgPercent.add(Math.round(sum));
		prop.setValues(months3AvgPercent);
		kpiDepartmentInformationns.add(prop);
		
		sum = 0L;
		List<Object> monthVsMonthGrowth = new ArrayList<Object>();
		for(String department : departments) {
			if(!kpiDepartmentHeaders.contains(department) && !department.toLowerCase().contains("return")) {
				kpiDepartmentHeaders.add(department);
			}
			if(!department.toLowerCase().contains("return") && !department.equals("Total Payments")) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter)) - Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				sum += value;
				monthVsMonthGrowth.add(value);
			}
		}
		prop = new Properties();
		monthVsMonthGrowth.add(0, "Month/Month Growth $");
		monthVsMonthGrowth.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowth);
		kpiDepartmentInformationns.add(prop);
		
		sum = 0L;
		Double percent = 0D;
		List<Object> monthVsMonthGrowthPercents = new ArrayList<Object>();
		for(String department : departments) {
			if(!kpiDepartmentHeaders.contains(department) && !department.toLowerCase().contains("return")) {
				kpiDepartmentHeaders.add(department);
			}
			if(!department.toLowerCase().contains("return") && !department.equals("Total Payments")) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter)) - Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				percent = 0D;
				try {
					percent = Double.parseDouble(String.valueOf(value / simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter)));
					percent = percent * 100;
				} catch(ArithmeticException ae) {}
				sum += (!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0;
				monthVsMonthGrowthPercents.add((!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0);
			}
		}
		prop = new Properties();
		monthVsMonthGrowthPercents.add(0, "Month/Month Growth %");
		monthVsMonthGrowthPercents.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowthPercents);
		kpiDepartmentInformationns.add(prop);
		
		List<Properties> kpiPaymentInformationns = new ArrayList<Properties>();
		List<String> kpiPaymentHeaders = new ArrayList<String>();
		
		sum = 0L;
		months3AvgValues = new ArrayList<Object>();
		for(String payment : payments) {
			if(!kpiPaymentHeaders.contains(payment) && !payment.toLowerCase().contains("return")) {
				kpiPaymentHeaders.add(payment);
			}
			if(!payment.toLowerCase().contains("return") && !payment.equals("Total Payments")) {
				
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter));
				value += Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				value += Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 3).getIndex()), monthFilter));
				value = Math.round(value / 3);
				sum += value;
				months3AvgValues.add(value);
			}
		}
		prop = new Properties();
		months3AvgValues.add(0, "3 Month Avg $");
		months3AvgValues.add(Math.round(sum));
		prop.setValues(months3AvgValues);
		kpiPaymentInformationns.add(prop);
		
		sum = 0L;
		val = 0D;
		months3AvgPercent = new ArrayList<Object>();
		for(int x = 1; x < months3AvgValues.size() - 1; x++) {
			val = 0D;
			try {
				val = Double.parseDouble(months3AvgValues.get(x).toString()) / Double.parseDouble(months3AvgValues.get(months3AvgValues.size() - 1).toString());
				val = val * 100;
			} catch(ArithmeticException ae){}
			months3AvgPercent.add(Math.round(val));
			sum += Math.round(val);
		}
		prop = new Properties();
		months3AvgPercent.add(0, "3 Month Avg %");
		months3AvgPercent.add(Math.round(sum));
		prop.setValues(months3AvgPercent);
		kpiPaymentInformationns.add(prop);
		
		sum = 0L;
		monthVsMonthGrowth = new ArrayList<Object>();
		for(String payment : payments) {
			if(!kpiPaymentHeaders.contains(payment) && !payment.toLowerCase().contains("return")) {
				kpiPaymentHeaders.add(payment);
			}
			if(!payment.toLowerCase().contains("return") && !payment.equals("Total Payments")) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter)) - Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				sum += value;
				monthVsMonthGrowth.add(value);
			}
		}
		prop = new Properties();
		monthVsMonthGrowth.add(0, "Month/Month Growth $");
		monthVsMonthGrowth.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowth);
		kpiPaymentInformationns.add(prop);
		
		sum = 0L;
		percent = 0D;
		monthVsMonthGrowthPercents = new ArrayList<Object>();
		for(String payment : payments) {
			if(!kpiPaymentHeaders.contains(payment) && !payment.toLowerCase().contains("return")) {
				kpiPaymentHeaders.add(payment);
			}
			if(!payment.toLowerCase().contains("return") && !payment.equals("Total Payments")) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter)) - Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				percent = 0D;
				try {
					percent = Double.parseDouble(String.valueOf(value / simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter)));
					percent = percent * 100;
				} catch(ArithmeticException ae) {}
				sum += (!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0;
				monthVsMonthGrowthPercents.add((!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0);
			}
		}
		prop = new Properties();
		monthVsMonthGrowthPercents.add(0, "Month/Month Growth %");
		monthVsMonthGrowthPercents.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowthPercents);
		kpiPaymentInformationns.add(prop);
		
		cachedData.put("title", "$ - By Department");
		cachedData.put("paymentTitle", "$ - By Payment Methods");
		cachedData.put("kpiDepartmentTitle", "KPI's - $ Collected By Departments");
		cachedData.put("kpiPaymentTitle", "KPI's - $ Collected By Payment Methods");
		
		cachedData.put("headers", departments);
		cachedData.put("paymentHeaders", payments);
		cachedData.put("kpiDepartmentHeaders", kpiDepartmentHeaders);
		cachedData.put("kpiPaymentHeaders", kpiPaymentHeaders);
		
		cachedData.put("informations", months);
		cachedData.put("paymentInformationns", paymentMonths);
		cachedData.put("kpiDepartmentInformationns", kpiDepartmentInformationns);
		cachedData.put("kpiPaymentInformationns", kpiPaymentInformationns);
		
		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsmarketviewbydepartmentamountsimplee");
		cachedData.put("apiName", "cdetailsmarketviewbydepartmentamountsimplee");
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("markets", markets);
		cachedData.put("market", market);
		
		cachedData.put("CDETAILS_MARKET_VIEW_BY_DEPARTMENT_AMOUNT_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsallmarketsamountbydepartmentsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, String market) throws Exception {
		
		SimpleeTransactionDAO simpleeTransactionDAO = new SimpleeTransactionDAO();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		String[] splittedMonthFilter = monthFilter.split(" ");
		
		List<String> departments = simpleeTransactionDAO.getDistinctDepartments(customerId, market, monthFilter);
		Collections.sort(departments);
		
		List<Properties> months = Utility.getInstance().monthHeaders(splittedMonthFilter[0]);
		
		for(Properties month : months) {
			List<Object> values = new ArrayList<Object>();
			Long sum = 0L;
			for(String department : departments) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, department, null, String.valueOf(month.getIndex()), monthFilter));
				sum += value;
				values.add(value);
			}
			values.add(0, month.getValue());
			values.add(sum);
			month.setValues(values);
		}
		departments.add("Total Payments");
		
		List<Properties> kpiDepartmentInformationns = new ArrayList<Properties>();
		List<String> kpiDepartmentHeaders = new ArrayList<String>();
		
		Long sum = 0L;
		List<Object> months3AvgValues = new ArrayList<Object>();
		for(String department : departments) {
			if(!kpiDepartmentHeaders.contains(department)) {
				kpiDepartmentHeaders.add(department);
			}
			if(!department.equals("Total Payments")) {
				
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, department, null, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter));
				value += Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				value += Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, department, null, String.valueOf(months.get(months.size() - 3).getIndex()), monthFilter));
				value = Math.round(value / 3);
				sum += value;
				months3AvgValues.add(value);
			}
		}
		Properties prop = new Properties();
		months3AvgValues.add(0, "3 Month Avg $");
		months3AvgValues.add(Math.round(sum));
		prop.setValues(months3AvgValues);
		kpiDepartmentInformationns.add(prop);
		
		sum = 0L;
		Double val = 0D;
		List<Object> months3AvgPercent = new ArrayList<Object>();
		for(int x = 1; x < months3AvgValues.size() - 1; x++) {
			val = 0D;
			try {
				val = Double.parseDouble(months3AvgValues.get(x).toString()) / Double.parseDouble(months3AvgValues.get(months3AvgValues.size() - 1).toString());
				val = val * 100;
			} catch(ArithmeticException ae){}
			months3AvgPercent.add(Math.round(val));
			sum += Math.round(val);
		}
		prop = new Properties();
		months3AvgPercent.add(0, "3 Month Avg %");
		months3AvgPercent.add(Math.round(sum));
		prop.setValues(months3AvgPercent);
		kpiDepartmentInformationns.add(prop);
		
		sum = 0L;
		List<Object> monthVsMonthGrowth = new ArrayList<Object>();
		for(String department : departments) {
			if(!kpiDepartmentHeaders.contains(department)) {
				kpiDepartmentHeaders.add(department);
			}
			if(!department.equals("Total Payments")) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, department, null, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter)) - Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				sum += value;
				monthVsMonthGrowth.add(value);
			}
		}
		prop = new Properties();
		monthVsMonthGrowth.add(0, "Month/Month Growth $");
		monthVsMonthGrowth.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowth);
		kpiDepartmentInformationns.add(prop);
		
		sum = 0L;
		Double percent = 0D;
		List<Object> monthVsMonthGrowthPercents = new ArrayList<Object>();
		for(String department : departments) {
			if(!kpiDepartmentHeaders.contains(department)) {
				kpiDepartmentHeaders.add(department);
			}
			if(!department.equals("Total Payments")) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, department, null, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter)) - Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				percent = 0D;
				try {
					percent = Double.parseDouble(String.valueOf(value / simpleeTransactionDAO.getAmountSumUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter)));
					percent = percent * 100;
				} catch(ArithmeticException ae) {}
				sum += (!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0;
				monthVsMonthGrowthPercents.add((!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0);
			}
		}
		prop = new Properties();
		monthVsMonthGrowthPercents.add(0, "Month/Month Growth %");
		monthVsMonthGrowthPercents.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowthPercents);
		kpiDepartmentInformationns.add(prop);
		
		cachedData.put("title", "Patient Collections - $ By Departments (All Markets)");
		cachedData.put("kpiDepartmentTitle", "KPI's - $ Collected By Departments (All Markets)");
		
		cachedData.put("headers", departments);
		cachedData.put("kpiDepartmentHeaders", kpiDepartmentHeaders);
		
		cachedData.put("informations", months);
		cachedData.put("kpiDepartmentInformationns", kpiDepartmentInformationns);
		
		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsallmarketsamountbydepartmentsimplee");
		cachedData.put("apiName", "cdetailsallmarketsamountbydepartmentsimplee");
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("market", market);
		
		cachedData.put("CDETAILS_ALL_MARKETS_AMOUNT_BY_DEPARTMENT_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsallmarketsamountbypaymentmethodsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, String market) throws Exception {
		
		SimpleeTransactionDAO simpleeTransactionDAO = new SimpleeTransactionDAO();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		String[] splittedMonthFilter = monthFilter.split(" ");
		
		List<String> payments = simpleeTransactionDAO.getDistinctPaymentMethods(customerId, market, monthFilter);
		Collections.sort(payments);
		
		List<Properties> months = Utility.getInstance().monthHeaders(splittedMonthFilter[0]);
		
		for(Properties month : months) {
			List<Object> values = new ArrayList<Object>();
			Long sum = 0L;
			for(String payment : payments) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, null, payment, String.valueOf(month.getIndex()), monthFilter));
				sum += value;
				values.add(value);
			}
			values.add(0, month.getValue());
			values.add(sum);
			month.setValues(values);
		}
		payments.add("Total Payments");
		
		List<Properties> kpiPaymentInformationns = new ArrayList<Properties>();
		List<String> kpiPaymentHeaders = new ArrayList<String>();
		
		Long sum = 0L;
		List<Object> months3AvgValues = new ArrayList<Object>();
		for(String payment : payments) {
			if(!kpiPaymentHeaders.contains(payment) && !payment.toLowerCase().contains("return")) {
				kpiPaymentHeaders.add(payment);
			}
			if(!payment.toLowerCase().contains("return") && !payment.equals("Total Payments")) {
				
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, null, payment, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter));
				value += Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				value += Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, null, payment, String.valueOf(months.get(months.size() - 3).getIndex()), monthFilter));
				value = Math.round(value / 3);
				sum += value;
				months3AvgValues.add(value);
			}
		}
		Properties prop = new Properties();
		months3AvgValues.add(0, "3 Month Avg $");
		months3AvgValues.add(Math.round(sum));
		prop.setValues(months3AvgValues);
		kpiPaymentInformationns.add(prop);
		
		sum = 0L;
		Double val = 0D;
		List<Object> months3AvgPercent = new ArrayList<Object>();
		for(int x = 1; x < months3AvgValues.size() - 1; x++) {
			val = 0D;
			try {
				val = Double.parseDouble(months3AvgValues.get(x).toString()) / Double.parseDouble(months3AvgValues.get(months3AvgValues.size() - 1).toString());
				val = val * 100;
			} catch(ArithmeticException ae){}
			months3AvgPercent.add(Math.round(val));
			sum += Math.round(val);
		}
		prop = new Properties();
		months3AvgPercent.add(0, "3 Month Avg %");
		months3AvgPercent.add(Math.round(sum));
		prop.setValues(months3AvgPercent);
		kpiPaymentInformationns.add(prop);
		
		sum = 0L;
		List<Object> monthVsMonthGrowth = new ArrayList<Object>();
		for(String payment : payments) {
			if(!kpiPaymentHeaders.contains(payment) && !payment.toLowerCase().contains("return")) {
				kpiPaymentHeaders.add(payment);
			}
			if(!payment.toLowerCase().contains("return") && !payment.equals("Total Payments")) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, null, payment, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter)) - Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				sum += value;
				monthVsMonthGrowth.add(value);
			}
		}
		prop = new Properties();
		monthVsMonthGrowth.add(0, "Month/Month Growth $");
		monthVsMonthGrowth.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowth);
		kpiPaymentInformationns.add(prop);
		
		sum = 0L;
		Double percent = 0D;
		List<Object> monthVsMonthGrowthPercents = new ArrayList<Object>();
		for(String payment : payments) {
			if(!kpiPaymentHeaders.contains(payment) && !payment.toLowerCase().contains("return")) {
				kpiPaymentHeaders.add(payment);
			}
			if(!payment.toLowerCase().contains("return") && !payment.equals("Total Payments")) {
				long value = Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, null, payment, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter)) - Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter));
				percent = 0D;
				try {
					percent = Double.parseDouble(String.valueOf(value / simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter)));
					percent = percent * 100;
				} catch(ArithmeticException ae) {}
				sum += (!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0;
				monthVsMonthGrowthPercents.add((!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0);
			}
		}
		prop = new Properties();
		monthVsMonthGrowthPercents.add(0, "Month/Month Growth %");
		monthVsMonthGrowthPercents.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowthPercents);
		kpiPaymentInformationns.add(prop);
		
		cachedData.put("title", "Patient Collections - $ By Payment Methods (All Markets)");
		cachedData.put("kpiPaymentTitle", "KPI's - $ Collected By Payment Methods (All Markets)");
		
		cachedData.put("headers", payments);
		cachedData.put("kpiPaymentHeaders", kpiPaymentHeaders);
		
		cachedData.put("informations", months);
		cachedData.put("kpiPaymentInformationns", kpiPaymentInformationns);
		
		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsallmarketsamountbypaymentmethodsimplee");
		cachedData.put("apiName", "cdetailsallmarketsamountbypaymentmethodsimplee");
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("market", market);
		
		cachedData.put("CDETAILS_ALL_MARKETS_AMOUNT_BY_PAYMENT_METHOD_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsmarketviewbydepartmenttransactionssimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, String market) throws Exception {
		
		SimpleeTransactionDAO simpleeTransactionDAO = new SimpleeTransactionDAO();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		String[] splittedMonthFilter = monthFilter.split(" ");
		
		List<String> markets = simpleeTransactionDAO.getDistinctMarkets(customerId, monthFilter);
		Collections.sort(markets);
		
		if(StringHelper.isEmpty(market)) {
			market = markets.get(0);
		}
		
		List<String> departments = simpleeTransactionDAO.getDistinctDepartments(customerId, market, monthFilter);
		Collections.sort(departments);
		
		List<Properties> months = Utility.getInstance().monthHeaders(splittedMonthFilter[0]);
		
		for(Properties month : months) {
			List<Object> values = new ArrayList<Object>();
			Long sum = 0L;
			for(String department : departments) {
				long value = simpleeTransactionDAO.countUsingFilters(customerId, market, department, null, String.valueOf(month.getIndex()), monthFilter);
				sum += value;
				values.add(value);
			}
			values.add(0, month.getValue());
			values.add(sum);
			month.setValues(values);
		}
		departments.add("Total Payments");
		
		List<String> payments = simpleeTransactionDAO.getDistinctPaymentMethods(customerId, market, monthFilter);
		Collections.sort(payments);
		
		List<Properties> paymentMonths = Utility.getInstance().monthHeaders(splittedMonthFilter[0]);
		
		for(Properties month : paymentMonths) {
			List<Object> values = new ArrayList<Object>();
			Long sum = 0L;
			for(String payment : payments) {
				long value = simpleeTransactionDAO.countUsingFilters(customerId, market, null, payment, String.valueOf(month.getIndex()), monthFilter);
				sum += value;
				values.add(value);
			}
			values.add(0, month.getValue());
			values.add(sum);
			month.setValues(values);
		}
		payments.add("Total Payments");
		
		List<Properties> kpiDepartmentInformationns = new ArrayList<Properties>();
		List<String> kpiDepartmentHeaders = new ArrayList<String>();
		
		Long sum = 0L;
		List<Object> months3AvgValues = new ArrayList<Object>();
		for(String department : departments) {
			if(!kpiDepartmentHeaders.contains(department) && !department.toLowerCase().contains("return")) {
				kpiDepartmentHeaders.add(department);
			}
			if(!department.toLowerCase().contains("return") && !department.equals("Total Payments")) {
				
				long value = simpleeTransactionDAO.countUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter);
				value += simpleeTransactionDAO.countUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter);
				value += simpleeTransactionDAO.countUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 3).getIndex()), monthFilter);
				value = Math.round(value / 3);
				sum += value;
				months3AvgValues.add(value);
			}
		}
		Properties prop = new Properties();
		months3AvgValues.add(0, "3 Month Avg $");
		months3AvgValues.add(Math.round(sum));
		prop.setValues(months3AvgValues);
		kpiDepartmentInformationns.add(prop);
		
		sum = 0L;
		Double val = 0D;
		List<Object> months3AvgPercent = new ArrayList<Object>();
		for(int x = 1; x < months3AvgValues.size() - 1; x++) {
			val = 0D;
			try {
				val = Double.parseDouble(months3AvgValues.get(x).toString()) / Double.parseDouble(months3AvgValues.get(months3AvgValues.size() - 1).toString());
				val = val * 100;
			} catch(ArithmeticException ae){}
			months3AvgPercent.add(Math.round(val));
			sum += Math.round(val);
		}
		prop = new Properties();
		months3AvgPercent.add(0, "3 Month Avg %");
		months3AvgPercent.add(Math.round(sum));
		prop.setValues(months3AvgPercent);
		kpiDepartmentInformationns.add(prop);
		
		sum = 0L;
		List<Object> monthVsMonthGrowth = new ArrayList<Object>();
		for(String department : departments) {
			if(!kpiDepartmentHeaders.contains(department) && !department.toLowerCase().contains("return")) {
				kpiDepartmentHeaders.add(department);
			}
			if(!department.toLowerCase().contains("return") && !department.equals("Total Payments")) {
				long value = simpleeTransactionDAO.countUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter) - simpleeTransactionDAO.countUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter); 
				sum += value;
				monthVsMonthGrowth.add(value);
			}
		}
		prop = new Properties();
		monthVsMonthGrowth.add(0, "Month/Month Growth $");
		monthVsMonthGrowth.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowth);
		kpiDepartmentInformationns.add(prop);
		
		sum = 0L;
		Double percent = 0d;
		List<Object> monthVsMonthGrowthPercents = new ArrayList<Object>();
		for(String department : departments) {
			if(!kpiDepartmentHeaders.contains(department) && !department.toLowerCase().contains("return")) {
				kpiDepartmentHeaders.add(department);
			}
			if(!department.toLowerCase().contains("return") && !department.equals("Total Payments")) {
				Double value = Double.parseDouble(String.valueOf(simpleeTransactionDAO.countUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter) - simpleeTransactionDAO.countUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter)));
				percent = 0d;
				try {
					percent = value / Double.parseDouble(String.valueOf(simpleeTransactionDAO.countUsingFilters(customerId, market, department, null, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter)));
					percent = percent * 100;
				} catch(ArithmeticException ae) {}
				sum += (!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0;
				monthVsMonthGrowthPercents.add((!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0);
			}
		}
		prop = new Properties();
		monthVsMonthGrowthPercents.add(0, "Month/Month Growth %");
		monthVsMonthGrowthPercents.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowthPercents);
		kpiDepartmentInformationns.add(prop);
		
		List<Properties> kpiPaymentInformationns = new ArrayList<Properties>();
		List<String> kpiPaymentHeaders = new ArrayList<String>();
		
		sum = 0L;
		months3AvgValues = new ArrayList<Object>();
		for(String payment : payments) {
			if(!kpiPaymentHeaders.contains(payment) && !payment.toLowerCase().contains("return")) {
				kpiPaymentHeaders.add(payment);
			}
			if(!payment.toLowerCase().contains("return") && !payment.equals("Total Payments")) {
				
				long value = simpleeTransactionDAO.countUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter);
				value += simpleeTransactionDAO.countUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter);
				value += simpleeTransactionDAO.countUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 3).getIndex()), monthFilter);
				value = Math.round(value / 3);
				sum += value;
				months3AvgValues.add(value);
			}
		}
		prop = new Properties();
		months3AvgValues.add(0, "3 Month Avg $");
		months3AvgValues.add(Math.round(sum));
		prop.setValues(months3AvgValues);
		kpiPaymentInformationns.add(prop);
		
		sum = 0L;
		val = 0D;
		months3AvgPercent = new ArrayList<Object>();
		for(int x = 1; x < months3AvgValues.size() - 1; x++) {
			val = 0D;
			try {
				val = Double.parseDouble(months3AvgValues.get(x).toString()) / Double.parseDouble(months3AvgValues.get(months3AvgValues.size() - 1).toString());
				val = val * 100;
			} catch(ArithmeticException ae){}
			months3AvgPercent.add(Math.round(val));
			sum += Math.round(val);
		}
		prop = new Properties();
		months3AvgPercent.add(0, "3 Month Avg %");
		months3AvgPercent.add(Math.round(sum));
		prop.setValues(months3AvgPercent);
		kpiPaymentInformationns.add(prop);
		
		sum = 0L;
		monthVsMonthGrowth = new ArrayList<Object>();
		for(String payment : payments) {
			if(!kpiPaymentHeaders.contains(payment) && !payment.toLowerCase().contains("return")) {
				kpiPaymentHeaders.add(payment);
			}
			if(!payment.toLowerCase().contains("return") && !payment.equals("Total Payments")) {
				long value = simpleeTransactionDAO.countUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter) - simpleeTransactionDAO.countUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter);
				sum += value;
				monthVsMonthGrowth.add(value);
			}
		}
		prop = new Properties();
		monthVsMonthGrowth.add(0, "Month/Month Growth $");
		monthVsMonthGrowth.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowth);
		kpiPaymentInformationns.add(prop);
		
		sum = 0L;
		percent = 0D;
		monthVsMonthGrowthPercents = new ArrayList<Object>();
		for(String payment : payments) {
			if(!kpiPaymentHeaders.contains(payment) && !payment.toLowerCase().contains("return")) {
				kpiPaymentHeaders.add(payment);
			}
			if(!payment.toLowerCase().contains("return") && !payment.equals("Total Payments")) {
				Double value = Double.parseDouble(String.valueOf(simpleeTransactionDAO.countUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 1).getIndex()), monthFilter) - simpleeTransactionDAO.countUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter)));
				percent = 0d;
				try {
					percent = value / Double.parseDouble(String.valueOf(simpleeTransactionDAO.countUsingFilters(customerId, market, null, payment, String.valueOf(months.get(months.size() - 2).getIndex()), monthFilter)));
					percent = percent * 100;
				} catch(ArithmeticException ae) {}
				sum += (!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0;
				monthVsMonthGrowthPercents.add((!Double.isNaN(percent) && !Double.isInfinite(percent)) ? Math.round(percent) : 0);
			}
		}
		prop = new Properties();
		monthVsMonthGrowthPercents.add(0, "Month/Month Growth %");
		monthVsMonthGrowthPercents.add(Math.round(sum));
		prop.setValues(monthVsMonthGrowthPercents);
		kpiPaymentInformationns.add(prop);
		
		cachedData.put("title", "Transactions - By Department");
		cachedData.put("paymentTitle", "Transactions - By Payment Methods");
		cachedData.put("kpiDepartmentTitle", "KPI's - Transactions Collected By Departments");
		cachedData.put("kpiPaymentTitle", "KPI's - Transactions Collected By Payment Methods");
		
		cachedData.put("headers", departments);
		cachedData.put("paymentHeaders", payments);
		cachedData.put("kpiDepartmentHeaders", kpiDepartmentHeaders);
		cachedData.put("kpiPaymentHeaders", kpiPaymentHeaders);
		
		cachedData.put("informations", months);
		cachedData.put("paymentInformationns", paymentMonths);
		cachedData.put("kpiDepartmentInformationns", kpiDepartmentInformationns);
		cachedData.put("kpiPaymentInformationns", kpiPaymentInformationns);
		
		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsmarketviewbydepartmenttransactionssimplee");
		cachedData.put("apiName", "cdetailsmarketviewbydepartmenttransactionssimplee");
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("markets", markets);
		cachedData.put("market", market);
		
		cachedData.put("CDETAILS_MARKET_VIEW_BY_DEPARTMENT_TRANSACTIONS_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsmarkettransactioncomparisonsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		fieldMap.put("type", "MARKET$COMPARISONANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKET$COMPARISONANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		List<Properties> headers = Utility.getInstance().monthHeaders(monthFilter);
		
		cachedData.put("headers", headers);
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsmarkettransactioncomparisonsimplee");
		cachedData.put("apiName", "cdetailsmarkettransactioncomparisonsimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Patient Collections Growth - Transactions Market Comparison");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_MARKET_TRANSACTION_COMPARISON_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailskpiscollectedtransactionsbymarketsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();

		fieldMap.put("type", "MARKET$COMPARISONANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKET$COMPARISONANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		List<Properties> headers = Utility.getInstance().monthHeaders(monthFilter);
		
		cachedData.put("headers", headers);
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailskpiscollectedtransactionsbymarketsimplee");
		cachedData.put("apiName", "cdetailskpiscollectedtransactionsbymarketsimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "KPI's - Transactions Collected By Market");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_KPIS_COLLECTED_TRANSACTIONS_BY_MARKET_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailseligibilityusagebymarketsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
	
		fieldMap.put("type", "ELIGIBILITYUSAGENANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "ELIGIBILITYUSAGENANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailseligibilityusagebymarketsimplee");
		cachedData.put("apiName", "cdetailseligibilityusagebymarketsimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Eligibility Usage By Markets");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_ELIGIBILITY_USAGE_BY_MARKET_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsmarketavgpaymentsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending) throws Exception {
		 
		SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();

		fieldMap.put("type", "MARKET$COMPARISONANALYSIS");
		fieldMap.put("customerId", customerId);
		fieldMap.put("mmyy", monthFilter);
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "market";
			ascending = true;
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = simpleeInformationDAO.countByFilters(customerId, "MARKET$COMPARISONANALYSIS", monthFilter);
		
		double k = (double) totalCount / limit;
		int pageCount = (int) Math.ceil(k);
		java.util.List<Integer> pager = Utility.getInstance().calculatePager(pageCount, page, numTabsToShown);
		
		List<Properties> headers = Utility.getInstance().monthHeaders(monthFilter);
		
		cachedData.put("headers", headers);
		cachedData.put("pageCount", pageCount);
		cachedData.put("numTabsToShown", numTabsToShown);
		cachedData.put("limit", limit);
		cachedData.put("pager", pager);
		cachedData.put("isPagerActive", ((pageCount > 1) ? 1 : null));
		cachedData.put("count", totalCount);
		cachedData.put("currentPage", page);
		if ((page - 1) > 0) {
			cachedData.put("prev", (page - 1));
		}
		if ((page + 1) <= pageCount) {
			cachedData.put("next", (page + 1));
		}

		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "cdetailsmarketavgpaymentsimplee");
		cachedData.put("apiName", "cdetailsmarketavgpaymentsimplee");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("monthFilter", monthFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerId", customerId);
		cachedData.put("title", "Patient Collections - $ Average Payment");
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		
		cachedData.put("CDETAILS_MARKET_AVG_PAYMENT_SIMPLEE", "yes");
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsallmarketsamountbydepartmentgraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, String market, Integer cacheHours) throws Exception {
		
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeTransactionDAO simpleeTransactionDAO = new SimpleeTransactionDAO();
			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			List<String> departments = simpleeTransactionDAO.getDistinctDepartments(customerId, market, monthFilter);
			Collections.sort(departments);
			
			List<Properties> months = Utility.getInstance().monthHeaders(monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			
			for(Properties month : months) {
				xAxis.add(month.getValue());
			}
			
			for(String department : departments) {
				List<Object> data = new ArrayList<Object>();
				Series series = new Series();
				series.setName(department);
				for(Properties month : months) {
					Data d = new Data();
					d.setY(Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, department, null, String.valueOf(month.getIndex()), monthFilter)));
					data.add(d);
				}
				series.setData(data);
				seriesList.add(series);
			}
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("Patient Collections - $ By Departments (All Markets)"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("$ Collection")));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsallmarketsamountbypaymentmethodgraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, String market, Integer cacheHours) throws Exception {
		
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeTransactionDAO simpleeTransactionDAO = new SimpleeTransactionDAO();
			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			List<String> payments = simpleeTransactionDAO.getDistinctPaymentMethods(customerId, market, monthFilter);
			Collections.sort(payments);
			
			List<Properties> months = Utility.getInstance().monthHeaders(monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			
			for(Properties month : months) {
				xAxis.add(month.getValue());
			}
			for(String payment : payments) {
				List<Object> data = new ArrayList<Object>();
				Series series = new Series();
				series.setName(payment);
				for(Properties month : months) {
					Data d = new Data();
					d.setY(Math.round(simpleeTransactionDAO.getAmountSumUsingFilters(customerId, null, null, payment, String.valueOf(month.getIndex()), monthFilter)));
					data.add(d);
				}
				series.setData(data);
				seriesList.add(series);
			}
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("Patient Collections - $ By Payment Methods (All Markets)"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("$ Collection")));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}

	public Map<String, Object> cdetailsbillsdunningcyclegraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, String market, Integer cacheHours) throws Exception {
	
		/*PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {*/
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();
			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			Map<String, Object> fieldMap = new HashMap<String, Object>();
			fieldMap.clear();
			fieldMap.put("type", "DUNNINGANALYSIS");
			fieldMap.put("market", market);
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			
			SimpleeInformation simpleeInformation = simpleeInformationDAO.findOne(fieldMap);
			if(simpleeInformation != null) {
				List<Object> xAxis = new ArrayList<Object>();
				xAxis.add("No Dunning");
				xAxis.add("S1");
				xAxis.add("S2");
				xAxis.add("C1");
				xAxis.add("C2");
				xAxis.add("P1");
				xAxis.add("P2");
				xAxis.add("P1M");
				xAxis.add("P2M");
				xAxis.add("C1M");
				xAxis.add("C2M");
				
				List<Object> data = new ArrayList<Object>();
				
				data.add(new Data(null, simpleeInformation.getNoDunning()));
				data.add(new Data(null, simpleeInformation.getS1()));
				data.add(new Data(null, simpleeInformation.getS2()));
				data.add(new Data(null, simpleeInformation.getC1()));
				data.add(new Data(null, simpleeInformation.getC2()));
				data.add(new Data(null, simpleeInformation.getP1()));
				data.add(new Data(null, simpleeInformation.getP2()));
				data.add(new Data(null, simpleeInformation.getP1m()));
				data.add(new Data(null, simpleeInformation.getP2m()));
				data.add(new Data(null, simpleeInformation.getC1m()));
				data.add(new Data(null, simpleeInformation.getC2m()));
	           
				Series series = new Series();
				series.setName("Cycles");
				series.setShowInLegend(false);
	            series.setData(data);
	           				
				List<Series> seriesList = new ArrayList<Series>();
				seriesList.add(series);
				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
				highchart.setTitle(new Title("Bills Dunning Cycle - Status"));
				highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
				
				List<XAxis> xAxisList = new ArrayList<XAxis>();
				xAxisList.add(new XAxis(xAxis));
				highchart.setxAxis(xAxisList);
				
				List<YAxis> yAxisList = new ArrayList<YAxis>();
				yAxisList.add(new YAxis(new Title("Counts")));
				highchart.setyAxis(yAxisList);
				
				highchart.setSeries(seriesList);
				
				cachedData.put("highchart", highchart);
			}
			return cachedData;
				/*PodDataCache newCache = new PodDataCache();
				newCache.setAccountId(account.getId());
				newCache.setPodId(podId);
				newCache.setParamHash(paramHash);
				newCache.setData(cachedData);
				newCache.setCreatedAt(new Date());

				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				fieldMap.put("podId", podId);
				fieldMap.put("paramHash", paramHash);
				cacheDAO.delete(fieldMap);
				newCache = cacheDAO.insert(newCache);
				return newCache.getData();
			}
		}
		return cache.getData();*/
	}	
	
	public Map<String, Object> cdetailspaperstatementsgraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			fieldMap.clear();
			fieldMap.put("type", "PAPERANALYSIS");
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			List<Object> data = new ArrayList<Object>();

			List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
			
			for(SimpleeInformation simpleeInformation : informations){

				xAxis.add(simpleeInformation.getMarket());
				data.add(new Data(null, simpleeInformation.getPapers()));
				
			}
			
			Series series = new Series();
			series.setName("Papers");
			series.setColor("#F7A74C");
			series.setShowInLegend(false);
			series.setData(data);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("Papers Sent"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Counts")));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailseligibilityusagebymarketgraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			fieldMap.clear();
			fieldMap.put("type", "ELIGIBILITYUSAGENANALYSIS");
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			List<Object> data = new ArrayList<Object>();

			List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
			
			for(SimpleeInformation simpleeInformation : informations){

				xAxis.add(simpleeInformation.getMarket());
				data.add(new Data(null, simpleeInformation.getEligibilityTransactions()));
				
			}
			
			Series series = new Series();
			series.setName("Eligibility");
			series.setColor("#91CC68");
			series.setShowInLegend(false);
			series.setData(data);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("Eligibility Usage"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Counts")));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsmarketavgpaymentgraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			fieldMap.clear();
			fieldMap.put("type", "MARKET$COMPARISONANALYSIS");
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			List<Object> data = new ArrayList<Object>();

			List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
			
			for(SimpleeInformation simpleeInformation : informations){
				xAxis.add(simpleeInformation.getMarket());
				Double v = Double.parseDouble(simpleeInformation.getAvgColumns().get(simpleeInformation.getAvgColumns().size()-1).toString());
				data.add(new Data(null, v));			
			}
			
			Series series = new Series();
			series.setName("Patient Collection");
			series.setColor("#FCE060");
			series.setShowInLegend(false);
			series.setData(data);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("Patient Collections - $ Average Payment"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Counts"), true));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsmarkettransactioncomparisongraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			fieldMap.clear();
			fieldMap.put("type", "MARKET$COMPARISONANALYSIS");
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			List<Object> data = new ArrayList<Object>();

			List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
			
			for(SimpleeInformation simpleeInformation : informations){
				xAxis.add(simpleeInformation.getMarket());
				/*Double sum = 0d;
				for(Object d : simpleeInformation.getAvgColumns()){
					sum += Double.parseDouble(d.toString());
				}*/
				Double v = Double.parseDouble(simpleeInformation.getTransactionColumns().get(simpleeInformation.getTransactionColumns().size()-1).toString());
				data.add(new Data(null, v.longValue()));			
			}
			
			Series series = new Series();
			series.setName("Patient Collection");
			series.setColor("#F7A74C");
			series.setShowInLegend(false);
			series.setData(data);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("Patient Collections Growth - Transactions Market Comparison"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Counts")));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsmarketamountcomparisongraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			fieldMap.clear();
			fieldMap.put("type", "MARKET$COMPARISONANALYSIS");
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			List<Object> data = new ArrayList<Object>();

			List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
			
			for(SimpleeInformation simpleeInformation : informations){
				xAxis.add(simpleeInformation.getMarket());
				/*Double sum = 0d;
				for(Object d : simpleeInformation.getAvgColumns()){
					sum += Double.parseDouble(d.toString());
				}*/
				Double v = Double.parseDouble(simpleeInformation.getColumns().get(simpleeInformation.getColumns().size()-1).toString());
				data.add(new Data(null, v.longValue(), "/customerdetails/" + customerId + "?dl=2&ddt=smlcollectiondrilldown&market=" + simpleeInformation.getMarket() + "#MARKET_VIEW_TAB"));			
			}
			
			Series series = new Series();
			series.setName("Patient Collection");
			series.setColor("#1CAF9A");
			series.setShowInLegend(false);
			series.setData(data);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("Patient Collections Growth - $ Market Comparison"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25, new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Collections")));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailskpiscollectedamountbymarketgraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			fieldMap.clear();
			fieldMap.put("type", "MARKET$COMPARISONANALYSIS");
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			List<Object> data = new ArrayList<Object>();

			List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
			
			for(SimpleeInformation simpleeInformation : informations){

				xAxis.add(simpleeInformation.getMarket());
				data.add(new Data(null, simpleeInformation.getKpiMonthVsMonthGrowthPercent()));
				
			}
			Series series = new Series();
			series.setName("KPI's");
			series.setColor("#F7A74C");
			series.setShowInLegend(false);
			series.setData(data);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("KPI's - $ Collected By Market"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Growth %"), new Labels("{value}%", null)));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailskpiscollectedtransactionsbymarketgraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		 
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			fieldMap.clear();
			fieldMap.put("type", "MARKET$COMPARISONANALYSIS");
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			List<Object> data = new ArrayList<Object>();

			List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
			
			for(SimpleeInformation simpleeInformation : informations){

				xAxis.add(simpleeInformation.getMarket());
				data.add(new Data(null, simpleeInformation.getKpiMonthVsMonthGrowthTransactionPercent()));
				
			}
			//kpiMonthVsMonthGrowthTransactionPercent
			Series series = new Series();
			series.setName("KPI's");
			series.setColor("#0073BF");
			series.setShowInLegend(false);
			series.setData(data);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("KPI's - Transactions Collected By Market"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Growth %"), new Labels("{value}%", null)));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsallemailssentgraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		 
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			fieldMap.clear();
			fieldMap.put("type", "MARKETANALYSIS");
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			List<Object> data = new ArrayList<Object>();

			List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
			
			for(SimpleeInformation simpleeInformation : informations){

				xAxis.add(simpleeInformation.getMarket());
				data.add(new Data(null, simpleeInformation.getAllSend()));
				
			}
			
			Series series = new Series();
			series.setName("Sent");
			series.setColor("#1CAF9A");
			series.setShowInLegend(false);
			series.setData(data);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("All Emails - Sent"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Counts")));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsallemailsopenrategraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		 
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			fieldMap.clear();
			fieldMap.put("type", "MARKETANALYSIS");
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			List<Object> data = new ArrayList<Object>();

			List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
			
			for(SimpleeInformation simpleeInformation : informations){

				xAxis.add(simpleeInformation.getMarket());
				data.add(new Data(null, simpleeInformation.getAllOpenRate()));
				
			}
			
			Series series = new Series();
			series.setName("Open Rate");
			series.setColor("#F7A74C");
			series.setShowInLegend(false);
			series.setData(data);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("All Emails - Open Rate"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Rate"), new Labels("{value}%", null)));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsallemailsclickthroughrategraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		 
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			fieldMap.clear();
			fieldMap.put("type", "MARKETANALYSIS");
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			List<Object> data = new ArrayList<Object>();

			List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
			
			for(SimpleeInformation simpleeInformation : informations){

				xAxis.add(simpleeInformation.getMarket());
				data.add(new Data(null, simpleeInformation.getAllClickThroughRate()));
				
			}
			
			Series series = new Series();
			series.setName("Click Through Rate");
			series.setColor("#0073BF");
			series.setShowInLegend(false);
			series.setData(data);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("All Emails - Click Through Rate"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Rate"), new Labels("{value}%", null)));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsallemailsmonthseriessentgraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		 
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			List<Properties> monthFilters = Utility.getInstance().monthFilters();
			
			List<String> markets = new ArrayList<String>();
			
			List<Series> seriesList = new ArrayList<Series>();
			
			for(Properties mFilter : monthFilters) {
				
				fieldMap.clear();
				fieldMap.put("type", "MARKETANALYSIS");
				fieldMap.put("customerId", customerId);
				fieldMap.put("mmyy", mFilter.getValue().toString());
				List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
				
				List<Object> data = new ArrayList<Object>();
				for(SimpleeInformation information : informations) {
					data.add(new Data(null, information.getAllSend()));
					
					if(!markets.contains(information.getMarket())) {
						markets.add(information.getMarket());
					}
				}
				Series series = new Series();
				series.setName(mFilter.getValue().toString());
				series.setData(data);
				seriesList.add(series);
			}
			
			List<Object> xAxis = new ArrayList<Object>();
			xAxis.addAll(markets);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("Monthly Status All Emails - Sent"));
			highchart.setPlotOptions(new PlotOptions(new Column("normal", 40)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Counts")));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsallemailsmonthseriesopenrategraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		 
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			List<Properties> monthFilters = Utility.getInstance().monthFilters();
			
			List<String> markets = new ArrayList<String>();
			
			List<Series> seriesList = new ArrayList<Series>();
			
			for(Properties mFilter : monthFilters) {
				
				fieldMap.clear();
				fieldMap.put("type", "MARKETANALYSIS");
				fieldMap.put("customerId", customerId);
				fieldMap.put("mmyy", mFilter.getValue().toString());
				List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
				
				List<Object> data = new ArrayList<Object>();
				for(SimpleeInformation information : informations) {
					data.add(new Data(null, information.getAllOpenRate()));
					
					if(!markets.contains(information.getMarket())) {
						markets.add(information.getMarket());
					}
				}
				Series series = new Series();
				series.setName(mFilter.getValue().toString());
				series.setData(data);
				seriesList.add(series);
			}
			
			List<Object> xAxis = new ArrayList<Object>();
			xAxis.addAll(markets);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("Monthly Status All Emails - Open Rate"));
			highchart.setPlotOptions(new PlotOptions(new Column("normal", 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Rate"), new Labels("{value}%", null)));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsallemailsmonthseriesclickthroughrategraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		 
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			List<Properties> monthFilters = Utility.getInstance().monthFilters();
			
			List<String> markets = new ArrayList<String>();
			
			List<Series> seriesList = new ArrayList<Series>();
			
			for(Properties mFilter : monthFilters) {
				
				fieldMap.clear();
				fieldMap.put("type", "MARKETANALYSIS");
				fieldMap.put("customerId", customerId);
				fieldMap.put("mmyy", mFilter.getValue().toString());
				List<SimpleeInformation> informations = simpleeInformationDAO.findAll(fieldMap, "market", true);
				
				List<Object> data = new ArrayList<Object>();
				for(SimpleeInformation information : informations) {
					data.add(new Data(null, information.getAllClickThroughRate()));
					
					if(!markets.contains(information.getMarket())) {
						markets.add(information.getMarket());
					}
				}
				Series series = new Series();
				series.setName(mFilter.getValue().toString());
				series.setData(data);
				seriesList.add(series);
			}
			
			List<Object> xAxis = new ArrayList<Object>();
			xAxis.addAll(markets);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", new Options3d(true, 10, 15, 70)));
			highchart.setTitle(new Title("Monthly Status All Emails - Click Through Rate"));
			highchart.setPlotOptions(new PlotOptions(new Column("normal", 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-45, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Rate"), new Labels("{value}%", null)));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsallemailsmonthseriesgraphsimplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		 
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();
		
		ObjectMapper om = new ObjectMapper();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();
			SimpleeCustomerDataDAO simpleeCustomerDataDAO = new SimpleeCustomerDataDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			List<String> headers = new ArrayList<String>();
			headers.add("Market");
			headers.add("Sent");
			headers.add("Open Rate");
			headers.add("Click Through Rate");
			
			List<Properties> monthFilters = Utility.getInstance().monthFilters();
			List<String> markets = simpleeCustomerDataDAO.getDistinctMarkets(customerId, monthFilter);
			Collections.sort(markets);
			
			List<Object> data1 = null;
			List<Object> data2 = null;
			List<Object> data3 = null;
			
			List<Properties> sparklines = new ArrayList<Properties>();
			for(String market : markets) {
				data1 = new ArrayList<Object>();
				data2 = new ArrayList<Object>();
				data3 = new ArrayList<Object>();
				
				for(Properties mFilter : monthFilters) {
					fieldMap.clear();
					fieldMap.put("type", "MARKETANALYSIS");
					fieldMap.put("customerId", customerId);
					fieldMap.put("market", market);
					fieldMap.put("mmyy", mFilter.getValue().toString());
					SimpleeInformation si = simpleeInformationDAO.findOne(fieldMap);
					
					Data d1 = new Data();
					d1.setName(mFilter.getValue().toString());
					
					Data d2 = new Data();
					d2.setName(mFilter.getValue().toString());
					
					Data d3 = new Data();
					d3.setName(mFilter.getValue().toString());
					
					if(si != null) {
						d1.setY(si.getAllSend());
						d2.setY(si.getAllOpenRate());
						d3.setY(si.getAllClickThroughRate());
					} else {
						d1.setY(0);
						d2.setY(0);
						d3.setY(0);
					}
					
					data1.add(d1);
					data2.add(d2);
					data3.add(d3);
				}
				
				Properties sparkline = new Properties();
				sparkline.setKey(market);
				sparkline.setData1(data1);
				sparkline.setData2(data2);
				sparkline.setData3(data3);
				
				sparkline.setJson1(om.writeValueAsString(data1));
				sparkline.setJson2(om.writeValueAsString(data2));
				sparkline.setJson3(om.writeValueAsString(data3));
				
				sparklines.add(sparkline);
			}
			
			cachedData.put("title", "All Emails - Month Over Month Progress");
			cachedData.put("headers", headers);
			cachedData.put("sparklines", sparklines);
			cachedData.put("CDETAILS_ALL_EMAILS_MONTH_SERIES_GRAPH_SIMPLEE", "yes");
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsmarketamountcomparisongraphlevel2simplee(Account account, PodConfiguration pc, String podId, String rangeFilter, String monthFilter, String customerId, String market, int page, String sortBy, boolean ascending, Integer cacheHours) throws Exception {
		
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(monthFilter)) {
			params += "&monthFilter=" + monthFilter; 
		}
		if(!StringHelper.isEmpty(customerId)) {
			params += "&customerId=" + customerId;
		}
		if(!StringHelper.isEmpty(market)) {
			params += "&market=" + market;
		}
		String paramHash = CryptoHelper.getMD5Hash(params);
		
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);
		
		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.HOUR, cacheHours);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {
			SimpleeInformationDAO simpleeInformationDAO = new SimpleeInformationDAO();

			Map<String, Object> cachedData = new HashMap<String, Object>();
			
			fieldMap.clear();
			fieldMap.put("type", "MARKET$COMPARISONANALYSIS");
			fieldMap.put("customerId", customerId);
			fieldMap.put("mmyy", monthFilter);
			fieldMap.put("market", market);
			
			logger.debug("market: " + market);
			
			List<Object> xAxis = new ArrayList<Object>();
			List<Series> seriesList = new ArrayList<Series>();
			List<Object> data = new ArrayList<Object>();

			SimpleeInformation information = simpleeInformationDAO.findOne(fieldMap);
			if(information != null) {
				data.addAll(information.getColumns());
				
				for(Properties header : information.getHeaders()) {
					xAxis.add(header.getValue());
				}
			}
			
			Series series = new Series();
			series.setName("Patient Collection");
			series.setColor("#1CAF9A");
			series.setShowInLegend(false);
			series.setData(data);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("column", new Options3d(true, 0, 0, 70)));
			highchart.setTitle(new Title(market + " - $ Market Comparison"));
			highchart.setPlotOptions(new PlotOptions(new Column(null, 25)));
			
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(0, 0)));
			highchart.setxAxis(xAxisList);
			
			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Collections")));
			highchart.setyAxis(yAxisList);
			
			highchart.setSeries(seriesList);
			
			cachedData.put("highchart", highchart);
			
			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(cachedData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
		}
		return cache.getData();
	}
}
