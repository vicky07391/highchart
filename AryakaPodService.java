package com.crucialbits.cy.custom.service;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.crucialbits.chart.highchart.Chart;
import com.crucialbits.chart.highchart.Column;
import com.crucialbits.chart.highchart.Data;
import com.crucialbits.chart.highchart.DataLabels;
import com.crucialbits.chart.highchart.DateTimeLabelFormats;
import com.crucialbits.chart.highchart.Events;
import com.crucialbits.chart.highchart.Highchart;
import com.crucialbits.chart.highchart.Labels;
import com.crucialbits.chart.highchart.Marker;
import com.crucialbits.chart.highchart.Options3d;
import com.crucialbits.chart.highchart.Pie;
import com.crucialbits.chart.highchart.PlotOptions;
import com.crucialbits.chart.highchart.Point;
import com.crucialbits.chart.highchart.Series;
import com.crucialbits.chart.highchart.Spline;
import com.crucialbits.chart.highchart.StackLabels;
import com.crucialbits.chart.highchart.Style;
import com.crucialbits.chart.highchart.Subtitle;
import com.crucialbits.chart.highchart.Title;
import com.crucialbits.chart.highchart.XAxis;
import com.crucialbits.chart.highchart.YAxis;
import com.crucialbits.cy.app.Constants;
import com.crucialbits.cy.app.Utility;
import com.crucialbits.cy.app.Constants.IssuesSortBy;
import com.crucialbits.cy.dao.CustomerDAO;
import com.crucialbits.cy.dao.CustomerTypeDAO;
import com.crucialbits.cy.dao.InformationDAO;
import com.crucialbits.cy.dao.LicenseDAO;
import com.crucialbits.cy.dao.LicenseTimeseriesByDayDAO;
import com.crucialbits.cy.dao.PodDataCacheDAO;
import com.crucialbits.cy.dao.SiteTimeseriesByDayDAO;
import com.crucialbits.cy.dao.TimeseriesByDayDAO;
import com.crucialbits.cy.model.Account;
import com.crucialbits.cy.model.Customer;
import com.crucialbits.cy.model.CustomerType;
import com.crucialbits.cy.model.Field;
import com.crucialbits.cy.model.FieldGroup;
import com.crucialbits.cy.model.Information;
import com.crucialbits.cy.model.License;
import com.crucialbits.cy.model.LicenseTimeseriesByDay;
import com.crucialbits.cy.model.PodConfiguration;
import com.crucialbits.cy.model.PodDataCache;
import com.crucialbits.cy.model.Site;
import com.crucialbits.cy.model.SiteTimeseriesByDay;
import com.crucialbits.cy.model.TimeseriesByDay;
import com.crucialbits.cy.service.AdminService;
import com.crucialbits.cy.service.IssueService;
import com.crucialbits.cy.service.ZendeskService;
import com.crucialbits.util.CryptoHelper;
import com.crucialbits.util.NumberHelper;
import com.crucialbits.util.StringHelper;

public class AryakaPodService implements Constants {

	private final static Logger logger = Logger.getLogger(AryakaPodService.class);

	public Map<String, Object> c360customerbyproducttype(
			Account account, String podId, String ignored, String partner, String accountManager, String csm, String cem, String customerType, Integer cacheHours) throws Exception {

		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(accountManager)) {
			params += "&accountManager=" + accountManager;
		}
		if(!StringHelper.isEmpty(csm)) {
			params += "&csm=" + csm;
		}
		if(!StringHelper.isEmpty(cem)) {
			params += "&cem=" + cem;
		}
		if(!StringHelper.isEmpty(customerType)) {
			params += "&customerType=" + customerType;
		}
		if(!StringHelper.isEmpty(partner)) {
			params += "&partner=" + partner;
		}
		if(!StringHelper.isEmpty(ignored)) {
			params += "&ignored=" + ignored;
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

			CustomerDAO customerDAO = new CustomerDAO();
			Set<Object> CRMs = customerDAO.getDistinctCustomFieldValues(account.getId(), "product_type");

			Map<String, Long> dataMap = new HashMap<String, Long>();
			Map<String, List<Customer>> countsMap = new LinkedHashMap<String, List<Customer>>();
			for(Object CRM : CRMs) {
				dataMap.put(CRM.toString(), 0l);
				countsMap.put(CRM.toString(), new ArrayList<Customer>());
			}

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("ignored", ignored);
			if(!StringHelper.isEmpty(accountManager)) {
				fieldMap.put("accountManager", accountManager);
			}
			if(!StringHelper.isEmpty(csm)) {
				fieldMap.put("csm", csm);
			}
			if(!StringHelper.isEmpty(cem)) {
				fieldMap.put("cem", cem);
			}
			if(!StringHelper.isEmpty(customerType)) {
				fieldMap.put("customerTypeId", customerType);
			}
			if(!StringHelper.isEmpty(partner)) {
				fieldMap.put("partnerId", partner);
			}
			List<Customer> customers = customerDAO.findAll(fieldMap);

			for(Customer customer : customers) {

				if(customer.getFieldGroups() != null) {
					for(FieldGroup fg : customer.getFieldGroups()) {
						if(fg.getFields() != null) {
							for(com.crucialbits.cy.model.Field field : fg.getFields()) {
								if(field.getName().equalsIgnoreCase("product_type") && field.getResponse() != null) {
									dataMap.put(field.getResponse().toString(), dataMap.get(field.getResponse().toString()) + 1);
									List<Customer> cls = countsMap.get(field.getResponse().toString());
									customer.setEventsData(null);
									cls.add(customer);
									countsMap.put(field.getResponse().toString(), cls);
								}
							}
						}
					}
				}

			}

			List<Object> data = new ArrayList<Object>();
			List<Object> xAxis = new ArrayList<Object>();

			for(Map.Entry<String, Long> entry : Utility.getInstance().sortByValues(dataMap).entrySet()) {
				Data d = new Data();
				d.setY(entry.getValue());
				String name = entry.getKey().toString();
				if(name.length() > 15) {
					name = name.substring(0, 15) + "...";
				}
				d.setName(name);
				String url = "/managecustomers?c__product_type=" + entry.getKey();
				if(!StringHelper.isEmpty(accountManager)) {
					url += "&accountmanager=" + accountManager;
				}
				if(!StringHelper.isEmpty(csm)) {
					url += ((url.contains("?")) ? "&" : "?") + "csm=" + csm;
				}
				if(!StringHelper.isEmpty(cem)) {
					url += ((url.contains("?")) ? "&" : "?") + "cem=" + cem;
				}
				d.setUrl(url);
				xAxis.add(entry.getKey());
				data.add(d);
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(false);
			seriesList.add(series);

			/*series = new Series();
			series.setType("spline");
			series.setName("Average");
			series.setyAxis(1l);
			series.setShowInLegend(true);

			data = new ArrayList<Object>();
			for(Map.Entry<String, Long> entry : dataMap.entrySet()) {
				Data d = new Data();
				d.setName(entry.getKey());
				List<Customer> cls = countsMap.get(entry.getKey());
				int activeUsers = 0;
				for(Customer cl : cls) {
					if(cl.getEventsData() != null) {
						activeUsers += Integer.parseInt(cl.getEventsData().get("activeUsers").toString());
					}
				}
				d.setY((entry.getValue() == 0) ? 0 : (activeUsers / entry.getValue()));
				data.add(d);
			}

			series.setData(data);
			seriesList.add(series);*/

			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by product type"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
//					yAxisList.add(new YAxis(new Title("Average"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, "#AAA"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));

			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
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

	public Map<String, Object> c360customerbybillingcountry(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				params += "&" + mp.getKey() + "=" + mp.getValue(); 
			}
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
             
//			logger.debug("=====Cache null====");
			CustomerDAO customerDAO = new CustomerDAO();
            List<String> CRMs = customerDAO.getDistinctCountries(account.getId(), paramMap);
// 			logger.debug("=========CRM SIZE=============\t" + CRMs.size());
            Map<String, Long> dataMap = new HashMap<String, Long>();
			Map<String, List<Customer>> countsMap = new LinkedHashMap<String, List<Customer>>();
			for(String CRM : CRMs) {
				if(!StringHelper.isEmpty(CRM)) {
				dataMap.put(CRM.toLowerCase().trim(), 0l);
				countsMap.put(CRM.toLowerCase().trim(), new ArrayList<Customer>());
				}
			}
			

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());					
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			
			List<Customer> customers = customerDAO.findAll(fieldMap);
			logger.debug("customerSize\t" + customers.size());
			for(Customer customer : customers) {
				if(!StringHelper.isEmpty(customer.getCountry()) && dataMap.containsKey(customer.getCountry().toLowerCase().trim())) {
					List<Customer> cls = countsMap.get(customer.getCountry().toLowerCase().trim());
					customer.setEventsData(null);
					cls.add(customer);
					countsMap.put(customer.getCountry().toLowerCase().trim(), cls);
				}
		}
            
			List<Object> data = new ArrayList<Object>();
			List<Object> xAxis = new ArrayList<Object>();

			for(Map.Entry<String, Long> entry : Utility.getInstance().sortByValues(dataMap).entrySet()) {
				Data d = new Data();
				d.setY(entry.getValue());
				String name = entry.getKey().toString();
				if(name.length() > 15) {
					name = name.substring(0, 15) + "...";
				}
				d.setName(name);
				/*String url = "/managecustomers?country=" + entry.getKey();			
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {					
						url += "&"+mp.getKey()+"=" + mp.getValue();							
					}
				}*/
				
				d.setUrl(null);
				xAxis.add(entry.getKey());
				data.add(d);
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(false);
			seriesList.add(series);

			/*series = new Series();
			series.setType("spline");
			series.setName("Average");
			series.setyAxis(1l);
			series.setShowInLegend(true);

			data = new ArrayList<Object>();
			for(Map.Entry<String, Long> entry : dataMap.entrySet()) {
				Data d = new Data();
				d.setName(entry.getKey());
				List<Customer> cls = countsMap.get(entry.getKey());
				int activeUsers = 0;
				for(Customer cl : cls) {
					if(cl.getEventsData() != null) {
						activeUsers += Integer.parseInt(cl.getEventsData().get("activeUsers").toString());
					}
				}
				d.setY((entry.getValue() == 0) ? 0 : (activeUsers / entry.getValue()));
				data.add(d);
			}

			series.setData(data);
			seriesList.add(series);*/

			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by billing country"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
//					yAxisList.add(new YAxis(new Title("Average"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, "#AAA"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));

			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
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

	public Map<String, Object> c360customerbycontractperiod(
			Account account, String podId,  String ignored, String partner, String accountManager, String csm, String cem, String customerType, Integer cacheHours) throws Exception {

		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(accountManager)) {
			params += "&accountManager=" + accountManager;
		}
		if(!StringHelper.isEmpty(csm)) {
			params += "&csm=" + csm;
		}
		if(!StringHelper.isEmpty(cem)) {
			params += "&cem=" + cem;
		}
		if(!StringHelper.isEmpty(customerType)) {
			params += "&customerType=" + customerType;
		}
		if(!StringHelper.isEmpty(partner)) {
			params += "&partner=" + partner;
		}
		if(!StringHelper.isEmpty(ignored)) {
			params += "&ignored=" + ignored;
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

			CustomerDAO customerDAO = new CustomerDAO();
			LicenseDAO licenseDAO = new LicenseDAO();

			List<Integer> terms = licenseDAO.getDistinctCurrentTerms(account.getId(), null);
			Collections.sort(terms);

			Map<Integer, Long> dataMap = new LinkedHashMap<Integer, Long>();
			Map<Integer, List<Customer>> countsMap = new LinkedHashMap<Integer, List<Customer>>();

			for(Integer term : terms) {
				dataMap.put(term, 0l);
				countsMap.put(term, new ArrayList<Customer>());
			}

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("ignored", ignored);
			if(!StringHelper.isEmpty(accountManager)) {
				fieldMap.put("accountManager", accountManager);
			}
			if(!StringHelper.isEmpty(csm)) {
				fieldMap.put("csm", csm);
			}
			if(!StringHelper.isEmpty(cem)) {
				fieldMap.put("cem", cem);
			}
			if(!StringHelper.isEmpty(customerType)) {
				fieldMap.put("customerTypeId", customerType);
			}
			if(!StringHelper.isEmpty(partner)) {
				fieldMap.put("partnerId", partner);
			}
			Map<String, Object> lfMap = new HashMap<String, Object>();

			int skip = 0;
			int limit = 500;
			while(true) {
				List<Customer> customers = customerDAO.findAll(fieldMap, skip, limit);
				if(customers.size() == 0) {
					break;
				}

				for(Customer customer : customers) {
					lfMap.clear();
					lfMap.put("accountId", account.getId());
					lfMap.put("customerId", customer.getId());
					List<License> licenses = licenseDAO.findAll(lfMap);
					for(License license : licenses) {
						if(license.getContractPeriod() != null ) {
							dataMap.put(license.getContractPeriod(), dataMap.get(license.getContractPeriod()) + 1);
							List<Customer> cls = countsMap.get(license.getContractPeriod());
							customer.setEventsData(null);
							cls.add(customer);
							countsMap.put(license.getContractPeriod(), cls);
						}
					}
				}

				skip += limit;
			}



			List<Object> data = new ArrayList<Object>();
			List<Object> xAxis = new ArrayList<Object>();

			for(Map.Entry<Integer, Long> entry : Utility.getInstance().sortByValues(dataMap).entrySet()) {
				Data d = new Data();
				d.setY(entry.getValue());
				String name = String.valueOf(entry.getKey());
				if(name.length() > 15) {
					name = name.substring(0, 15) + "...";
				}
				d.setName(name);
				/*String url = "/managecustomers?currentterm=" + d.getName();
				if(!StringHelper.isEmpty(accountManager)) {
					url += "&accountmanager=" + accountManager;
				}*/
				d.setUrl(null);
				xAxis.add(entry.getKey());
				data.add(d);
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(false);
			seriesList.add(series);

			/*series = new Series();
			series.setType("spline");
			series.setName("Average");
			series.setyAxis(1l);
			series.setShowInLegend(true);

			data = new ArrayList<Object>();
			for(Map.Entry<String, Long> entry : dataMap.entrySet()) {
				Data d = new Data();
				d.setName(entry.getKey());
				List<Customer> cls = countsMap.get(entry.getKey());
				int activeUsers = 0;
				for(Customer cl : cls) {
					if(cl.getEventsData() != null) {
						activeUsers += Integer.parseInt(cl.getEventsData().get("activeUsers").toString());
					}
				}
				d.setY((entry.getValue() == 0) ? 0 : (activeUsers / entry.getValue()));
				data.add(d);
			}

			series.setData(data);
			seriesList.add(series);*/

			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by contract period"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true, new Title("Period (in months)")));
//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
//					yAxisList.add(new YAxis(new Title("Average"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, "#AAA"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));

			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
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

	public Map<String, Object> c360customerbybillingfrequency(
			Account account, String podId,  String ignored, String partner, String accountManager, String csm, String cem, String customerType, Integer cacheHours) throws Exception {

		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(accountManager)) {
			params += "&accountManager=" + accountManager;
		}
		if(!StringHelper.isEmpty(csm)) {
			params += "&csm=" + csm;
		}
		if(!StringHelper.isEmpty(cem)) {
			params += "&cem=" + cem;
		}
		if(!StringHelper.isEmpty(customerType)) {
			params += "&customerType=" + customerType;
		}
		if(!StringHelper.isEmpty(partner)) {
			params += "&partner=" + partner;
		}
		if(!StringHelper.isEmpty(ignored)) {
			params += "&ignored=" + ignored;
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

			CustomerDAO customerDAO = new CustomerDAO();
			LicenseDAO licenseDAO = new LicenseDAO();

			List<String> terms = licenseDAO.getDistinctBillingFrequencies(account.getId());
			Collections.sort(terms);

			Map<String, Long> dataMap = new LinkedHashMap<String, Long>();
			Map<String, List<Customer>> countsMap = new LinkedHashMap<String, List<Customer>>();
			for(String term : terms) {
				dataMap.put(term, 0l);
				countsMap.put(term, new ArrayList<Customer>());
			}

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("ignored", ignored);
			if(!StringHelper.isEmpty(accountManager)) {
				fieldMap.put("accountManager", accountManager);
			}
			if(!StringHelper.isEmpty(csm)) {
				fieldMap.put("csm", csm);
			}
			if(!StringHelper.isEmpty(cem)) {
				fieldMap.put("cem", cem);
			}
			if(!StringHelper.isEmpty(customerType)) {
				fieldMap.put("customerTypeId", customerType);
			}

			if(!StringHelper.isEmpty(partner)) {
				fieldMap.put("partnerId", partner);
			}
			Map<String, Object> lfMap = new HashMap<String, Object>();

			int skip = 0;
			int limit = 500;
			while(true) {
				List<Customer> customers = customerDAO.findAll(fieldMap, skip, limit);
				if(customers.size() == 0) {
					break;
				}

				for(Customer customer : customers) {
					lfMap.clear();
					lfMap.put("accountId", account.getId());
					lfMap.put("customerId", customer.getId());
					List<License> licenses = licenseDAO.findAll(lfMap);
					for(License license : licenses) {
						if(!StringHelper.isEmpty(license.getBillingFrequency())) {
							dataMap.put(license.getBillingFrequency(), dataMap.get(license.getBillingFrequency()) + 1);
							List<Customer> cls = countsMap.get(license.getBillingFrequency());
							customer.setEventsData(null);
							cls.add(customer);
							countsMap.put(license.getBillingFrequency(), cls);
						}
					}
				}

				skip += limit;
			}

			List<Object> data = new ArrayList<Object>();
			List<Object> xAxis = new ArrayList<Object>();

			for(Map.Entry<String, Long> entry : Utility.getInstance().sortByValues(dataMap).entrySet()) {
				Data d = new Data();
				d.setY(entry.getValue());
				String name = String.valueOf(entry.getKey());
				if(name.length() > 15) {
					name = name.substring(0, 15) + "...";
				}
				d.setName(name);
				d.setUrl(null);
				xAxis.add(entry.getKey());
				data.add(d);
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(false);
			seriesList.add(series);

			/*series = new Series();
			series.setType("spline");
			series.setName("Average");
			series.setyAxis(1l);
			series.setShowInLegend(true);

			data = new ArrayList<Object>();
			for(Map.Entry<String, Long> entry : dataMap.entrySet()) {
				Data d = new Data();
				d.setName(entry.getKey());
				List<Customer> cls = countsMap.get(entry.getKey());
				int activeUsers = 0;
				for(Customer cl : cls) {
					if(cl.getEventsData() != null) {
						activeUsers += Integer.parseInt(cl.getEventsData().get("activeUsers").toString());
					}
				}
				d.setY((entry.getValue() == 0) ? 0 : (activeUsers / entry.getValue()));
				data.add(d);
			}

			series.setData(data);
			seriesList.add(series);*/

			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by billing frequency"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true, new Title("Frequency")));
//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
//					yAxisList.add(new YAxis(new Title("Average"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, "#AAA"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));

			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
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

	public Map<String, Object> c360contractperiodrenewalsbymonthlevel1(
			Account account, PodConfiguration pc, String podId, String rangeFilter, String ignored, String partner, String accountManager, String csm, String cem, String customerType, Integer cacheHours) throws Exception {


		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		
		if(!StringHelper.isEmpty(accountManager)) {
			params += "&accountManager=" + accountManager;
		}
		if(!StringHelper.isEmpty(csm)) {
			params += "&csm=" + csm;
		}
		if(!StringHelper.isEmpty(cem)) {
			params += "&cem=" + cem;
		}
		if(!StringHelper.isEmpty(customerType)) {
			params += "&customerType=" + customerType;
		}
		if(!StringHelper.isEmpty(ignored)) {
			params += "&ignored=" + ignored;
		}
		if(!StringHelper.isEmpty(partner)) {
			params += "&partner=" + partner;
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

			CustomerDAO customerDAO = new CustomerDAO();
			//
			LicenseDAO licenseDAO = new LicenseDAO();
			//
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("ignored", ignored);
			if(!StringHelper.isEmpty(accountManager)) {
				fieldMap.put("accountManager", accountManager);
			}
			if(!StringHelper.isEmpty(csm)) {
				fieldMap.put("csm", csm);
			}
			if(!StringHelper.isEmpty(cem)) {
				fieldMap.put("cem", cem);
			}
			if(!StringHelper.isEmpty(customerType)) {
				fieldMap.put("customerTypeId", customerType);
			}
			if(!StringHelper.isEmpty(partner)) {
				fieldMap.put("partnerId", partner);
			}
			List<Customer> customers = customerDAO.findAll(fieldMap);

			List<Integer> terms = licenseDAO.getDistinctCurrentTerms(account.getId(), customerType);
			Collections.sort(terms);




			List<Object> xAxis = new ArrayList<Object>();
			SimpleDateFormat sdf = new SimpleDateFormat("MMM yyyy");

			Calendar calendar = Calendar.getInstance();

			xAxis.add(sdf.format(calendar.getTime()));
			for(int x = 0; x < 11; x++) {
				calendar.add(Calendar.MONTH, 1);
				xAxis.add(sdf.format(calendar.getTime()));
			}

			List<Series> seriesList = new ArrayList<Series>();

			Map<String, Object> lfMap = new HashMap<String, Object>();

			for(Integer term : terms) {
				Series series = new Series();
				series.setName(String.valueOf(term));


				List<Object> data = new ArrayList<Object>();

				for(Object axis : xAxis) {
					long count = 0;

					for(Customer customer : customers) {
						lfMap.clear();
						lfMap.put("accountId", account.getId());
						lfMap.put("customerId", customer.getId());
						List<License> licenses = licenseDAO.findAll(lfMap);
						for(License license : licenses) {
							if(license.getContractPeriod() != null) {
								if(license.getRenewalDate() != null && sdf.format(license.getRenewalDate()).equals(axis) && license.getContractPeriod() == term) {
									count++;
								}
							}
						}
					}

					Data d = new Data();
					d.setY(count);
					String url = "/customer360/currenttermrenewals/" + axis.toString().replace(" ", "-");
					if(!StringHelper.isEmpty(accountManager)) {
						url += ((url.contains("?")) ? "&" : "?") + "accountmanager=" + accountManager;
					}
					if(!StringHelper.isEmpty(customerType)) {
						url += ((url.contains("?")) ? "&" : "?") + "customertype=" + customerType;
					}
					if(!StringHelper.isEmpty(ignored)) {
						url += ((url.contains("?")) ? "&" : "?") + "ignored=" + ignored;
					}
					if(!StringHelper.isEmpty(csm)) {
						url += ((url.contains("?")) ? "&" : "?") + "csm=" + csm;
					}
					if(!StringHelper.isEmpty(cem)) {
						url += ((url.contains("?")) ? "&" : "?") + "cem=" + cem;
					}
					url += "#RENEWALS_TAB-tab";
					d.setUrl(url);
					data.add(d);
				}

				series.setData(data);
				seriesList.add(series);
			}

			//constructing graph object
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("column", null));
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), false));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Renewals"), 0, new StackLabels(true, new Style("#ccc"))));
			highchart.setyAxis(yAxisList);
			highchart.setTitle(new Title("Customer renewal by contract period"));
			highchart.setPlotOptions(new PlotOptions(new Column("normal", new DataLabels(false, "#fff"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));
			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			return newCache.getData();
			//end
		}

		return cache.getData();
	}

	public Map<String, Object> c360contractperiodrenewalsbymonthlevel2(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, String month, String mrrFrom, String mrrTo) throws Exception {

		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				params += "&" + mp.getKey() + "=" + mp.getValue(); 
			}
		}
		
		if(!StringHelper.isEmpty(month)) {
			params += "&month=" + month;
		}
		
		float mrrValueFrom = (!StringHelper.isEmpty(mrrFrom)) ? Float.parseFloat(mrrFrom) : 0f;
		float mrrValueTo = (!StringHelper.isEmpty(mrrTo)) ? Float.parseFloat(mrrTo) : 0f;
		params += "&mrrFrom=" + mrrValueFrom;
		params += "&mrrTo=" + mrrValueTo;

		String paramHash = CryptoHelper.getMD5Hash(params);

		Map<String, Object> fieldMap = new HashMap<String, Object>();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("paramHash", paramHash);

		Calendar cal = Calendar.getInstance();
		Date to = cal.getTime();
		cal.add(Calendar.MINUTE, -15);
		Date from = cal.getTime();

		PodDataCache cache = cacheDAO.findCacheByTime(account.getId(), podId, paramHash, from, to);
		if(cache == null) {

			CustomerDAO customerDAO = new CustomerDAO();
			LicenseDAO licenseDAO = new LicenseDAO();

			List<Object> xAxis = new ArrayList<Object>();

			SimpleDateFormat sdf = new SimpleDateFormat("MMM yyyy");
			Calendar cal1 = Calendar.getInstance();
			cal1.setTime(sdf.parse(month));

			int minDay = cal1.getActualMinimum(Calendar.DAY_OF_MONTH);
			int maxDay = cal1.getActualMaximum(Calendar.DAY_OF_MONTH);

			String minDateStr = minDay + "-" + month;
			String maxDateStr = maxDay + "-" + month;
			SimpleDateFormat sdf1 = new SimpleDateFormat("dd-MMM yyyy");

			Date minDate = sdf1.parse(minDateStr);
			Date maxDate = sdf1.parse(maxDateStr);

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			List<Customer> customers = customerDAO.findAll(fieldMap);

			List<Integer> terms = licenseDAO.getDistinctCurrentTerms(account.getId(), ((paramMap.get("customerTypes") != null) ? paramMap.get("customerTypes").toString() : null));

			Collections.sort(terms);

			for(int i = minDay; i <= maxDay; i++) {
				xAxis.add(String.valueOf(i));
			}

			List<Series> seriesList = new ArrayList<Series>();

			for(Integer term : terms) {

				Series series = new Series();
				series.setName(String.valueOf(term));
				/*if(risk.equalsIgnoreCase("new")) {
					series.setColor("#006633");
				} else if(risk.equalsIgnoreCase("low")) {
					series.setColor("#99ff66");
				} else if(risk.equalsIgnoreCase("medium")) {
					series.setColor("#ffcc00");
				} else if(risk.equalsIgnoreCase("high")) {
					series.setColor("#ff4c4c");
				} else if(risk.equalsIgnoreCase("severe")) {
					series.setColor("#b22222");
				}*/

				List<Object> data = new ArrayList<Object>();

				for(Object axis : xAxis) {
					long count = 0;

					for(Customer customer : customers) {
						if(customer.getLicenses() != null) {
							for(License l : customer.getLicenses()) {
								if(l.getRenewalDate() != null && (l.getRenewalDate().equals(minDate) || l.getRenewalDate().after(minDate))
										&& (l.getRenewalDate().equals(maxDate) || l.getRenewalDate().before(maxDate))) {
									cal1.setTime(l.getRenewalDate());
									if(Integer.parseInt(axis.toString()) == cal1.get(Calendar.DAY_OF_MONTH) && l.getContractPeriod() == term) {
										count++;
									}
								}
							}
						}


					}
					Data d = new Data();
					d.setY(count);
					data.add(d);
				}

				series.setData(data);
				seriesList.add(series);
			}

			String chartTitle = "";
			/*if(mrrValueFrom == 0) {
				chartTitle = "MRR < $750";
			} else if(mrrValueFrom == 751) {
				chartTitle = "MRR $751 - $1500";
			} else if(mrrValueFrom == 1501) {
				chartTitle = "MRR $1501 - $3000";
			} else if(mrrValueFrom == 3001) {
				chartTitle = "MRR > $3000";
			}*/

			//constructing graph object
			Highchart highchart = new Highchart();

			int step = (int) Math.ceil(xAxis.size() / 10);

			highchart.setChart(new Chart("column", null));
			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(0, step), false, new Title("Day of month")));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Renewals"), 0, new StackLabels(true, new Style("#ccc"))));
			highchart.setyAxis(yAxisList);
			highchart.setTitle(new Title(month));
			highchart.setPlotOptions(new PlotOptions(new Column("normal", new DataLabels(false, "#fff"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));
			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
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
	
	public Map<String, Object> c360customerbyaccountrating(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				params += "&" + mp.getKey() + "=" + mp.getValue(); 
			}
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

			CustomerDAO customerDAO = new CustomerDAO();
			Set<Object> CRMs = customerDAO.getDistinctCustomFieldValuesEnhanced(account.getId(), "account_rating", paramMap);
			
			Map<String, Long> dataMap = new HashMap<String, Long>();
			for(Object CRM : CRMs) {
				dataMap.put(CRM.toString(), customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "account_rating", CRM));
//				countsMap.put(CRM.toString(), new ArrayList<Customer>());
			}


			List<Object> data = new ArrayList<Object>();
			List<Object> xAxis = new ArrayList<Object>();

			for(Map.Entry<String, Long> entry : Utility.getInstance().sortByValues(dataMap).entrySet()) {
				Data d = new Data();
				d.setY(entry.getValue());
				String name = entry.getKey().toString();
				if(name.length() > 15) {
					name = name.substring(0, 15) + "...";
				}
				d.setName(name);
				String url = "/managecustomers?c__account_rating=" + entry.getKey();
				
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						url +=  "&"+mp.getKey()+"=" + mp.getValue();							
					}
				}
				d.setUrl(url);
				xAxis.add(entry.getKey());
				data.add(d);
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(false);
			seriesList.add(series);

		
			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by account rating"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
//					yAxisList.add(new YAxis(new Title("Average"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, "#AAA"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));

			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
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
	
	public Map<String, Object> c360customerbyaryakalocations(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				params += "&" + mp.getKey() + "=" + mp.getValue(); 
			}
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

			CustomerDAO customerDAO = new CustomerDAO();
			Set<Object> CRMs = customerDAO.getDistinctCustomFieldValuesEnhanced(account.getId(), "aryaka_locations", paramMap);


			Map<String, Long> dataMap = new HashMap<String, Long>();
			for(Object CRM : CRMs) {			
				dataMap.put(CRM.toString(), customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "aryaka_locations", CRM));
			}

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			

			List<Object> data = new ArrayList<Object>();
			List<Object> xAxis = new ArrayList<Object>();

		 for(Map.Entry<String, Long> entry : Utility.getInstance().sortByValues(dataMap).entrySet()) {
				if(data.size() > 20) {
					break;
				}
				Data d = new Data();
				d.setY(entry.getValue());
				String name = entry.getKey().toString();
				if(name.length() > 15) {
					name = name.substring(0, 15) + "...";
				}
				d.setName(name);
				String url = "/managecustomers?c__aryaka_locations=" + entry.getKey();
				
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						url +=  "&"+mp.getKey()+"=" + mp.getValue();							
					}
				}
				/*if(!StringHelper.isEmpty(accountManager)) {
					url += "&accountmanager=" + accountManager;
				}
				if(!StringHelper.isEmpty(csm)) {
					url += ((url.contains("?")) ? "&" : "?") + "csm=" + csm;
				}
				if(!StringHelper.isEmpty(cem)) {
					url += ((url.contains("?")) ? "&" : "?") + "cem=" + cem;
				}*/
				
				d.setUrl(url);
				xAxis.add(entry.getKey());
				data.add(d);
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(false);
			seriesList.add(series);

		
			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by aryaka locations"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
//					yAxisList.add(new YAxis(new Title("Average"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, "#AAA"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));

			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
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
	
	public Map<String, Object> c360customerbypubliccompany(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String , Object> paramMap, Integer cacheHours) throws Exception {


		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				params += "&" + mp.getKey() + "=" + mp.getValue(); 
			}
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

			
			CustomerDAO customerDAO = new CustomerDAO();
			long trueCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "public_company", "Yes");
			long falseCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "public_company", "No");

			
			List<Object> data = new ArrayList<Object>();
			Data d = new Data();
			d.setName("Yes");
			d.setY(trueCount);
			String url = "/managecustomers?c__public_company=Yes";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			/*if(!StringHelper.isEmpty(accountManager)) {
				url += ((url.contains("?")) ? "&" : "?") + "accountmanager=" + accountManager;
			}
			if(!StringHelper.isEmpty(customerType)) {
				url += ((url.contains("?")) ? "&" : "?") + "customertype=" + customerType;
			}
			if(!StringHelper.isEmpty(customerType)) {
				url += ((url.contains("?")) ? "&" : "?") + "partner=" + partner;
			}
			if(!StringHelper.isEmpty(ignored)) {
				url += ((url.contains("?")) ? "&" : "?") + "ignored=" + ignored;
			}
			if(!StringHelper.isEmpty(csm)) {
				url += ((url.contains("?")) ? "&" : "?") + "csm=" + csm;
			}
			if(!StringHelper.isEmpty(cem)) {
				url += ((url.contains("?")) ? "&" : "?") + "cem=" + cem;
			}*/
			
			d.setUrl(null);
			d.setColor("#1CAF9A");
			data.add(d);
			
			d = new Data();
			d.setName("No");
			d.setY(falseCount);
			url = "/managecustomers?c__public_company=No";

			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			/*if(!StringHelper.isEmpty(accountManager)) {
				url += ((url.contains("?")) ? "&" : "?") + "accountmanager=" + accountManager;
			}
			if(!StringHelper.isEmpty(customerType)) {
				url += ((url.contains("?")) ? "&" : "?") + "customertype=" + customerType;
			}
			if(!StringHelper.isEmpty(customerType)) {
				url += ((url.contains("?")) ? "&" : "?") + "partner=" + partner;
			}
			if(!StringHelper.isEmpty(ignored)) {
				url += ((url.contains("?")) ? "&" : "?") + "ignored=" + ignored;
			}
			if(!StringHelper.isEmpty(csm)) {
				url += ((url.contains("?")) ? "&" : "?") + "csm=" + csm;
			}
			if(!StringHelper.isEmpty(cem)) {
				url += ((url.contains("?")) ? "&" : "?") + "cem=" + cem;
			}
*/			d.setUrl(null);
			d.setColor("#D9534F");
			data.add(d);

			//constructing graph object
			Highchart highchart = new Highchart();
			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			seriesList.add(series);

			highchart.setChart(new Chart("pie", new Options3d(45, true)));
			highchart.setTitle(new Title("Is Public Company"));
			highchart.setPlotOptions(new PlotOptions(new Pie(true, "pointer", 0, 35, new DataLabels(true, "{point.name} ({point.y})", 10), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));
			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
						
			return newCache.getData();
			//end
		}

		return cache.getData();
	}
	public Map<String, Object> c360customerbytrialaccount(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String , Object> paramMap, Integer cacheHours) throws Exception {


		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				params += "&" + mp.getKey() + "=" + mp.getValue(); 
			}
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

			
			CustomerDAO customerDAO = new CustomerDAO();
			
			long trueCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_this_trial_account_", true);
			long falseCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_this_trial_account_", false);
			
			List<Object> data = new ArrayList<Object>();
			Data d = new Data();
			d.setName("Yes");
			d.setY(trueCount);
			String url = "/managecustomers?c__is_this_trial_account_=true";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			/*if(!StringHelper.isEmpty(accountManager)) {
				url += ((url.contains("?")) ? "&" : "?") + "accountmanager=" + accountManager;
			}
			if(!StringHelper.isEmpty(customerType)) {
				url += ((url.contains("?")) ? "&" : "?") + "customertype=" + customerType;
			}
			if(!StringHelper.isEmpty(customerType)) {
				url += ((url.contains("?")) ? "&" : "?") + "partner=" + partner;
			}
			if(!StringHelper.isEmpty(ignored)) {
				url += ((url.contains("?")) ? "&" : "?") + "ignored=" + ignored;
			}
			if(!StringHelper.isEmpty(csm)) {
				url += ((url.contains("?")) ? "&" : "?") + "csm=" + csm;
			}
			if(!StringHelper.isEmpty(cem)) {
				url += ((url.contains("?")) ? "&" : "?") + "cem=" + cem;
			}*/
			d.setUrl(null);
			d.setColor("#1CAF9A");
			data.add(d);
			
			d = new Data();
			d.setName("No");
			d.setY(falseCount);
			url = "/managecustomers?c__is_this_trial_account_=false";
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			/*if(!StringHelper.isEmpty(accountManager)) {
				url += ((url.contains("?")) ? "&" : "?") + "accountmanager=" + accountManager;
			}
			if(!StringHelper.isEmpty(customerType)) {
				url += ((url.contains("?")) ? "&" : "?") + "customertype=" + customerType;
			}
			if(!StringHelper.isEmpty(customerType)) {
				url += ((url.contains("?")) ? "&" : "?") + "partner=" + partner;
			}
			if(!StringHelper.isEmpty(ignored)) {
				url += ((url.contains("?")) ? "&" : "?") + "ignored=" + ignored;
			}
			if(!StringHelper.isEmpty(csm)) {
				url += ((url.contains("?")) ? "&" : "?") + "csm=" + csm;
			}
			if(!StringHelper.isEmpty(cem)) {
				url += ((url.contains("?")) ? "&" : "?") + "cem=" + cem;
			}*/
			d.setUrl(null);
			d.setColor("#D9534F");
			data.add(d);

			//constructing graph object
			Highchart highchart = new Highchart();
			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			seriesList.add(series);

			highchart.setChart(new Chart("pie", new Options3d(45, true)));
			highchart.setTitle(new Title("Is Trial Approved"));
			highchart.setPlotOptions(new PlotOptions(new Pie(true, "pointer", 0, 35, new DataLabels(true, "{point.name} ({point.y})", 10), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));
			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
						
			return newCache.getData();
			//end
		}

		return cache.getData();
	}
	
	
	public Map<String, Object> c360customerbyservicetype(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {


		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				params += "&" + mp.getKey() + "=" + mp.getValue(); 
			}
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

			
			CustomerDAO customerDAO = new CustomerDAO();
			LicenseDAO  licenseDAO = new LicenseDAO();
			long iads = 0l;
			long wads = 0l;
			long wos = 0l;
			long nas = 0l;
			
			int skip = 0;
			int limit = 500;
			while(true) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						fieldMap.put(mp.getKey(), mp.getValue()); 
					}
				}
				
				List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
				if(customers.size() == 0) {
					break;
				}
				for(Customer customer : customers){
					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("customerId", customer.getId());
					List<License> licenses = licenseDAO.findAll(fieldMap);
					if(licenses!= null){
						for(License license : licenses){
							if(!StringHelper.isEmpty(license.getLicenseType()) ){
								if(license.getLicenseType().equals("WOS")){
									wos++;
								}else if(license.getLicenseType().equals("IADS")){
									iads++;
								}else if(license.getLicenseType().equals("NAS")){
									nas++;
								}else if(license.getLicenseType().equals("WADS")){
									wads++;
								}
							}
						}
					}
				}
				skip += limit;
			}

			
//			logger.debug("wos\t" + wos +"\tIADS\t" + iads +"\tNAS\t" + nas +"\tWADS\t" +wads);
			
			List<Object> data = new ArrayList<Object>();
			Data d = new Data();
			d.setName("IODS");
			d.setY(iads);
			String url = "/managecustomers?c__is_this_trial_account_=true";
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}

		
			d.setUrl(null);
			d.setColor("#1CAF9A");
			data.add(d);
			
			d = new Data();
			d.setName("WADS");
			d.setY(wads);
			url = "/managecustomers?c__is_this_trial_account_=false";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}

			
			d.setUrl(null);
			d.setColor("#D9534F");
			data.add(d);
			
			
			d = new Data();
			d.setName("WOS");
			d.setY(wos);
			url = "/managecustomers?c__is_this_trial_account_=false";
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}

		
			d.setUrl(null);
			d.setColor("#428BCA");
			data.add(d);
			
			
			d = new Data();
			d.setName("NAS");
			d.setY(nas);
			url = "/managecustomers?c__is_this_trial_account_=false";
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}

		
			d.setUrl(null);
			d.setColor("#F7A35C");
			data.add(d);

			//constructing graph object
			Highchart highchart = new Highchart();
			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			seriesList.add(series);

			highchart.setChart(new Chart("pie", new Options3d(45, true)));
			highchart.setTitle(new Title("Service type"));
			highchart.setPlotOptions(new PlotOptions(new Pie(true, "pointer", 0, 35, new DataLabels(true, "{point.name} ({point.y})", 10), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));
			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
						
			return newCache.getData();
			//end
		}

		return cache.getData();
	}
	

	
	public Map<String, Object> c360customerby95thmbpstraffic(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {


		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				params += "&" + mp.getKey() + "=" + mp.getValue(); 
			}
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

			DecimalFormat df1Precision = new DecimalFormat("#");
			CustomerDAO customerDAO = new CustomerDAO();
			LicenseDAO licenseDAO = new LicenseDAO();
			Map<String, Long> dataMap = new LinkedHashMap<String, Long>();
			Map<String, List<Customer>> countsMap = new LinkedHashMap<String, List<Customer>>();
			List<String> accountManagers = customerDAO.getDistinctAccountManagersNew(account.getId(), paramMap);
			for(String am : accountManagers) {
				dataMap.put(am, 0l);
				countsMap.put(am, new ArrayList<Customer>());
			}

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			List<Customer> customers = customerDAO.findAll(fieldMap);
			

			for(Customer customer : customers) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				fieldMap.put("customerId", customer.getId());
				List<License> licenses = licenseDAO.findAll(fieldMap);
				double total = 0;
				if(licenses != null){				
					for(License license : licenses){

						if(license.getCustomFields() != null){
							for(Field field : license.getCustomFields()){								
								if(field.getName().equals("app_traffic_95th_mbps") && field.getValue() != null){
									total +=  Double.parseDouble(field.getValue().toString());
								}									
							}
						}
					}
				}
				dataMap.put(customer.getName(), Long.parseLong(df1Precision.format(total)));			
			}

			dataMap = Utility.getInstance().sortByValues(dataMap);
		    
	        
			List<Object> data = new ArrayList<Object>();
			List<Object> xAxis = new ArrayList<Object>();
			for(Map.Entry<String, Long> entry : dataMap.entrySet()) {
				if(data.size() > 10) {
					break;
				}
				if(entry.getValue() > 0) {
					Data d = new Data();
					d.setY(entry.getValue());

					String name = entry.getKey().toString();
					if(name.length() > 15) {
						name = name.substring(0, 15) + "...";
					}

					d.setName(name);
					String url = "/managecustomers?accountmanager=" + entry.getKey();
					
					for(Map.Entry<String, Object> mp : paramMap.entrySet()){
						if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
							url +=  "&"+mp.getKey()+"=" + mp.getValue();							
						}
					}
					d.setUrl(null);
					xAxis.add(name);
					data.add(d);
				}
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(false);
			seriesList.add(series);

	

			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by 95thMBps"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
//			xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
//			xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Traffic count"), 0, new Labels("{value}", new Style("red")), false));
			yAxisList.add(new YAxis(new Title("Average"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, "#AAA"), new Point(new Events("function(e) {if(this.url != 'undefined' && this.url != null){window.location = this.url;}}")))));

			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
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
	
	public Map<String, Object> c360customerbypopname(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;		
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				params += "&" + mp.getKey() + "=" + mp.getValue(); 
			}
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
			CustomerDAO customerDAO = new CustomerDAO();
			LicenseDAO licenseDAO = new LicenseDAO();
			Set<Object> popNames = licenseDAO.getDistinctLicenseCustomFieldValuesEnhanced(account.getId(), "pop_name");
			Map<String, Long> dataMap = new HashMap<String, Long>();
			
	
			int skip = 0;
			int limit = 1000;
			while(true) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						fieldMap.put(mp.getKey(), mp.getValue()); 
					}
				}				
				List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
				if(customers.size() == 0) {
					break;
				}
				for(Customer customer : customers) {
					
					for(Object popName : popNames) {
						dataMap.put(popName.toString(), (dataMap.get(popName) != null ? dataMap.get(popName) : 0l) + licenseDAO.countByLicenseCustomFieldNameAndValue(account.getId(), customer.getId(), "pop_name", popName));
					}			
				}
				skip += limit;
			}
			
			List<Object> data = new ArrayList<Object>();
			List<Object> xAxis = new ArrayList<Object>();

			for(Map.Entry<String, Long> entry : Utility.getInstance().sortByValues(dataMap).entrySet()) {
				Data d = new Data();
				d.setY(entry.getValue());
				String name = entry.getKey().toString();
				if(name.length() > 15) {
					name = name.substring(0, 15) + "...";
				}
				d.setName(name);
				String url = "/managecustomers?c__pop_name=" + entry.getKey();			
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						url +=  "&"+mp.getKey()+"=" + mp.getValue();							
					}
				}
				d.setUrl(null);
				xAxis.add(entry.getKey());
				data.add(d);
			}

			List<Series> seriesList = new ArrayList<Series>();
			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(false);
			seriesList.add(series);
		
			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by POP Name"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
//					yAxisList.add(new YAxis(new Title("Average"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);
			
			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, "#AAA"), new Point(new Events("function(e) {if(this.url != 'undefined' && this.url != null){window.location = this.url;}}")))));

			highchart.setSeries(seriesList);

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
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
	
	public Map<String, Object> c360customerbytype(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				params += "&" + mp.getKey() + "=" + mp.getValue(); 
			}
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
			CustomerDAO customerDAO = new CustomerDAO();
			LicenseDAO licenseDAO = new LicenseDAO();
			Set<Object> types = licenseDAO.getDistinctLicenseCustomFieldValuesEnhanced(account.getId(), "type");
			Map<String, Long> dataMap = new HashMap<String, Long>();
			int skip = 0;
			int limit = 1000;
			while(true) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						fieldMap.put(mp.getKey(), mp.getValue()); 
					}
				}
				List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
				if(customers.size() == 0) {
					break;
				}
				for(Customer customer : customers) {
					for(Object type : types) {
						dataMap.put(type.toString(), (dataMap.get(type) != null ? dataMap.get(type) : 0l) + licenseDAO.countByLicenseCustomFieldNameAndValue(account.getId(), customer.getId(), "type", type));
					}			
				}
				skip += limit;
			}
			List<Object> data = new ArrayList<Object>();
			List<Object> xAxis = new ArrayList<Object>();

			for(Map.Entry<String, Long> entry : Utility.getInstance().sortByValues(dataMap).entrySet()) {
				Data d = new Data();
				d.setY(entry.getValue());
				String name = entry.getKey().toString();
				if(name.length() > 15) {
					name = name.substring(0, 15) + "...";
				}
				d.setName(name);
				String url = "/managecustomers?c__type=" + entry.getKey();				
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						url +=  "&"+mp.getKey()+"=" + mp.getValue();							
					}
				}
				d.setUrl(null);
				xAxis.add(entry.getKey());
				data.add(d);
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(false);
			seriesList.add(series);


			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by Type"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, "#AAA"), new Point(new Events("function(e) {if(this.url != 'undefined' && this.url != null){window.location = this.url;}}")))));
			highchart.setSeries(seriesList);
			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
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

	public Map<String, Object> c360docustomermetricscalculationsaryaka(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

		InformationDAO informationDAO = new InformationDAO();
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		
		boolean infoCached = false;
		Information cachedInfo = informationDAO.findCachedData(account.getId(), calendar.getTime(), "UPSELL", "CUSTOMER");
		if(cachedInfo != null) {
			infoCached = true;
		}
		
		if(!infoCached) {
			informationDAO.deleteCachedData(account.getId(), null, null, "UPSELL", "CUSTOMER");
			
			Map<String, Object> fieldMap = new HashMap<String, Object>();
			CustomerDAO customerDAO = new CustomerDAO();
			LicenseDAO licenseDAO = new LicenseDAO();
			CustomerTypeDAO customerTypeDAO = new CustomerTypeDAO();
			DecimalFormat df = new DecimalFormat("#.###");
			
			Calendar range = Calendar.getInstance();
			range.set(Calendar.HOUR_OF_DAY, 0);
			range.set(Calendar.MINUTE, 0);
			range.set(Calendar.SECOND, 0);
			range.set(Calendar.MILLISECOND, 0);
			range.add(Calendar.DATE, -90);
			
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			Map<String, String> customerTypes = new HashMap<String, String>();
			for(CustomerType type : customerTypeDAO.findAll()) {
				customerTypes.put(type.getName(), type.getId());
			}
			
			Double percent = 0d;
			int skip = 0;
			int limit = 1000;
			while(true) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
				if(customers.size() == 0) {
					break;
				}
				for(Customer customer : customers) {
					try {
						if(customer.getSites() != null) {
							for(Site site : customer.getSites()) {
								Information info = new Information();
								info.setAccountId(account.getId());
								info.setType("UPSELL");
								info.setEntity("CUSTOMER");
								info.setEntityId(customer.getId());
								info.setName(customer.getName());
								info.setSiteName(site.getNormalizedName());
								
								info.setIgnored(customer.getIgnored());
//								info.setCustomerTypeId(customerTypes.get(license.getLicenseType()));
								info.setPartnerId(customer.getPartnerId());
								info.setAccountManager(customer.getAccountManager());
								info.setCsm(customer.getCsm());
								info.setCem(customer.getCem());
								info.setSalesperson(customer.getSalesperson());
								info.setCustomerTypes((customer.getCustomerTypes() != null && customer.getCustomerTypes().size() > 0 ? customer.getCustomerTypes() : null));
								info.setCurrentMrr((customer.getCurrentMrr() != null) ? Double.valueOf(df.format(customer.getCurrentMrr())) : 0d);
								info.setDate(calendar.getTime());
								info.setCommit(site.getBandwidthCommit());
								info.setUtilized(site.getBandwidthUsage());
								info.setUtilizedPercent(0d);
								
								percent = 0d;
								percent = info.getUtilized() * 100;
								info.setUtilizedPercent((info.getCommit() != 0) ? Double.valueOf(df.format(percent / info.getCommit())) : 0);
								
								informationDAO.insert(info);
							}
						}
						
						
						/*fieldMap.clear();
						fieldMap.put("accountId", account.getId());
						fieldMap.put("customerId", customer.getId());
						List<License> licenses = licenseDAO.findAll(fieldMap);
						
						for(License license : licenses) {
							if(!StringHelper.isEmpty(license.getLicenseType())) {
								Information info = new Information();
								info.setAccountId(account.getId());
								info.setType("UPSELL");
								info.setEntity("CUSTOMER");
								info.setEntityId(customer.getId());
								info.setName(customer.getName());
								info.setIgnored(customer.getIgnored());
								info.setCustomerTypeId(customerTypes.get(license.getLicenseType()));
								info.setPartnerId(customer.getPartnerId());
								info.setAccountManager(customer.getAccountManager());
								info.setCsm(customer.getCsm());
								info.setCem(customer.getCem());
								info.setSalesperson(customer.getSalesperson());
								info.setCustomerTypes((customer.getCustomerTypes() != null && customer.getCustomerTypes().size() > 0 ? customer.getCustomerTypes() : null));
								info.setCurrentMrr((customer.getCurrentMrr() != null) ? Double.valueOf(df.format(customer.getCurrentMrr())) : 0d);
								info.setDate(calendar.getTime());
								info.setCommit(0d);
								info.setUtilized(0d);
								info.setUtilizedPercent(0d);
								info.setContractId(license.getContractId());
								
								if(license.getCustomFields() != null) {
									percent = 0d;
									for(Field field : license.getCustomFields()) {
										if(field.getName().equals("commit") && field.getValue() != null) {
											info.setCommit(info.getCommit() + Double.parseDouble(field.getValue().toString()));
										} else if(field.getName().equals("app_traffic_95th_mbps") && field.getValue() != null) {
											info.setUtilized(info.getUtilized() + Double.parseDouble(field.getValue().toString()));
										} else if(field.getName().equals("pop_name") && field.getValue() != null) {
											info.setPopName(field.getValue().toString());
										} else if(field.getName().equals("application") && field.getValue() != null) {
											info.setApplication(field.getValue().toString());
										}
									}
									percent = info.getUtilized() * 100;
									info.setUtilizedPercent((info.getCommit() != 0) ? Double.valueOf(df.format(percent / info.getCommit())) : 0);
								}
								
								informationDAO.insert(info);
							}
						}*/
					} catch(Exception e) {
//						logger.error(e);
					}
				}
				skip += limit;
			}
		}
		
		return null;
	}
	
	public Map<String, Object> c360customersbyproductrate1aryaka(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, int page, String sortBy, boolean ascending) throws Exception {
		
		InformationDAO informationDAO = new InformationDAO();
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		logger.debug("Customer Type \t" + paramMap.get("customerTypes"));
		CustomerTypeDAO customerTypeDAO = new CustomerTypeDAO();
		CustomerType customerType = customerTypeDAO.findOneById(((paramMap.get("customerTypes") != null) ? paramMap.get("customerTypes").toString() : null));
		
		if(true) {
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			
			if(StringHelper.isEmpty(sortBy)) {
				sortBy = "utilizedPercent";
			}
			
			int skip = 0;
			int limit = 15;
			int numTabsToShown = 7;
			skip = (page - 1) * limit;
			
			List<Information> informations = informationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
			long totalCount = informationDAO.countByFilters(account.getId(), "UPSELL", "CUSTOMER", paramMap);
			
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
			cachedData.put("listName", "c360customersbyproductrate1aryaka");
			cachedData.put("apiName", "c360customersbyproductrate1aryaka");
			cachedData.put("sortBy", sortBy);
			cachedData.put("ascending", ascending);
			cachedData.put("title", "Customers by Commit/Utilization (" + customerType.getName() + ")");
			cachedData.put("informations", informations);
			cachedData.put("pageCount", pageCount);
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					cachedData.put(mp.getKey(), mp.getValue());
				}
			}
			
			cachedData.put("C360_CUSTOMERS_BY_PRODUCT_RATE_1_ARYAKA", "yes");
		}
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailscommittrafficseries(
			Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
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

			LicenseDAO licenseDAO = new LicenseDAO();
			CustomerDAO customerDAO = new CustomerDAO();
			Customer customer = customerDAO.findOneById(customerId);
			DecimalFormat df1Precision = new DecimalFormat("#");

			if(customer != null) {
				long startTime = new Date().getTime();
			
				SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");
				LicenseTimeseriesByDayDAO licenseTimeseriesByDayDAO = new LicenseTimeseriesByDayDAO();
				
				Calendar calendar = Calendar.getInstance();		
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				
				Date rangeEnd = calendar.getTime();
				calendar.add(Calendar.DATE, -((!StringHelper.isEmpty(rangeFilter)) ? Integer.parseInt(rangeFilter) : 90));
				Date rangeStart = calendar.getTime();
				long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
				
				List<Series> seriesList = new ArrayList<Series>();
				
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				fieldMap.put("customerId", customer.getId());
				List<License> licenses = licenseDAO.findAll(fieldMap);
				for(License license : licenses) {
					Map<Date, Long> map = new LinkedHashMap<Date, Long>();
					List<LicenseTimeseriesByDay> timeseriesData = licenseTimeseriesByDayDAO.getData(account.getId(), customer.getId(), license.getId(), "commit", rangeStart, rangeEnd);
					for(LicenseTimeseriesByDay timeseries : timeseriesData) {
						if(!map.containsKey(timeseries.getDate()) && map.size() < days && timeseries.getFieldValue() != null) {
							map.put(timeseries.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
						} else if(map.containsKey(timeseries.getDate()) && timeseries.getFieldValue() != null) {
							map.put(timeseries.getDate(), map.get(timeseries.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
						}
					}
					
					Series series = new Series();
					series.setName(license.getContractId());
					series.setShowInLegend(true);
					List<Object> data = new ArrayList<Object>();
					for(Map.Entry<Date, Long> entry : map.entrySet()) {
						Data dta = new Data();				
						dta.setX(entry.getKey().getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("Commit: " + entry.getValue().longValue());
						data.add(dta);
					}
					series.setData(data);
					seriesList.add(series);
				}
				
				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("Bandwidth Commit"));
				if(!StringHelper.isEmpty(rangeFilter)) {
					highchart.setSubtitle(new Subtitle("Last " + rangeFilter + " days"));
				}
				
				List<XAxis> xAxisList = new ArrayList<XAxis>();
				XAxis xaAxis = new XAxis();
				xaAxis.setType("datetime");
				xaAxis.setDateTimeLabelFormats(new DateTimeLabelFormats("%b", "%b %e", null, null, null, null, null));
				xAxisList.add(xaAxis);

				highchart.setxAxis(xAxisList);

				List<YAxis> yAxisList = new ArrayList<YAxis>();
				YAxis yAxis = new YAxis();
				yAxis.setTitle(new Title(""));
				yAxis.setMin(0);
				yAxis.setAllowDecimals(false);
				yAxisList.add(yAxis);

				highchart.setyAxis(yAxisList);
				highchart.setSeries(seriesList);

				highchart.setPlotOptions(new PlotOptions(new Spline(new Marker(3, true))));

				Map<String, Object> podData = new HashMap<String, Object>();
				podData.put("highchart", highchart);

				PodDataCache newCache = new PodDataCache();
				newCache.setAccountId(account.getId());
				newCache.setPodId(podId);
				newCache.setParamHash(paramHash);
				newCache.setData(podData);
				newCache.setCreatedAt(new Date());

				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				fieldMap.put("podId", podId);
				fieldMap.put("paramHash", paramHash);
				cacheDAO.delete(fieldMap);
				newCache = cacheDAO.insert(newCache);
				long endTime = new Date().getTime();
				long diff = endTime - startTime;
//				logger.debug("by c360customerby Mrr Utilized [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
				
			}
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360committrafficseries(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				params += "&" + mp.getKey() + "=" + mp.getValue(); 
			}
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
			long startTime = new Date().getTime();
		
			SiteTimeseriesByDayDAO siteTimeseriesByDayDAO = new SiteTimeseriesByDayDAO();
	
			Calendar calendar = Calendar.getInstance();		
			calendar.set(Calendar.HOUR_OF_DAY, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MILLISECOND, 0);
			
			Date rangeEnd = calendar.getTime();
			calendar.add(Calendar.DATE, -((!StringHelper.isEmpty(rangeFilter)) ? Integer.parseInt(rangeFilter) : 90));
			Date rangeStart = calendar.getTime();		
			long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
			
			List<SiteTimeseriesByDay> siteTimeseriesByDays = siteTimeseriesByDayDAO.getData(account.getId(), "bandwidthCommit", rangeStart, rangeEnd);
			
			Map<Date, Double> pbMap = new LinkedHashMap<Date, Double>();
			
			for(SiteTimeseriesByDay std : siteTimeseriesByDays) {
				if(pbMap.size() <= days && std.getFieldValue() != null) {
					pbMap.put(std.getDate(), 0D);
				}
			}
			for(SiteTimeseriesByDay std : siteTimeseriesByDays) {
				if(std.getFieldValue() != null && pbMap.get(std.getDate()) != null) {
					pbMap.put(std.getDate(), pbMap.get(std.getDate()) + Double.parseDouble(std.getFieldValue().toString()));
				}
			}
			
			List<Series> seriesList = new ArrayList<Series>();
			Series series = new Series();
			
			series.setName("Bandwidth Commit");
			List<Object> data = new ArrayList<Object>();
			for(Map.Entry<Date, Double> entry : pbMap.entrySet()) {
				Data dta = new Data();				
				dta.setX(entry.getKey().getTime());					
				dta.setY(entry.getValue().longValue());
				dta.setInfo("Commit: " + entry.getValue().longValue());
				data.add(dta);
			}
				
			series.setData(data);
			series.setColor("#808000");
			series.setShowInLegend(true);
			seriesList.add(series);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("spline", null));
			highchart.setTitle(new Title("Bandwidth Commit"));
			if(!StringHelper.isEmpty(rangeFilter)) {
				highchart.setSubtitle(new Subtitle("Last " + rangeFilter + " days"));
			}

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			XAxis xaAxis = new XAxis();
			xaAxis.setType("datetime");
			xaAxis.setDateTimeLabelFormats(new DateTimeLabelFormats("%b", "%b %e", null, null, null, null, null));
			xAxisList.add(xaAxis);

			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			YAxis yAxis = new YAxis();
			yAxis.setTitle(new Title(""));
			yAxis.setMin(0);
			yAxis.setAllowDecimals(false);
			yAxisList.add(yAxis);

			highchart.setyAxis(yAxisList);
			highchart.setSeries(seriesList);

			highchart.setPlotOptions(new PlotOptions(new Spline(new Marker(3, true))));

			Map<String, Object> podData = new HashMap<String, Object>();
			podData.put("highchart", highchart);

			PodDataCache newCache = new PodDataCache();
			newCache.setAccountId(account.getId());
			newCache.setPodId(podId);
			newCache.setParamHash(paramHash);
			newCache.setData(podData);
			newCache.setCreatedAt(new Date());

			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("paramHash", paramHash);
			cacheDAO.delete(fieldMap);
			newCache = cacheDAO.insert(newCache);
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//				logger.debug("by c360customerby Mrr Utilized [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			return newCache.getData();
		}
		return cache.getData();
	}
	
	
	//cdetailswaninout95thmbpstrafficseries
		public Map<String, Object> cdetailswaninout95thmbpstrafficseries(
				Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
			PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
			String params = "podId=" + podId;
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				params += "&rangeFilter=" + rangeFilter; 
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

				LicenseDAO licenseDAO = new LicenseDAO();
				CustomerDAO customerDAO = new CustomerDAO();
				Customer customer = customerDAO.findOneById(customerId);
				DecimalFormat df1Precision = new DecimalFormat("#");
				LicenseTimeseriesByDayDAO licenseTimeseriesByDayDAO = new LicenseTimeseriesByDayDAO();
				if(customer != null) {
					
					long startTime = new Date().getTime();
				
					Calendar calendar = Calendar.getInstance();		
					calendar.set(Calendar.HOUR_OF_DAY, 0);
					calendar.set(Calendar.MINUTE, 0);
					calendar.set(Calendar.SECOND, 0);
					calendar.set(Calendar.MILLISECOND, 0);
					
					Date rangeEnd = calendar.getTime();
					calendar.add(Calendar.DATE, -((!StringHelper.isEmpty(rangeFilter)) ? Integer.parseInt(rangeFilter) : 90));
					Date rangeStart = calendar.getTime();
					long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
					
					List<Series> seriesList = new ArrayList<Series>();
					
					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("customerId", customer.getId());
					List<License> licenses = licenseDAO.findAll(fieldMap);
					for(License license : licenses) {
						Map<Date, Long> map = new LinkedHashMap<Date, Long>();
						List<LicenseTimeseriesByDay> timeseriesData = licenseTimeseriesByDayDAO.getData(account.getId(), customer.getId(), license.getId(), "wan_in_95th_mbps", rangeStart, rangeEnd);
						for(LicenseTimeseriesByDay timeseries : timeseriesData) {
							if(!map.containsKey(timeseries.getDate()) && map.size() < days && timeseries.getFieldValue() != null) {
								map.put(timeseries.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
							} else if(map.containsKey(timeseries.getDate()) && timeseries.getFieldValue() != null) {
								map.put(timeseries.getDate(), map.get(timeseries.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
							}
						}
						
						Series series = new Series();
						series.setName(license.getContractId());
						series.setShowInLegend(true);
						List<Object> data = new ArrayList<Object>();
						for(Map.Entry<Date, Long> entry : map.entrySet()) {
							Data dta = new Data();				
							dta.setX(entry.getKey().getTime());					
							dta.setY(entry.getValue().longValue());
							dta.setInfo("Commit: " + entry.getValue().longValue());
							data.add(dta);
						}
						series.setData(data);
						seriesList.add(series);
					}

					Highchart highchart = new Highchart();

					highchart.setChart(new Chart("spline", null));
					highchart.setTitle(new Title("Bandwidth Usage"));
					if(!StringHelper.isEmpty(rangeFilter)) {
						highchart.setSubtitle(new Subtitle("Last " + rangeFilter + " days"));
					}

					List<XAxis> xAxisList = new ArrayList<XAxis>();
					XAxis xaAxis = new XAxis();
					xaAxis.setType("datetime");
					xaAxis.setDateTimeLabelFormats(new DateTimeLabelFormats("%b", "%b %e", null, null, null, null, null));
					xAxisList.add(xaAxis);

					highchart.setxAxis(xAxisList);

					List<YAxis> yAxisList = new ArrayList<YAxis>();
					YAxis yAxis = new YAxis();
					yAxis.setTitle(new Title(""));
					yAxis.setMin(0);
					yAxis.setAllowDecimals(false);
					yAxisList.add(yAxis);

					highchart.setyAxis(yAxisList);
					highchart.setSeries(seriesList);

					highchart.setPlotOptions(new PlotOptions(new Spline(new Marker(3, true))));

					Map<String, Object> podData = new HashMap<String, Object>();
					podData.put("highchart", highchart);

					PodDataCache newCache = new PodDataCache();
					newCache.setAccountId(account.getId());
					newCache.setPodId(podId);
					newCache.setParamHash(paramHash);
					newCache.setData(podData);
					newCache.setCreatedAt(new Date());

					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("podId", podId);
					fieldMap.put("paramHash", paramHash);
					cacheDAO.delete(fieldMap);
					newCache = cacheDAO.insert(newCache);
					long endTime = new Date().getTime();
					long diff = endTime - startTime;
//					logger.debug("by cdetails by PU bought [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

					return newCache.getData();
					
				}
			}
			return cache.getData();
		}
		
		
		//c360waninout95thmbpstrafficseries
		public Map<String, Object> c360waninout95thmbpstrafficseries(
				Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

				PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
				
				String params = "podId=" + podId;
				if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
					params += "&rangeFilter=" + rangeFilter; 
				}
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						params += "&" + mp.getKey() + "=" + mp.getValue(); 
					}
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
					long startTime = new Date().getTime();
					
					SiteTimeseriesByDayDAO siteTimeseriesByDayDAO = new SiteTimeseriesByDayDAO();
			
					Calendar calendar = Calendar.getInstance();		
					calendar.set(Calendar.HOUR_OF_DAY, 0);
					calendar.set(Calendar.MINUTE, 0);
					calendar.set(Calendar.SECOND, 0);
					calendar.set(Calendar.MILLISECOND, 0);
					
					Date rangeEnd = calendar.getTime();
					calendar.add(Calendar.DATE, -((!StringHelper.isEmpty(rangeFilter)) ? Integer.parseInt(rangeFilter) : 90));
					Date rangeStart = calendar.getTime();	
					long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
					
					Map<Date, Double> pbMap = new LinkedHashMap<Date, Double>();	
					
					List<SiteTimeseriesByDay> siteTimeseriesByDays = siteTimeseriesByDayDAO.getData(account.getId(), "bandwidthUsage", rangeStart, rangeEnd);
					
					for(SiteTimeseriesByDay std : siteTimeseriesByDays) {
						if(pbMap.size() <= days && std.getFieldValue() != null) {
							pbMap.put(std.getDate(), 0D);
						}
					}
					for(SiteTimeseriesByDay std : siteTimeseriesByDays) {
						if(std.getFieldValue() != null && pbMap.get(std.getDate()) != null) {
							pbMap.put(std.getDate(), pbMap.get(std.getDate()) + Double.parseDouble(std.getFieldValue().toString()));
						}
					}
					
					
					List<Series> seriesList = new ArrayList<Series>();
					Series series = new Series();
					
					series.setName("Usage");
					List<Object> data = new ArrayList<Object>();
					for(Map.Entry<Date, Double> entry : pbMap.entrySet()) {
						Data dta = new Data();				
						dta.setX(entry.getKey().getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("Usage: " + entry.getValue().longValue());
						data.add(dta);
					}
						
					series.setData(data);
					series.setColor("#FFB90F");
					series.setShowInLegend(true);
					seriesList.add(series);
					

					Highchart highchart = new Highchart();

					highchart.setChart(new Chart("spline", null));
					highchart.setTitle(new Title("Bandwidth Usage"));
					if(!StringHelper.isEmpty(rangeFilter)) {
						highchart.setSubtitle(new Subtitle("Last " + rangeFilter + " days"));
					}

					List<XAxis> xAxisList = new ArrayList<XAxis>();
					XAxis xaAxis = new XAxis();
					xaAxis.setType("datetime");
					xaAxis.setDateTimeLabelFormats(new DateTimeLabelFormats("%b", "%b %e", null, null, null, null, null));
					xAxisList.add(xaAxis);

					highchart.setxAxis(xAxisList);

					List<YAxis> yAxisList = new ArrayList<YAxis>();
					YAxis yAxis = new YAxis();
					yAxis.setTitle(new Title(""));
					yAxis.setMin(0);
					yAxis.setAllowDecimals(false);
					yAxisList.add(yAxis);

					highchart.setyAxis(yAxisList);
					highchart.setSeries(seriesList);

					highchart.setPlotOptions(new PlotOptions(new Spline(new Marker(3, true))));

					Map<String, Object> podData = new HashMap<String, Object>();
					podData.put("highchart", highchart);

					PodDataCache newCache = new PodDataCache();
					newCache.setAccountId(account.getId());
					newCache.setPodId(podId);
					newCache.setParamHash(paramHash);
					newCache.setData(podData);
					newCache.setCreatedAt(new Date());

					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("podId", podId);
					fieldMap.put("paramHash", paramHash);
					cacheDAO.delete(fieldMap);
					newCache = cacheDAO.insert(newCache);
					long endTime = new Date().getTime();
					long diff = endTime - startTime;
//					logger.debug("by c360customerby PU bought [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

					return newCache.getData();
				}
				return cache.getData();
			}
		
		public Map<String, Object> cdetailscommitusagetrafficseries(
				Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
			PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
			String params = "podId=" + podId;
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				params += "&rangeFilter=" + rangeFilter; 
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

				LicenseDAO licenseDAO = new LicenseDAO();
				CustomerDAO customerDAO = new CustomerDAO();
				LicenseTimeseriesByDayDAO licenseTimeseriesByDayDAO = new LicenseTimeseriesByDayDAO();
				Customer customer = customerDAO.findOneById(customerId);
				DecimalFormat df1Precision = new DecimalFormat("#");
				SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");

				List<Highchart> highcharts = new ArrayList<Highchart>();
				if(customer != null) {
					long startTime = new Date().getTime();
					
					Calendar calendar = Calendar.getInstance();		
					calendar.set(Calendar.HOUR_OF_DAY, 0);
					calendar.set(Calendar.MINUTE, 0);
					calendar.set(Calendar.SECOND, 0);
					calendar.set(Calendar.MILLISECOND, 0);
					SiteTimeseriesByDayDAO siteTimeseriesByDayDAO = new SiteTimeseriesByDayDAO();
					Date rangeEnd = calendar.getTime();
					calendar.add(Calendar.DATE, -((!StringHelper.isEmpty(rangeFilter)) ? Integer.parseInt(rangeFilter) : 90));
					Date rangeStart = calendar.getTime();
					long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
					if(customer.getSites() != null && customer.getSites().size() > 0){

						for(Site site : customer.getSites()){
							
							List<Series> seriesList = new ArrayList<Series>();
							Map<Date, Long> map = new LinkedHashMap<Date, Long>();
							Map<Date, Long> usageMap = new LinkedHashMap<Date, Long>();
					
							List<SiteTimeseriesByDay> timeseriesData =  siteTimeseriesByDayDAO.getData(account.getId(), customerId, site.getNormalizedName(), "bandwidthCommit", rangeStart, rangeEnd);
							
							for(SiteTimeseriesByDay timeseries : timeseriesData) {
								if(!map.containsKey(timeseries.getDate()) && map.size() < days && timeseries.getFieldValue() != null) {
									map.put(timeseries.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
								} else if(map.containsKey(timeseries.getDate()) && timeseries.getFieldValue() != null) {
									map.put(timeseries.getDate(), map.get(timeseries.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
								}
							}
							
							Series series = new Series();
							series.setName("Commit");
							series.setShowInLegend(true);
							List<Object> data = new ArrayList<Object>();
							for(Map.Entry<Date, Long> entry : map.entrySet()) {				
								Data dta = new Data();				
								dta.setX(entry.getKey().getTime());					
								dta.setY(entry.getValue().longValue());
								dta.setInfo("Commit: " + entry.getValue().longValue());
								data.add(dta);
							}
							series.setData(data);
							//bandwidthUsage
							List<SiteTimeseriesByDay> usageTimeseriesData =  siteTimeseriesByDayDAO.getData(account.getId(), customerId, site.getNormalizedName(), "bandwidthUsage", rangeStart, rangeEnd);


							for(SiteTimeseriesByDay timeseries : usageTimeseriesData) {
								if(!usageMap.containsKey(timeseries.getDate()) && usageMap.size() < days && timeseries.getFieldValue() != null) {
									usageMap.put(timeseries.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
								} else if(usageMap.containsKey(timeseries.getDate()) && timeseries.getFieldValue() != null) {
									usageMap.put(timeseries.getDate(), usageMap.get(timeseries.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
								}
							}
							Series usageSeries = new Series();
							usageSeries.setName("Usage");
							usageSeries.setShowInLegend(true);
							List<Object> usageData = new ArrayList<Object>();
							for(Map.Entry<Date, Long> entry : usageMap.entrySet()) {
								Data dta = new Data();				
								dta.setX(entry.getKey().getTime());					
								dta.setY(entry.getValue().longValue());
								dta.setInfo("Usage: " + entry.getValue().longValue());
								usageData.add(dta);
							}
							usageSeries.setData(usageData);
							
							seriesList.add(usageSeries);
							seriesList.add(series);
							
							Highchart highchart = new Highchart();

							highchart.setChart(new Chart("spline", null));
							highchart.setTitle(new Title("Bandwidth Commit/Usage (" + site.getName() + ")"));
							if(!StringHelper.isEmpty(rangeFilter)) {
								highchart.setSubtitle(new Subtitle("Last " + rangeFilter + " days"));
							}
							
							List<XAxis> xAxisList = new ArrayList<XAxis>();
							XAxis xaAxis = new XAxis();
							xaAxis.setType("datetime");
							xaAxis.setDateTimeLabelFormats(new DateTimeLabelFormats("%b", "%b %e", null, null, null, null, null));
							xAxisList.add(xaAxis);

							highchart.setxAxis(xAxisList);

							List<YAxis> yAxisList = new ArrayList<YAxis>();
							YAxis yAxis = new YAxis();
							yAxis.setTitle(new Title(""));
							yAxis.setMin(0);
							yAxis.setAllowDecimals(false);
							yAxisList.add(yAxis);

							highchart.setyAxis(yAxisList);
							highchart.setSeries(seriesList);

							highchart.setPlotOptions(new PlotOptions(new Spline(new Marker(3, true))));
							
							highcharts.add(highchart);
							
						}
					}

					
					

					Map<String, Object> podData = new HashMap<String, Object>();
					podData.put("highcharts", highcharts);

					PodDataCache newCache = new PodDataCache();
					newCache.setAccountId(account.getId());
					newCache.setPodId(podId);
					newCache.setParamHash(paramHash);
					newCache.setData(podData);
					newCache.setCreatedAt(new Date());

					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("podId", podId);
					fieldMap.put("paramHash", paramHash);
					cacheDAO.delete(fieldMap);
					newCache = cacheDAO.insert(newCache);
					long endTime = new Date().getTime();
					long diff = endTime - startTime;
//					logger.debug("by c360customerby Mrr Utilized [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

					return newCache.getData();
					
				}
			}
			return cache.getData();
		}
		/*public Map<String, Object> cdetailscommitusagetrafficseries(
				Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
			PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
			String params = "podId=" + podId;
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				params += "&rangeFilter=" + rangeFilter; 
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

				LicenseDAO licenseDAO = new LicenseDAO();
				CustomerDAO customerDAO = new CustomerDAO();
				LicenseTimeseriesByDayDAO licenseTimeseriesByDayDAO = new LicenseTimeseriesByDayDAO();
				Customer customer = customerDAO.findOneById(customerId);
				DecimalFormat df1Precision = new DecimalFormat("#");
				SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");

				List<Highchart> highcharts = new ArrayList<Highchart>();
				if(customer != null) {
					long startTime = new Date().getTime();
					
					Calendar calendar = Calendar.getInstance();		
					calendar.set(Calendar.HOUR_OF_DAY, 0);
					calendar.set(Calendar.MINUTE, 0);
					calendar.set(Calendar.SECOND, 0);
					calendar.set(Calendar.MILLISECOND, 0);
					
					Date rangeEnd = calendar.getTime();
					calendar.add(Calendar.DATE, -((!StringHelper.isEmpty(rangeFilter)) ? Integer.parseInt(rangeFilter) : 90));
					Date rangeStart = calendar.getTime();
					long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
					
					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("customerId", customer.getId());
					List<License> licenses = licenseDAO.findAll(fieldMap);
					for(License license : licenses) {
						
						List<Series> seriesList = new ArrayList<Series>();
						Map<Date, Long> map = new LinkedHashMap<Date, Long>();
						Map<Date, Long> usageMap = new LinkedHashMap<Date, Long>();
						
						List<LicenseTimeseriesByDay> timeseriesData = licenseTimeseriesByDayDAO.getData(account.getId(), customer.getId(), license.getId(), "commit", rangeStart, rangeEnd);
						for(LicenseTimeseriesByDay timeseries : timeseriesData) {
							if(!map.containsKey(timeseries.getDate()) && map.size() < days && timeseries.getFieldValue() != null) {
								map.put(timeseries.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
							} else if(map.containsKey(timeseries.getDate()) && timeseries.getFieldValue() != null) {
								map.put(timeseries.getDate(), map.get(timeseries.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
							}
						}
						
						
						Series series = new Series();
						series.setName("Commit");
						series.setShowInLegend(true);
						List<Object> data = new ArrayList<Object>();
						for(Map.Entry<Date, Long> entry : map.entrySet()) {				
							Data dta = new Data();				
							dta.setX(entry.getKey().getTime());					
							dta.setY(entry.getValue().longValue());
							dta.setInfo("Commit: " + entry.getValue().longValue());
							data.add(dta);
						}
						series.setData(data);
						
						//***********
						List<LicenseTimeseriesByDay> usageTimeseriesData = licenseTimeseriesByDayDAO.getData(account.getId(), customer.getId(), license.getId(), "wan_in_95th_mbps", rangeStart, rangeEnd);
						for(LicenseTimeseriesByDay timeseries : usageTimeseriesData) {
							if(!usageMap.containsKey(timeseries.getDate()) && usageMap.size() < days && timeseries.getFieldValue() != null) {
								usageMap.put(timeseries.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
							} else if(usageMap.containsKey(timeseries.getDate()) && timeseries.getFieldValue() != null) {
								usageMap.put(timeseries.getDate(), usageMap.get(timeseries.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
							}
						}
						
						Series usageSeries = new Series();
						usageSeries.setName("Usage");
						usageSeries.setShowInLegend(true);
						List<Object> usageData = new ArrayList<Object>();
						for(Map.Entry<Date, Long> entry : usageMap.entrySet()) {
							Data dta = new Data();				
							dta.setX(entry.getKey().getTime());					
							dta.setY(entry.getValue().longValue());
							dta.setInfo("Usage: " + entry.getValue().longValue());
							usageData.add(dta);
						}
						usageSeries.setData(usageData);
						
						seriesList.add(usageSeries);
						seriesList.add(series);
						
						Highchart highchart = new Highchart();

						highchart.setChart(new Chart("spline", null));
						highchart.setTitle(new Title("Bandwidth Commit/Usage (" + license.getContractId() + ")"));
						if(!StringHelper.isEmpty(rangeFilter)) {
							highchart.setSubtitle(new Subtitle("Last " + rangeFilter + " days"));
						}
						
						List<XAxis> xAxisList = new ArrayList<XAxis>();
						XAxis xaAxis = new XAxis();
						xaAxis.setType("datetime");
						xaAxis.setDateTimeLabelFormats(new DateTimeLabelFormats("%b", "%b %e", null, null, null, null, null));
						xAxisList.add(xaAxis);

						highchart.setxAxis(xAxisList);

						List<YAxis> yAxisList = new ArrayList<YAxis>();
						YAxis yAxis = new YAxis();
						yAxis.setTitle(new Title(""));
						yAxis.setMin(0);
						yAxis.setAllowDecimals(false);
						yAxisList.add(yAxis);

						highchart.setyAxis(yAxisList);
						highchart.setSeries(seriesList);

						highchart.setPlotOptions(new PlotOptions(new Spline(new Marker(3, true))));
						
						highcharts.add(highchart);
						
//						break;
					}
					
					

					Map<String, Object> podData = new HashMap<String, Object>();
					podData.put("highcharts", highcharts);

					PodDataCache newCache = new PodDataCache();
					newCache.setAccountId(account.getId());
					newCache.setPodId(podId);
					newCache.setParamHash(paramHash);
					newCache.setData(podData);
					newCache.setCreatedAt(new Date());

					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("podId", podId);
					fieldMap.put("paramHash", paramHash);
					cacheDAO.delete(fieldMap);
					newCache = cacheDAO.insert(newCache);
					long endTime = new Date().getTime();
					long diff = endTime - startTime;
//					logger.debug("by c360customerby Mrr Utilized [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

					return newCache.getData();
					
				}
			}
			return cache.getData();
		}*/
		
		//cdetailstotalusagecommittrafficseries
		
		public Map<String, Object> cdetailstotalusagecommittrafficseries(
				Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
			PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
			String params = "podId=" + podId;
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				params += "&rangeFilter=" + rangeFilter; 
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
				CustomerDAO customerDAO = new CustomerDAO();
				Customer customer = customerDAO.findOneById(customerId);
				DecimalFormat df1Precision = new DecimalFormat("#");
				SiteTimeseriesByDayDAO siteTimeseriesByDayDAO = new SiteTimeseriesByDayDAO();
			
				if(customer != null) {
					
					long startTime = new Date().getTime();
				
					Calendar calendar = Calendar.getInstance();		
					calendar.set(Calendar.HOUR_OF_DAY, 0);
					calendar.set(Calendar.MINUTE, 0);
					calendar.set(Calendar.SECOND, 0);
					calendar.set(Calendar.MILLISECOND, 0);
					
					Date rangeEnd = calendar.getTime();
					calendar.add(Calendar.DATE, -((!StringHelper.isEmpty(rangeFilter)) ? Integer.parseInt(rangeFilter) : 90));
					Date rangeStart = calendar.getTime();
					long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
					
					List<Series> seriesList = new ArrayList<Series>();
					Map<Date, Long> map = new TreeMap<Date, Long>();
					Map<Date, Long> usageMap = new TreeMap<Date, Long>();
					if(customer.getSites() != null && customer.getSites().size() > 0){

						for(Site site : customer.getSites()){
							List<SiteTimeseriesByDay> timeseriesData =  siteTimeseriesByDayDAO.getData(account.getId(), customerId, site.getNormalizedName(), "bandwidthCommit", rangeStart, rangeEnd);
							
							for(SiteTimeseriesByDay timeseries : timeseriesData) {
								if(!map.containsKey(timeseries.getDate()) && map.size() < days && timeseries.getFieldValue() != null) {
									map.put(timeseries.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
								} else if(map.containsKey(timeseries.getDate()) && timeseries.getFieldValue() != null) {
									map.put(timeseries.getDate(), map.get(timeseries.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
								}
							}
							//bandwidthUsage
							List<SiteTimeseriesByDay> usageTimeseriesData =  siteTimeseriesByDayDAO.getData(account.getId(), customerId, site.getNormalizedName(), "bandwidthUsage", rangeStart, rangeEnd);


							for(SiteTimeseriesByDay timeseries : usageTimeseriesData) {
								if(!usageMap.containsKey(timeseries.getDate()) && usageMap.size() < days && timeseries.getFieldValue() != null) {
									usageMap.put(timeseries.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
								} else if(usageMap.containsKey(timeseries.getDate()) && timeseries.getFieldValue() != null) {
									usageMap.put(timeseries.getDate(), usageMap.get(timeseries.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
								}
							}
							
							
						}
					}
					
					
					
					Series usageSeries = new Series();
					usageSeries.setName("Usage");
					usageSeries.setShowInLegend(true);
					List<Object> usageData = new ArrayList<Object>();
					for(Map.Entry<Date, Long> entry : usageMap.entrySet()) {
						Data dta = new Data();				
						dta.setX(entry.getKey().getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("Usage: " + entry.getValue().longValue());
						usageData.add(dta);
					}
					usageSeries.setData(usageData);
					
					seriesList.add(usageSeries);
					
					Series series = new Series();
					series.setName("commit");
					series.setShowInLegend(true);
					List<Object> data = new ArrayList<Object>();
					for(Map.Entry<Date, Long> entry : map.entrySet()) {
						Data dta = new Data();				
						dta.setX(entry.getKey().getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("Commit: " + entry.getValue().longValue());
						data.add(dta);
					}
					series.setData(data);
					seriesList.add(series);
					
					Highchart highchart = new Highchart();

					highchart.setChart(new Chart("spline", null));
					highchart.setTitle(new Title("Total Bandwidth Commit/Usage"));
					if(!StringHelper.isEmpty(rangeFilter)) {
						highchart.setSubtitle(new Subtitle("Last " + rangeFilter + " days"));
					}

					List<XAxis> xAxisList = new ArrayList<XAxis>();
					XAxis xaAxis = new XAxis();
					xaAxis.setType("datetime");
					xaAxis.setDateTimeLabelFormats(new DateTimeLabelFormats("%b", "%b %e", null, null, null, null, null));
					xAxisList.add(xaAxis);

					highchart.setxAxis(xAxisList);

					List<YAxis> yAxisList = new ArrayList<YAxis>();
					YAxis yAxis = new YAxis();
					yAxis.setTitle(new Title(""));
					yAxis.setMin(0);
					yAxis.setAllowDecimals(false);
					yAxisList.add(yAxis);

					highchart.setyAxis(yAxisList);
					highchart.setSeries(seriesList);

					highchart.setPlotOptions(new PlotOptions(new Spline(new Marker(3, true))));

					Map<String, Object> podData = new HashMap<String, Object>();
					podData.put("highchart", highchart);

					PodDataCache newCache = new PodDataCache();
					newCache.setAccountId(account.getId());
					newCache.setPodId(podId);
					newCache.setParamHash(paramHash);
					newCache.setData(podData);
					newCache.setCreatedAt(new Date());

					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("podId", podId);
					fieldMap.put("paramHash", paramHash);
					cacheDAO.delete(fieldMap);
					newCache = cacheDAO.insert(newCache);
					long endTime = new Date().getTime();
					long diff = endTime - startTime;
//					logger.debug("by cdetails by PU bought [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

					return newCache.getData();
					
				}
			}
			return cache.getData();
		}
	/*	public Map<String, Object> cdetailstotalusagecommittrafficseries(
				Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
			PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
			String params = "podId=" + podId;
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				params += "&rangeFilter=" + rangeFilter; 
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

				LicenseDAO licenseDAO = new LicenseDAO();
				CustomerDAO customerDAO = new CustomerDAO();
				Customer customer = customerDAO.findOneById(customerId);
				DecimalFormat df1Precision = new DecimalFormat("#");
				LicenseTimeseriesByDayDAO licenseTimeseriesByDayDAO = new LicenseTimeseriesByDayDAO();
				if(customer != null) {
					
					long startTime = new Date().getTime();
				
					Calendar calendar = Calendar.getInstance();		
					calendar.set(Calendar.HOUR_OF_DAY, 0);
					calendar.set(Calendar.MINUTE, 0);
					calendar.set(Calendar.SECOND, 0);
					calendar.set(Calendar.MILLISECOND, 0);
					
					Date rangeEnd = calendar.getTime();
					calendar.add(Calendar.DATE, -((!StringHelper.isEmpty(rangeFilter)) ? Integer.parseInt(rangeFilter) : 90));
					Date rangeStart = calendar.getTime();
					long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
					
					List<Series> seriesList = new ArrayList<Series>();
					
					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("customerId", customer.getId());
					List<License> licenses = licenseDAO.findAll(fieldMap);
					Map<Date, Long> map = new LinkedHashMap<Date, Long>();
					Map<Date, Long> usageMap = new LinkedHashMap<Date, Long>();
					for(License license : licenses) {						
						List<LicenseTimeseriesByDay> timeseriesData = licenseTimeseriesByDayDAO.getData(account.getId(), customer.getId(), license.getId(), "commit", rangeStart, rangeEnd);
						for(LicenseTimeseriesByDay timeseries : timeseriesData) {
							if(!map.containsKey(timeseries.getDate()) && map.size() < days && timeseries.getFieldValue() != null) {
								map.put(timeseries.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
							} else if(map.containsKey(timeseries.getDate()) && timeseries.getFieldValue() != null) {
								map.put(timeseries.getDate(), map.get(timeseries.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
							}
						}
						
						
						List<LicenseTimeseriesByDay> usageTimeseriesData = licenseTimeseriesByDayDAO.getData(account.getId(), customer.getId(), license.getId(), "wan_in_95th_mbps", rangeStart, rangeEnd);
						for(LicenseTimeseriesByDay timeseries : usageTimeseriesData) {
							if(!usageMap.containsKey(timeseries.getDate()) && usageMap.size() < days && timeseries.getFieldValue() != null) {
								usageMap.put(timeseries.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
							} else if(usageMap.containsKey(timeseries.getDate()) && timeseries.getFieldValue() != null) {
								usageMap.put(timeseries.getDate(), usageMap.get(timeseries.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(timeseries.getFieldValue().toString()))));
							}
						}
					}
					
			
					
					Series usageSeries = new Series();
					usageSeries.setName("Usage");
					usageSeries.setShowInLegend(true);
					List<Object> usageData = new ArrayList<Object>();
					for(Map.Entry<Date, Long> entry : usageMap.entrySet()) {
						Data dta = new Data();				
						dta.setX(entry.getKey().getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("Usage: " + entry.getValue().longValue());
						usageData.add(dta);
					}
					usageSeries.setData(usageData);
					
					seriesList.add(usageSeries);
					
					Series series = new Series();
					series.setName("commit");
					series.setShowInLegend(true);
					List<Object> data = new ArrayList<Object>();
					for(Map.Entry<Date, Long> entry : map.entrySet()) {
						Data dta = new Data();				
						dta.setX(entry.getKey().getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("Commit: " + entry.getValue().longValue());
						data.add(dta);
					}
					series.setData(data);
					seriesList.add(series);
					
					Highchart highchart = new Highchart();

					highchart.setChart(new Chart("spline", null));
					highchart.setTitle(new Title("Total Bandwidth Commit/Usage"));
					if(!StringHelper.isEmpty(rangeFilter)) {
						highchart.setSubtitle(new Subtitle("Last " + rangeFilter + " days"));
					}

					List<XAxis> xAxisList = new ArrayList<XAxis>();
					XAxis xaAxis = new XAxis();
					xaAxis.setType("datetime");
					xaAxis.setDateTimeLabelFormats(new DateTimeLabelFormats("%b", "%b %e", null, null, null, null, null));
					xAxisList.add(xaAxis);

					highchart.setxAxis(xAxisList);

					List<YAxis> yAxisList = new ArrayList<YAxis>();
					YAxis yAxis = new YAxis();
					yAxis.setTitle(new Title(""));
					yAxis.setMin(0);
					yAxis.setAllowDecimals(false);
					yAxisList.add(yAxis);

					highchart.setyAxis(yAxisList);
					highchart.setSeries(seriesList);

					highchart.setPlotOptions(new PlotOptions(new Spline(new Marker(3, true))));

					Map<String, Object> podData = new HashMap<String, Object>();
					podData.put("highchart", highchart);

					PodDataCache newCache = new PodDataCache();
					newCache.setAccountId(account.getId());
					newCache.setPodId(podId);
					newCache.setParamHash(paramHash);
					newCache.setData(podData);
					newCache.setCreatedAt(new Date());

					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("podId", podId);
					fieldMap.put("paramHash", paramHash);
					cacheDAO.delete(fieldMap);
					newCache = cacheDAO.insert(newCache);
					long endTime = new Date().getTime();
					long diff = endTime - startTime;
//					logger.debug("by cdetails by PU bought [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

					return newCache.getData();
					
				}
			}
			return cache.getData();
		}*/
		
		public Map<String, Object> cdetailssitemetric(
				Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
			PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
			String params = "podId=" + podId;
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				params += "&rangeFilter=" + rangeFilter; 
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
				cacheDAO.delete(fieldMap);

				Customer customer = new CustomerDAO().findOneById(customerId);
				if(customer != null) {
					Map<String, Object> accountFeatures = new AdminService().accountFeatures(account);

					long siteCount = 0;
					
					if(customer.getSites() != null && customer.getSites().size() > 0){
						siteCount = customer.getSites().size();
					}
					

					Map<String, Object> podData = new HashMap<String, Object>();
					podData.put("customer", customer);
					podData.put("siteCount", siteCount);

					PodDataCache newCache = new PodDataCache();
					newCache.setAccountId(account.getId());
					newCache.setPodId(podId);
					newCache.setParamHash(paramHash);
					newCache.setData(podData);
					newCache.setCreatedAt(new Date());

					newCache = cacheDAO.insert(newCache);
					return newCache.getData();
				}
			}
			return cache.getData();
		}
		
		
		public Map<String, Object> cdetailscontractmetric(
				Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
			PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
			String params = "podId=" + podId;
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				params += "&rangeFilter=" + rangeFilter; 
			}
			if(!StringHelper.isEmpty(customerId)) {
				params += "&customerId=" + customerId;
			}
			String paramHash = CryptoHelper.getMD5Hash(params);
			LicenseDAO licenseDAO = new LicenseDAO();
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
				cacheDAO.delete(fieldMap);

				Customer customer = new CustomerDAO().findOneById(customerId);
				if(customer != null) {
				

					long contractCount = 0;
					
					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("customerId", customer.getId());
					List<License> licenses = licenseDAO.findAll(fieldMap);
					if(licenses.size() > 0){
					contractCount = licenses.size();
					}
					

					Map<String, Object> podData = new HashMap<String, Object>();
					podData.put("customer", customer);
					podData.put("contractCount", contractCount);

					PodDataCache newCache = new PodDataCache();
					newCache.setAccountId(account.getId());
					newCache.setPodId(podId);
					newCache.setParamHash(paramHash);
					newCache.setData(podData);
					newCache.setCreatedAt(new Date());

					newCache = cacheDAO.insert(newCache);
					return newCache.getData();
				}
			}
			return cache.getData();
		}
		
		public Map<String, Object> cdetailsupsellpotentialmetric(Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
			PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
			String params = "podId=" + podId;
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				params += "&rangeFilter=" + rangeFilter; 
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
				
				Customer customer = new CustomerDAO().findOneById(customerId);
				if(customer != null) {
					
					Map<String, Object> metricsData = new HashMap<String, Object>();

					long bandwidthCommit = 0;
					long bandwidthUsage = 0;
					
					if(customer.getSites() != null) {
						for(Site site : customer.getSites()) {
							if(site.getBandwidthCommit() != null) {
								bandwidthCommit += site.getBandwidthCommit();
							}
							if(site.getBandwidthUsage() != null) {
								bandwidthUsage += site.getBandwidthUsage();
							}
						}
					}

					metricsData.put("value01", (bandwidthUsage > bandwidthCommit) ? "True" : "False");
					metricsData.put("value02", NumberHelper.format(bandwidthCommit));
					metricsData.put("value03", NumberHelper.format(bandwidthUsage));

					metricsData.put("label01Url", null);
					metricsData.put("label02Url", null);
					metricsData.put("label03Url", null);
					
					metricsData.put("stringValue01", "yes");

					metricsData.put("customer", customer);

					PodDataCache newCache = new PodDataCache();
					newCache.setAccountId(account.getId());
					newCache.setPodId(podId);
					newCache.setParamHash(paramHash);
					newCache.setData(metricsData);
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
			return cache.getData();
		}
}

