
package com.crucialbits.cy.custom.service;

import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.crucialbits.chart.highchart.Style;
import com.crucialbits.chart.highchart.Subtitle;
import com.crucialbits.chart.highchart.Title;
import com.crucialbits.chart.highchart.XAxis;
import com.crucialbits.chart.highchart.YAxis;
import com.crucialbits.cy.app.Constants;
import com.crucialbits.cy.app.Utility;
import com.crucialbits.cy.dao.CustomerDAO;
import com.crucialbits.cy.dao.CustomerGroupDAO;
import com.crucialbits.cy.dao.CustomerTimeseriesByDayDAO;
import com.crucialbits.cy.dao.InformationDAO;
import com.crucialbits.cy.dao.LicenseDAO;
import com.crucialbits.cy.dao.PodDataCacheDAO;
import com.crucialbits.cy.dao.TempDAO;
import com.crucialbits.cy.dao.TimeSeriesDAO;
import com.crucialbits.cy.dao.TimeSeriesDateDAO;
import com.crucialbits.cy.dao.TimeseriesByDayDAO;
import com.crucialbits.cy.model.Account;
import com.crucialbits.cy.model.Customer;
import com.crucialbits.cy.model.CustomerGroup;
import com.crucialbits.cy.model.CustomerTimeseriesByDay;
import com.crucialbits.cy.model.CustomerTimeseriesByDayContentDoc;
import com.crucialbits.cy.model.Field;
import com.crucialbits.cy.model.Information;
import com.crucialbits.cy.model.License;
import com.crucialbits.cy.model.PodConfiguration;
import com.crucialbits.cy.model.PodDataCache;
import com.crucialbits.cy.model.Properties;
import com.crucialbits.cy.model.Temp;
import com.crucialbits.cy.model.TimeSeries;
import com.crucialbits.cy.model.TimeSeriesDate;
import com.crucialbits.cy.model.TimeseriesByDay;
import com.crucialbits.cy.search.CustomerTimeseriesByDayContentIndex;
import com.crucialbits.solr.SearchResult;
import com.crucialbits.util.CryptoHelper;
import com.crucialbits.util.NumberHelper;
import com.crucialbits.util.StringHelper;
import com.mongodb.BasicDBObject;

public class EgnytePodService implements Constants{

	private final static Logger logger = Logger.getLogger(EgnytePodService.class);	

	public Map<String, Object> c360customerbyindustryegyte(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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
			
			CustomerDAO customerDAO = new CustomerDAO();
			Set<Object> industries = customerDAO.getDistinctCustomFieldValuesEnhanced(account.getId(), "industry", paramMap);


			Map<String, Long> countsMap = new HashMap<String, Long>();
			Map<String, Long> dataMap = new HashMap<String, Long>();
			for(Object industry : industries) {
				dataMap.put(industry.toString(), customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "industry", industry));
				countsMap.put(industry.toString(), 0l);
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
				String url = "/managecustomers?c__industry=" + entry.getKey();
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						url +=  "&"+mp.getKey()+"=" + mp.getValue();							
					}
				}
				
				d.setUrl(url);
				xAxis.add(name);
				data.add(d);
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(true);
			seriesList.add(series);

			series = new Series();
			series.setType("spline");
			series.setName("Power Users");
			series.setyAxis(1l);
			series.setShowInLegend(true);

			data = new ArrayList<Object>();
			for(Map.Entry<String, Long> entry : Utility.getInstance().sortByValues(dataMap).entrySet()) {
				if(data.size() > 20) {
					break;
				}
				Data d = new Data();
				d.setName(entry.getKey());
				d.setY(countsMap.get(entry.getKey()));
				data.add(d);
			}

			series.setData(data);
			seriesList.add(series);

			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by Industry"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
			//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
			yAxisList.add(new YAxis(new Title("No. of Power Users"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, -90, -15, "#AAA"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerbyindustryegyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360customerbyregionegnyte(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object>  paramMap, Integer cacheHours) throws Exception {

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
			
			CustomerDAO customerDAO = new CustomerDAO();
			Set<Object> regions = customerDAO.getDistinctCustomFieldValuesEnhanced(account.getId(), "region", paramMap);


			Map<String, Long> countsMap = new HashMap<String, Long>();
			Map<String, Long> dataMap = new HashMap<String, Long>();
			for(Object region : regions) {
				dataMap.put(region.toString(), customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "region", region));
				countsMap.put(region.toString(), 0l);
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
				String url = "/managecustomers?c__region=" + entry.getKey();
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						url +=  "&"+mp.getKey()+"=" + mp.getValue();							
					}
				}
				
				d.setUrl(url);
				xAxis.add(name);
				data.add(d);
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(true);
			seriesList.add(series);

			series = new Series();
			series.setType("spline");
			series.setName("Active Links");
			series.setyAxis(1l);
			series.setShowInLegend(true);

			data = new ArrayList<Object>();
			for(Map.Entry<String, Long> entry : Utility.getInstance().sortByValues(dataMap).entrySet()) {
				if(data.size() > 20) {
					break;
				}
				Data d = new Data();
				d.setName(entry.getKey());
				d.setY(countsMap.get(entry.getKey()));
				data.add(d);
			}

			series.setData(data);
			seriesList.add(series);

			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by Region"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
			//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
			yAxisList.add(new YAxis(new Title("No. of Active Links"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, -90, -15, "#AAA"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerbyregionegnyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360customersegmentegnyte(
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
			
			CustomerDAO customerDAO = new CustomerDAO();
			Set<Object> segments = customerDAO.getDistinctCustomFieldValuesEnhanced(account.getId(), "customer_segment", paramMap);


			Map<String, Long> countsMap = new HashMap<String, Long>();
			Map<String, Long> dataMap = new HashMap<String, Long>();
			for(Object segment : segments) {
				dataMap.put(segment.toString(), customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "customer_segment", segment));
				countsMap.put(segment.toString(), 0l);
			}

			List<Object> data = new ArrayList<Object>();
			List<Object> xAxis = new ArrayList<Object>();

			
			for(Map.Entry<String, Long> entry : Utility.getInstance().sortByValues(dataMap).entrySet()) {
				if(data.size() > 25) {
					break;
				}
				Data d = new Data();
				d.setY(entry.getValue());
				String name = entry.getKey().toString();
				if(name.length() > 15) {
					name = name.substring(0, 15) + "...";
				}
				d.setName(name);
				String url = "/managecustomers?c__customer_segment=" + entry.getKey();
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						url +=  "&"+mp.getKey()+"=" + mp.getValue();							
					}
				}				
				d.setUrl(url);
				xAxis.add(name);
				data.add(d);
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(true);
			seriesList.add(series);

			series = new Series();
			series.setType("spline");
			series.setName("Groups");
			series.setyAxis(1l);
			series.setShowInLegend(true);

			data = new ArrayList<Object>();
			for(Map.Entry<String, Long> entry : dataMap.entrySet()) {
				Data d = new Data();
				d.setName(entry.getKey());
				d.setY(countsMap.get(entry.getKey()));
				data.add(d);
			}

			series.setData(data);
			seriesList.add(series);

			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by Segment"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
			//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
			yAxisList.add(new YAxis(new Title("No. of Groups"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, -90, -15, "#AAA"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customersegmentegnyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360customerbystorageexceeded(
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
			
			CustomerDAO customerDAO = new CustomerDAO();
			long trueCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "storage_exceeded", true);
			long falseCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "storage_exceeded", false);

			List<Object> data = new ArrayList<Object>();
			Data d = new Data();
			d.setName("Yes");
			d.setY(trueCount);
			String url = "/managecustomers?c__storage_exceeded=true";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			
			d.setUrl(url);
			d.setColor("#D9534F");
			data.add(d);
			
			d = new Data();
			d.setName("No");
			d.setY(falseCount);
			url = "/managecustomers?c__storage_exceeded=false";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			
			d.setUrl(url);
			d.setColor("#1CAF9A");
			data.add(d);

			//constructing graph object
			Highchart highchart = new Highchart();
			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			seriesList.add(series);

			highchart.setChart(new Chart("pie", new Options3d(45, true)));
			highchart.setTitle(new Title("Storage Status"));
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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerbystorageexceeded [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
		}

		return cache.getData();
	}
	
	public Map<String, Object> c360customerbyadvancedauthentication(
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
			
			CustomerDAO customerDAO = new CustomerDAO();
			long trueCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_advanced_authentication_enabled", true);
			long falseCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_advanced_authentication_enabled", false);

			List<Object> data = new ArrayList<Object>();
			Data d = new Data();
			d.setName("Yes");
			d.setY(trueCount);
			String url = "/managecustomers?c__is_advanced_authentication_enabled=true";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			d.setUrl(url);
			d.setColor("#1CAF9A");
			data.add(d);
			
			d = new Data();
			d.setName("No");
			d.setY(falseCount);
			url = "/managecustomers?c__is_advanced_authentication_enabled=false";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			
			d.setUrl(url);
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
			highchart.setTitle(new Title("Adv. Auth Status"));
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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerbyadvancedauthentication [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
			//end
		}

		return cache.getData();
	}
	
	public Map<String, Object> c360customerbyoutlookintegration(
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
			
			CustomerDAO customerDAO = new CustomerDAO();
			long trueCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_outlook_integration_enabled", true);
			long falseCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_outlook_integration_enabled", false);

			List<Object> data = new ArrayList<Object>();
			Data d = new Data();
			d.setName("Yes");
			d.setY(trueCount);
			String url = "/managecustomers?c__is_outlook_integration_enabled=true";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			
			d.setUrl(url);
			d.setColor("#1CAF9A");
			data.add(d);
			
			d = new Data();
			d.setName("No");
			d.setY(falseCount);
			url = "/managecustomers?c__is_outlook_integration_enabled=false";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			d.setUrl(url);
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
			highchart.setTitle(new Title("Outlook Integration"));
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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerbyoutlookintegration [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
			//end
		}

		return cache.getData();
	}
	
	public Map<String, Object> c360customerbysalesforceintegration(
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
			
			CustomerDAO customerDAO = new CustomerDAO();
			long trueCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_salesforce_integration_enabled", true);
			long falseCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_salesforce_integration_enabled", false);

			List<Object> data = new ArrayList<Object>();
			Data d = new Data();
			d.setName("Yes");
			d.setY(trueCount);
			String url = "/managecustomers?c__is_salesforce_integration_enabled=true";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}

			
			d.setUrl(url);
			d.setColor("#1CAF9A");
			data.add(d);
			
			d = new Data();
			d.setName("No");
			d.setY(falseCount);
			url = "/managecustomers?c__is_salesforce_integration_enabled=false";
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}

			d.setUrl(url);
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
			highchart.setTitle(new Title("Salesforce Integration"));
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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerbysalesforceintegration [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
			//end
		}

		return cache.getData();
	}
	
	public Map<String, Object> c360customerby90powerusersegnyte(
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
			
			CustomerDAO customerDAO = new CustomerDAO();
			long trueCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "90_power_users", true);
			long falseCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "90_power_users", false);
			
			List<Object> data = new ArrayList<Object>();
			Data d = new Data();
			d.setName("Yes");
			d.setY(trueCount);
			String url = "/managecustomers?c__90_power_users=true";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			
			d.setUrl(url);
			d.setColor("#1CAF9A");
			data.add(d);
			
			d = new Data();
			d.setName("No");
			d.setY(falseCount);
			url = "/managecustomers?c__90_power_users=false";
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			d.setUrl(url);
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
			highchart.setTitle(new Title("90% Power Users"));
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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerby90powerusersegnyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
			//end
		}

		return cache.getData();
	}
	
	public Map<String, Object> c360customerbydevicecontrolegnyte(
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
			
			CustomerDAO customerDAO = new CustomerDAO();
			long trueCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_device_control_enabled", true);
			long falseCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_device_control_enabled", false);

			List<Object> data = new ArrayList<Object>();
			Data d = new Data();
			d.setName("Yes");
			d.setY(trueCount);
			String url = "/managecustomers?c__is_device_control_enabled=true";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			
			d.setUrl(url);
			d.setColor("#1CAF9A");
			data.add(d);
			
			d = new Data();
			d.setName("No");
			d.setY(falseCount);
			url = "/managecustomers?c__is_device_control_enabled=false";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			d.setUrl(url);
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
			highchart.setTitle(new Title("Device Control Status"));
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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerbydevicecontrolegnyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
			//end
		}

		return cache.getData();
	}
	
	public Map<String, Object> c360customerbyelcenabledegnyte(
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
			
			CustomerDAO customerDAO = new CustomerDAO();
			long trueCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_elc_enabled", true);
			long falseCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_elc_enabled", false);

			List<Object> data = new ArrayList<Object>();
			Data d = new Data();
			d.setName("Yes");
			d.setY(trueCount);
			String url = "/managecustomers?c__is_elc_enabled=true";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			
			d.setUrl(url);
			d.setColor("#1CAF9A");
			data.add(d);
			
			d = new Data();
			d.setName("No");
			d.setY(falseCount);
			url = "/managecustomers?c__is_elc_enabled=false";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			d.setUrl(url);
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
			highchart.setTitle(new Title("ELC Status"));
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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerbyelcenabledegnyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
			//end
		}

		return cache.getData();
	}
	
	public Map<String, Object> c360customerbyolcenabledegnyte(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object>  paramMap, Integer cacheHours) throws Exception {


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
			
			CustomerDAO customerDAO = new CustomerDAO();
			long trueCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_olc_enabled", true);
			long falseCount = customerDAO.countByCustomFieldNameAndValue(account.getId(), paramMap, "is_olc_enabled", false);

			List<Object> data = new ArrayList<Object>();
			Data d = new Data();
			d.setName("Yes");
			d.setY(trueCount);
			String url = "/managecustomers?c__is_olc_enabled=true";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			
			d.setUrl(url);
			d.setColor("#1CAF9A");
			data.add(d);
			
			d = new Data();
			d.setName("No");
			d.setY(falseCount);
			url = "/managecustomers?c__is_olc_enabled=false";
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			d.setUrl(url);
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
			highchart.setTitle(new Title("OLC Status"));
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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerbyolcenabledegnyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
			//end
		}

		return cache.getData();
	}
	
	public Map<String, Object> c360sumetric(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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
			
			CustomerDAO customerDAO = new CustomerDAO();
			Map<String, Object> metricsData = new HashMap<String, Object>();

			long suBought = 0;
			long suUtilized = 0;
			
			int skip = 0;
			int limit = 200;
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
					if(customer.getCustomFields() != null) {
						for(Field field : customer.getCustomFields()) {
							if(field.getName().equals("su_bought") && field.getValue() != null) {
								suBought += Long.parseLong(field.getValue().toString());
							} else if(field.getName().equals("su_utilized") && field.getValue() != null) {
								suUtilized += Long.parseLong(field.getValue().toString());
							}
						}
					}
				}
				skip += limit;
			}

			metricsData.put("value01", NumberHelper.format(suBought));
			metricsData.put("value02", NumberHelper.format(suUtilized));
			metricsData.put("value03", Utility.getInstance().calculateUpDownPercentage(suUtilized, suBought));

			/*metricsData.put("label01Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType + "&status=open");
			metricsData.put("label02Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType + "&status=pending");
			metricsData.put("label03Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType);*/

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360sumetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360pumetric(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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
			
			CustomerDAO customerDAO = new CustomerDAO();
			Map<String, Object> metricsData = new HashMap<String, Object>();

			long puBought = 0;
			long puUtilized = 0;
			
			int skip = 0;
			int limit = 200;
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
					if(customer.getCustomFields() != null) {
						for(Field field : customer.getCustomFields()) {
							if(field.getName().equals("pu_bought") && field.getValue() != null) {
								puBought += Long.parseLong(field.getValue().toString());
							} else if(field.getName().equals("pu_utilized") && field.getValue() != null) {
								puUtilized += Long.parseLong(field.getValue().toString());
							}
						}
					}
				}
				skip += limit;
			}

			metricsData.put("value01", NumberHelper.format(puBought));
			metricsData.put("value02", NumberHelper.format(puUtilized));
			metricsData.put("value03", Utility.getInstance().calculateUpDownPercentage(puUtilized, puBought));

			/*metricsData.put("label01Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType + "&status=open");
			metricsData.put("label02Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType + "&status=pending");
			metricsData.put("label03Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType);*/

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360pumetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));
			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360activeuserloginsmetric(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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
			
			CustomerDAO customerDAO = new CustomerDAO();
			Map<String, Object> metricsData = new HashMap<String, Object>();

			long activeUsers = 0;
			long powerUsers = 0;
			long standardUsers = 0;
			
			int skip = 0;
			int limit = 200;
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
					if(customer.getCustomFields() != null) {
						for(Field field : customer.getCustomFields()) {
							if(field.getName().equals("active_users") && field.getValue() != null) {
								activeUsers += Long.parseLong(field.getValue().toString());
							} else if(field.getName().equals("num_power_users") && field.getValue() != null) {
								powerUsers += Long.parseLong(field.getValue().toString());
							} else if(field.getName().equals("num_std_users") && field.getValue() != null) {
								standardUsers += Long.parseLong(field.getValue().toString());
							}
						}
					}
				}
				skip += limit;
			}
			
			metricsData.put("value01", NumberHelper.format(activeUsers));
			metricsData.put("value02", NumberHelper.format(powerUsers));
			metricsData.put("value03", NumberHelper.format(standardUsers));

			/*metricsData.put("label01Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType + "&status=open");
			metricsData.put("label02Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType + "&status=pending");
			metricsData.put("label03Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType);*/

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360activeuserloginsmetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
		}
		return cache.getData();
	}

	public Map<String, Object> cdetailssumetric(Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
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
//			long startTime = new Date().getTime();
			
			Customer customer = new CustomerDAO().findOneById(customerId);
			if(customer != null) {
				
				Map<String, Object> metricsData = new HashMap<String, Object>();

				long suBought = 0;
				long suUtilized = 0;
				
				if(customer.getCustomFields() != null) {
					for(Field field : customer.getCustomFields()) {
						if(field.getName().equals("su_bought") && field.getValue() != null) {
							suBought = Long.parseLong(field.getValue().toString());
						} else if(field.getName().equals("su_utilized") && field.getValue() != null) {
							suUtilized = Long.parseLong(field.getValue().toString());
						}
					}
				}
				
				if(suBought >= 99999) {
					suBought = suUtilized + ((suUtilized * 40) / 100);
				}

				metricsData.put("value01", NumberHelper.format(suBought));
				metricsData.put("value02", NumberHelper.format(suUtilized));
				metricsData.put("value03", Utility.getInstance().calculateUpDownPercentage(suUtilized, suBought));

				metricsData.put("label01Url", null);
				metricsData.put("label02Url", null);
				metricsData.put("label03Url", null);

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
				
//				long endTime = new Date().getTime();
//				long diff = endTime - startTime;
//				logger.debug("by cdetailssumetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
			}
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailspumetric(Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
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
//			long startTime = new Date().getTime();
			
			Customer customer = new CustomerDAO().findOneById(customerId);
			if(customer != null) {
				
				Map<String, Object> metricsData = new HashMap<String, Object>();

				long puBought = 0;
				long puUtilized = 0;
				
				if(customer.getCustomFields() != null) {
					for(Field field : customer.getCustomFields()) {
						if(field.getName().equals("pu_bought") && field.getValue() != null) {
							puBought = Long.parseLong(field.getValue().toString());
						} else if(field.getName().equals("pu_utilized") && field.getValue() != null) {
							puUtilized = Long.parseLong(field.getValue().toString());
						}
					}
				}
				
				if(puBought >= 99999) {
					puBought = puUtilized + ((puUtilized * 40) / 100);
				}

				metricsData.put("value01", NumberHelper.format(puBought));
				metricsData.put("value02", NumberHelper.format(puUtilized));
				metricsData.put("value03", Utility.getInstance().calculateUpDownPercentage(puUtilized, puBought));

				metricsData.put("label01Url", null);
				metricsData.put("label02Url", null);
				metricsData.put("label03Url", null);

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
				
//				long endTime = new Date().getTime();
//				long diff = endTime - startTime;
//				logger.debug("by cdetailsspumetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
			}
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsactiveuserloginsmetric(Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
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
//			long startTime = new Date().getTime();
			
			Customer customer = new CustomerDAO().findOneById(customerId);
			if(customer != null) {
				
				Map<String, Object> metricsData = new HashMap<String, Object>();

				long activeUsers = 0;
				long powerUsers = 0;
				long standardUsers = 0;
				
				/*if(customer.getFieldGroups() != null) {
					for(FieldGroup fg : customer.getFieldGroups()) {
						if(fg.getFields() != null) {
							for(Field field : fg.getFields()) {
								if(field.getName().equals("active_users") && field.getResponse() != null) {
									activeUsers += Long.parseLong(field.getResponse().toString());
								} else if(field.getName().equals("num_power_users") && field.getResponse() != null) {
									powerUsers += Long.parseLong(field.getResponse().toString());
								} else if(field.getName().equals("num_std_users") && field.getResponse() != null) {
									standardUsers += Long.parseLong(field.getResponse().toString());
								}
							}
						}
					}
				}*/
				
				if(customer.getCustomFields() != null) {
					for(Field field : customer.getCustomFields()) {
						if(field.getName().equals("active_users") && field.getValue() != null) {
							activeUsers += Long.parseLong(field.getValue().toString());
						} else if(field.getName().equals("num_power_users") && field.getValue() != null) {
							powerUsers += Long.parseLong(field.getValue().toString());
						} else if(field.getName().equals("num_std_users") && field.getValue() != null) {
							standardUsers += Long.parseLong(field.getValue().toString());
						}
					}
				}

				metricsData.put("value01", NumberHelper.format(activeUsers));
				metricsData.put("value02", NumberHelper.format(powerUsers));
				metricsData.put("value03", NumberHelper.format(standardUsers));

				metricsData.put("label01Url", null);
				metricsData.put("label02Url", null);
				metricsData.put("label03Url", null);

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
				
//				long endTime = new Date().getTime();
//				long diff = endTime - startTime;
//				logger.debug("by cdetailsactiveuserloginsmetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
			}
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsstoragemetric(Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
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
//			long startTime = new Date().getTime();
			
			Customer customer = new CustomerDAO().findOneById(customerId);
			if(customer != null) {
				
				Map<String, Object> metricsData = new HashMap<String, Object>();

				Double storageBought = 0d;
				Double storageUtilized = 0d;

				if(customer.getCustomFields() != null) {
					for(Field field : customer.getCustomFields()) {
						if(field.getName().equals("storage_bought_mb_") && field.getValue() != null) {
							storageBought = Double.parseDouble(field.getValue().toString());
						} else if(field.getName().equals("storage_used_mb_") && field.getValue() != null) {
							storageUtilized = Double.parseDouble(field.getValue().toString());
						}
					}
				}

				if(storageBought >= 99999) {
					storageBought = storageUtilized + ((storageUtilized * 40) / 100);
				}
				
				metricsData.put("value01", NumberHelper.format(storageBought.longValue()));
				metricsData.put("value02", NumberHelper.format(storageUtilized.longValue()));
				metricsData.put("value03", Utility.getInstance().calculateUpDownPercentage(storageUtilized, storageBought));

				metricsData.put("label01Url", null);
				metricsData.put("label02Url", null);
				metricsData.put("label03Url", null);

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
				
//				long endTime = new Date().getTime();
//				long diff = endTime - startTime;
//				logger.debug("by cdetailsstoragemetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
			}
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsstoragelinksmetric(Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
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
//			long startTime = new Date().getTime();
			
			Customer customer = new CustomerDAO().findOneById(customerId);
			if(customer != null) {
				
				Map<String, Object> metricsData = new HashMap<String, Object>();

				long downloads = 0;
				long files = 0;
				long activeUsers = 0;
				
				/*if(customer.getFieldGroups() != null) {
					for(FieldGroup fg : customer.getFieldGroups()) {
						if(fg.getFields() != null) {
							for(Field field : fg.getFields()) {
								if(field.getName().equals("total_downloads") && field.getResponse() != null) {
									downloads += Long.parseLong(field.getResponse().toString());
								} else if(field.getName().equals("num_files") && field.getResponse() != null) {
									files += Long.parseLong(field.getResponse().toString());
								} else if(field.getName().equals("num_folders") && field.getResponse() != null) {
									folders += Long.parseLong(field.getResponse().toString());
								}
							}
						}
					}
				}*/
				
				if(customer.getCustomFields() != null) {
					for(Field field : customer.getCustomFields()) {
						if(field.getName().equals("total_downloads") && field.getValue() != null) {
							downloads += Long.parseLong(field.getValue().toString());
						} else if(field.getName().equals("num_files") && field.getValue() != null) {
							files += Long.parseLong(field.getValue().toString());
						} else if(field.getName().equals("active_users") && field.getValue() != null) {
							activeUsers += Long.parseLong(field.getValue().toString());
						}
					}
				}

				metricsData.put("value01", NumberHelper.format(activeUsers));
				metricsData.put("value02", NumberHelper.format(files));
				metricsData.put("value03", NumberHelper.format(downloads));

				metricsData.put("label01Url", null);
				metricsData.put("label02Url", null);
				metricsData.put("label03Url", null);

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
				
//				long endTime = new Date().getTime();
//				long diff = endTime - startTime;
//				logger.debug("by cdetailsstoragelinksmetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
			}
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360activeuserdownloadmetric(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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
			
			CustomerDAO customerDAO = new CustomerDAO();
			Map<String, Object> metricsData = new HashMap<String, Object>();

			long files = 0;
			long activeUsers = 0;
			long downloads = 0;
			
			int skip = 0;
			int limit = 200;
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
					if(customer.getCustomFields() != null) {
						for(Field field : customer.getCustomFields()) {
							if(field.getName().equals("num_files") && field.getValue() != null) {
								files += Long.parseLong(field.getValue().toString());
							} else if(field.getName().equals("total_downloads") && field.getValue() != null) {
								downloads += Long.parseLong(field.getValue().toString());
							} else if(field.getName().equals("active_users") && field.getValue() != null) {
								activeUsers += Long.parseLong(field.getValue().toString());
							}
						}
					}
				}
				skip += limit;
			}

			metricsData.put("value01", NumberHelper.format(activeUsers));
			metricsData.put("value02", NumberHelper.format(files));
			metricsData.put("value03", NumberHelper.format(downloads));

			/*metricsData.put("label01Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType + "&status=open");
			metricsData.put("label02Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType + "&status=pending");
			metricsData.put("label03Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType);*/

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360activeuserdownloadmetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	
	public Map<String, Object> c360puboughttrafficseriesegnyte(
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
			DecimalFormat df1Precision = new DecimalFormat("#");			
			SimpleDateFormat sdf = new SimpleDateFormat("MMM dd, yyyy");
			List<Long> values = new ArrayList<Long>();
	
			Calendar cal1 = Calendar.getInstance();		
			cal1.set(Calendar.HOUR_OF_DAY, 0);
			cal1.set(Calendar.AM_PM, 0);
			cal1.set(Calendar.HOUR, 0);
			cal1.set(Calendar.MINUTE, 0);
			cal1.set(Calendar.SECOND, 0);
			cal1.set(Calendar.MILLISECOND, 0);
			cal1.add(Calendar.DATE, -90);
			Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();
			for(int p = 0; p < 90; p++){				
				values.add(0l);
				cal1.add(Calendar.DATE, 1);
				pbMap.put(cal1.getTime(), 0l);								
			}
			
			int skip = 0;
			int limit = 200;

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
					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("customerId", customer.getId());

					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					fieldMap.put("customerId", customer.getId());
					fieldMap.put("fieldName", "pu_bought");
					List<TimeSeriesDate> timeSeriesList = new TimeSeriesDateDAO().findAll(fieldMap, "timestamp", true);

					if(timeSeriesList.size() > 0  && timeSeriesList.size() <= values.size()){
						for(int i = 0 ; i < timeSeriesList.size() ; i++){
							if(timeSeriesList.get(i) != null && timeSeriesList.get(i).getValue() != null){			
								long val =	Long.parseLong(df1Precision.format(Double.parseDouble(timeSeriesList.get(i).getValue().toString())));
								values.set(i, values.get(i) + val);
								if(pbMap.containsKey(timeSeriesList.get(i).getTimestampDate())){
									pbMap.put(timeSeriesList.get(i).getTimestampDate(), pbMap.get(timeSeriesList.get(i).getTimestampDate())+ values.get(i) + val);
								}								
							}							
						}												
					}									
				}
											
				skip += limit;
			}
			
			List<Object> xAxis = new ArrayList<Object>();
			Map<String, Long> prodMap = new LinkedHashMap<String, Long>(); 
			
			for(Map.Entry<Date, Long> mp :pbMap.entrySet()){
				if(mp.getValue() != null){
				prodMap.put(sdf.format(mp.getKey()), mp.getValue());
				xAxis.add(sdf.format(mp.getKey()));
				}
			}
			
			
			Series series = new Series();
			series.setName("Traffic");
			List<Object> data = new ArrayList<Object>();
			for(Map.Entry<String, Long> entry : prodMap.entrySet()) {
				Data dta = new Data();					
				Date d = sdf.parse(entry.getKey());				
				dta.setX(d.getTime());					
				dta.setY(entry.getValue().longValue());
				dta.setInfo("Traffic: " + entry.getValue().longValue());
				data.add(dta);
			}
				
			series.setData(data);
			series.setColor("#FFB90F");
			series.setShowInLegend(false);
			List<Series> seriesList = new ArrayList<Series>();
			seriesList.add(series);

			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("spline", null));
			highchart.setTitle(new Title("PUBought traffic series 90 days"));

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
			return newCache.getData();
		}
		return cache.getData();
	}
	
	
	public Map<String, Object> cdetailspuboughttrafficseriesegnyte(
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

			if(customer != null) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				fieldMap.put("customerId", customerId);
				fieldMap.put("fieldName", "pu_bought");
                
				
				List<TimeSeries> timeSeriesList = new TimeSeriesDAO().findAll(fieldMap, "timestamp", true);				
				Map<String, Long> prodMap = new LinkedHashMap<String, Long>(); 
				List<Object> xAxis = new ArrayList<Object>();
				SimpleDateFormat sdf11 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				if(timeSeriesList.size() > 0){					
					for(TimeSeries timeSeries :  timeSeriesList){
						if(timeSeries.getValue() != null){
							long val =	Long.parseLong(df1Precision.format(Double.parseDouble(timeSeries.getValue().toString())));
							Date d = sdf11.parse(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(timeSeries.getTimestamp()));
							String dStr = sdf11.format(d);	  
							prodMap.put(dStr, val);
							xAxis.add(dStr);
						}
					}
										
					Series series = new Series();
					series.setName("Traffic");
					List<Object> data = new ArrayList<Object>();
					for(Map.Entry<String, Long> entry : prodMap.entrySet()) {
						Data dta = new Data();					
						Date d = sdf11.parse(entry.getKey());				
						dta.setX(d.getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("PU Bought: " + entry.getValue().longValue());
						data.add(dta);
					}
						
					
					series.setData(data);
					series.setColor("#FFB90F");
					series.setShowInLegend(false);
					List<Series> seriesList = new ArrayList<Series>();
					seriesList.add(series);
	
					Highchart highchart = new Highchart();
					highchart.setChart(new Chart("spline", null));
					highchart.setTitle(new Title("PU Bought traffic series 90 days"));
	
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
					return newCache.getData();
		
				
			 }
				
			}
		}
		return cache.getData();
	}
	
	
	
	public Map<String, Object> cdetailssuboughttrafficseriesegnyte(
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

			if(customer != null) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				fieldMap.put("customerId", customerId);
				fieldMap.put("fieldName", "su_bought");
                
				
				List<TimeSeries> timeSeriesList = new TimeSeriesDAO().findAll(fieldMap, "timestamp", true);				
				Map<String, Long> prodMap = new LinkedHashMap<String, Long>(); 			
				List<Object> xAxis = new ArrayList<Object>();	
				SimpleDateFormat sdf11 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
				if(timeSeriesList.size() > 0){
					for(TimeSeries timeSeries :  timeSeriesList){
						if(timeSeries.getValue() != null){						
							long val =	Long.parseLong(df1Precision.format(Double.parseDouble(timeSeries.getValue().toString())));
							Date d = sdf11.parse(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(timeSeries.getTimestamp()));
							String dStr = sdf11.format(d);	  
							prodMap.put(dStr, val);
							xAxis.add(dStr);
						}
					}
					
					Series series = new Series();
					series.setName("Traffic");
					List<Object> data = new ArrayList<Object>();
					for(Map.Entry<String, Long> entry : prodMap.entrySet()) {
						Data dta = new Data();					
						Date d = sdf11.parse(entry.getKey());				
						dta.setX(d.getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("SU Bought: " + entry.getValue().longValue());
						data.add(dta);
					}
						
					
					series.setData(data);
					series.setColor("#1BBA61");
					series.setShowInLegend(false);
					List<Series> seriesList = new ArrayList<Series>();
					seriesList.add(series);
	
					Highchart highchart = new Highchart();	
					highchart.setChart(new Chart("spline", null));
					highchart.setTitle(new Title("SU Bought traffic series 90 days"));
	
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
					return newCache.getData();
		
				
			 }
				
			}
		}
		return cache.getData();
	}
	

	
	
	public Map<String, Object> cdetailspuutilizedtrafficseriesegnyte(
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

			if(customer != null) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				fieldMap.put("customerId", customerId);
				fieldMap.put("fieldName", "pu_utilized");
                
				
				List<TimeSeries> timeSeriesList = new TimeSeriesDAO().findAll(fieldMap, "timestamp", true);				
				Map<String, Long> prodMap = new LinkedHashMap<String, Long>(); 				
				List<Object> xAxis = new ArrayList<Object>();
				SimpleDateFormat sdf11 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

				if(timeSeriesList.size() > 0){
				for(TimeSeries timeSeries :  timeSeriesList){
						if(timeSeries.getValue() != null){						
							long val =	Long.parseLong(df1Precision.format(Double.parseDouble(timeSeries.getValue().toString())));
							Date d = sdf11.parse(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(timeSeries.getTimestamp()));
							String dStr = sdf11.format(d);	  
							prodMap.put(dStr, val);
							xAxis.add(dStr);
						}
					}
					
					Series series = new Series();
					series.setName("Traffic");
					List<Object> data = new ArrayList<Object>();
					for(Map.Entry<String, Long> entry : prodMap.entrySet()) {
						Data dta = new Data();					
						Date d = sdf11.parse(entry.getKey());				
						dta.setX(d.getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("PU Utilized: " + entry.getValue().longValue());
						data.add(dta);
					}
						
					
					series.setData(data);
					series.setColor("#348781");
					series.setShowInLegend(false);
					List<Series> seriesList = new ArrayList<Series>();
					seriesList.add(series);
	
					Highchart highchart = new Highchart();	
					highchart.setChart(new Chart("spline", null));
					highchart.setTitle(new Title("PU utilized traffic series 90 days"));
	
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
					return newCache.getData();
		
				
			 }
				
			}
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailssuutilizedtrafficseriesegnyte(
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

			if(customer != null) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				fieldMap.put("customerId", customerId);
				fieldMap.put("fieldName", "su_utilized");
                
				
				List<TimeSeries> timeSeriesList = new TimeSeriesDAO().findAll(fieldMap, "timestamp", true);				
				Map<String, Long> prodMap = new LinkedHashMap<String, Long>(); 				
				List<Object> xAxis = new ArrayList<Object>();
				SimpleDateFormat sdf11 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

				if(timeSeriesList.size() > 0){
					for(TimeSeries timeSeries :  timeSeriesList){
						if(timeSeries.getValue() != null){
							long val =	Long.parseLong(df1Precision.format(Double.parseDouble(timeSeries.getValue().toString())));						
							Date d = sdf11.parse(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(timeSeries.getTimestamp()));
							String dStr = sdf11.format(d);	  
							prodMap.put(dStr, val);
							xAxis.add(dStr);
						}
					}
										
					Series series = new Series();
					series.setName("Traffic");
					List<Object> data = new ArrayList<Object>();
					for(Map.Entry<String, Long> entry : prodMap.entrySet()) {
						Data dta = new Data();					
						Date d = sdf11.parse(entry.getKey());				
						dta.setX(d.getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("SU Utilized: " + entry.getValue().longValue());
						data.add(dta);
					}
						
					
					series.setData(data);
					series.setColor("#03AACC");
					series.setShowInLegend(false);
					List<Series> seriesList = new ArrayList<Series>();
					seriesList.add(series);
	
					Highchart highchart = new Highchart();
	
					highchart.setChart(new Chart("spline", null));
					highchart.setTitle(new Title("SU utilized traffic series 90 days"));
	
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
					return newCache.getData();
		
				
			 }
				
			}
		}
		return cache.getData();
	}
	
	
	/*public Map<String, Object> cdetailscurrentmrrtrafficseriesegnyte(
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

			if(customer != null) {
				long startTime = new Date().getTime();
			
				SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");
				CustomerTimeseriesByDayDAO customerTimeseriesByDayDAO = new CustomerTimeseriesByDayDAO();
			
				Calendar calendar = Calendar.getInstance();		
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				
				Date rangeEnd = calendar.getTime();
				calendar.add(Calendar.DATE, -((!StringHelper.isEmpty(rangeFilter) && !rangeFilter.equalsIgnoreCase("date_range")) ? Integer.parseInt(rangeFilter) : 90));
				Date rangeStart = calendar.getTime();
				long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
				
				List<CustomerTimeseriesByDay> list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "currentMrr", rangeStart, rangeEnd);
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();
				for(CustomerTimeseriesByDay ctd : list) {
					if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
				
				List<Series> seriesList = new ArrayList<Series>();
				Series series = new Series();
				
				series.setName("MRR");
				List<Object> data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					Data dta = new Data();				
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("MRR: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#808000");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("MRR"));
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
	}*/
	
	public Map<String, Object> cdetailscurrentmrrtrafficseriesegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, String rangeStartDate, String rangeEndDate, String customerId, Integer cacheHours) throws Exception {
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
			params += "&rangeStartDate=" + rangeStartDate; 
			params += "&rangeEndDate=" + rangeEndDate; 
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

			if(customer != null) {
				long startTime = new Date().getTime();
			
				SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");
//				CustomerTimeseriesByDayDAO customerTimeseriesByDayDAO = new CustomerTimeseriesByDayDAO();
				CustomerTimeseriesByDayContentIndex indexer = new CustomerTimeseriesByDayContentIndex();
			
				Calendar calendar = Calendar.getInstance();		
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				
				Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
				Date rangeEnd = calendar.getTime();
				calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
				Date rangeStart = calendar.getTime();
				String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
				
//				logger.debug(rangeStartDate + "\t" + rangeEndDate);
				if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
					subTitle = rangeStartDate + " - " + rangeEndDate;
					calendar.setTime(dateFormatter.parse(rangeStartDate));
					rangeStart = calendar.getTime();
					calendar.setTime(dateFormatter.parse(rangeEndDate));
					rangeEnd = calendar.getTime();
				}
				
//				logger.debug(rangeStart + "\t" + rangeEnd);
				long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
				
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();
				
				/*List<CustomerTimeseriesByDay> list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "currentMrr", rangeStart, rangeEnd);
				for(CustomerTimeseriesByDay ctd : list) {
					if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}*/
				
				
				SearchResult sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "currentMrr", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
							pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
							pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				
				List<Series> seriesList = new ArrayList<Series>();
				Series series = new Series();
				series.setName("MRR");
				List<Object> data = new ArrayList<Object>();
				
				Series series2 = new Series();
				series2.setName("Initial MRR");
				List<Object> data2 = new ArrayList<Object>();
				
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					Data dta = new Data();				
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("MRR: " + entry.getValue().longValue());
					data.add(dta);
					
					Data dta2 = new Data();				
					dta2.setX(entry.getKey().getTime());					
					dta2.setY((customer.getInitialMrr() != null) ? customer.getInitialMrr().longValue() : 0);
					dta2.setInfo("Initial MRR: " + entry.getValue().longValue());
					data2.add(dta2);
				}	
				series.setData(data);
				series.setColor("#808000");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				series2.setData(data2);
				series2.setColor("#FFB90F");
				series2.setShowInLegend(true);
				seriesList.add(series2);
				
				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("MRR"));
				if(!StringHelper.isEmpty(rangeFilter)) {
					highchart.setSubtitle(new Subtitle(subTitle));
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
	
	
	public Map<String, Object> c360suboughttrafficseriesegnyte(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap,  Integer cacheHours) throws Exception {

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
				

				DecimalFormat df1Precision = new DecimalFormat("#");			
				SimpleDateFormat sdf = new SimpleDateFormat("MMM dd, yyyy");
				List<Long> values = new ArrayList<Long>();
			
				Calendar cal1 = Calendar.getInstance();		
				cal1.set(Calendar.HOUR_OF_DAY, 0);
				cal1.set(Calendar.AM_PM, 0);
				cal1.set(Calendar.HOUR, 0);
				cal1.set(Calendar.MINUTE, 0);
				cal1.set(Calendar.SECOND, 0);
				cal1.set(Calendar.MILLISECOND, 0);
				cal1.add(Calendar.DATE, -90);
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();
				for(int p = 0; p < 90; p++){				
					values.add(0l);
					cal1.add(Calendar.DATE, 1);
					pbMap.put(cal1.getTime(), 0l);								
				}
				
				int skip = 0;
				int limit = 200;
				
				
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
						fieldMap.clear();
						fieldMap.put("accountId", account.getId());
						fieldMap.put("customerId", customer.getId());

						fieldMap.clear();
						fieldMap.put("accountId", account.getId());
						fieldMap.put("customerId", customer.getId());
						fieldMap.put("fieldName", "su_bought");
						List<TimeSeriesDate> timeSeriesList = new TimeSeriesDateDAO().findAll(fieldMap, "timestamp", true);

						if(timeSeriesList.size() > 0  && timeSeriesList.size() <= values.size()){
							for(int i = 0 ; i < timeSeriesList.size() ; i++){

								if(timeSeriesList.get(i) != null && timeSeriesList.get(i).getValue() != null){			
									long val =	Long.parseLong(df1Precision.format(Double.parseDouble(timeSeriesList.get(i).getValue().toString())));
									values.set(i, values.get(i) + val);
									if(pbMap.containsKey(timeSeriesList.get(i).getTimestampDate())){
										pbMap.put(timeSeriesList.get(i).getTimestampDate(), pbMap.get(timeSeriesList.get(i).getTimestampDate())+ values.get(i) + val);
									}
								}
							}
						}				
					}
												
					skip += limit;
				}
				
				List<Object> xAxis = new ArrayList<Object>();
				Map<String, Long> prodMap = new LinkedHashMap<String, Long>(); 
				
				for(Map.Entry<Date, Long> mp :pbMap.entrySet()){
					if(mp.getValue() != null){
					prodMap.put(sdf.format(mp.getKey()), mp.getValue());
					xAxis.add(sdf.format(mp.getKey()));
					}
				}
				
				Series series = new Series();
				series.setName("Traffic");
				List<Object> data = new ArrayList<Object>();
				for(Map.Entry<String, Long> entry : prodMap.entrySet()) {
					Data dta = new Data();					
					Date d = sdf.parse(entry.getKey());				
					dta.setX(d.getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Traffic: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#1BBA61");
				series.setShowInLegend(false);
				List<Series> seriesList = new ArrayList<Series>();
				seriesList.add(series);

				Highchart highchart = new Highchart();
				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("SUBought traffic series 90 days"));

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
				return newCache.getData();
			}
			return cache.getData();
		}
		
	
	public Map<String, Object> c360puutilizedtrafficseriesegnyte(
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
				DecimalFormat df1Precision = new DecimalFormat("#");
				SimpleDateFormat sdf = new SimpleDateFormat("MMM dd, yyyy");
				List<Long> values = new ArrayList<Long>();
			
				Calendar cal1 = Calendar.getInstance();		
				cal1.set(Calendar.HOUR_OF_DAY, 0);
				cal1.set(Calendar.AM_PM, 0);
				cal1.set(Calendar.HOUR, 0);
				cal1.set(Calendar.MINUTE, 0);
				cal1.set(Calendar.SECOND, 0);
				cal1.set(Calendar.MILLISECOND, 0);
				cal1.add(Calendar.DATE, -90);
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();
				for(int p = 0; p < 90; p++){				
					values.add(0l);
					cal1.add(Calendar.DATE, 1);
					pbMap.put(cal1.getTime(), 0l);								
				}
				
				int skip = 0;
				int limit = 200;
				
				
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
						fieldMap.clear();
						fieldMap.put("accountId", account.getId());
						fieldMap.put("customerId", customer.getId());

						fieldMap.clear();
						fieldMap.put("accountId", account.getId());
						fieldMap.put("customerId", customer.getId());
						fieldMap.put("fieldName", "pu_utilized");
						List<TimeSeriesDate> timeSeriesList = new TimeSeriesDateDAO().findAll(fieldMap, "timestamp", true);

						if(timeSeriesList.size() > 0  && timeSeriesList.size() <= values.size()){
							for(int i = 0 ; i < timeSeriesList.size() ; i++){
								if(timeSeriesList.get(i) != null && timeSeriesList.get(i).getValue() != null){			
									long val =	Long.parseLong(df1Precision.format(Double.parseDouble(timeSeriesList.get(i).getValue().toString())));
									values.set(i, values.get(i) + val);
									if(pbMap.containsKey(timeSeriesList.get(i).getTimestampDate())){
										pbMap.put(timeSeriesList.get(i).getTimestampDate(), pbMap.get(timeSeriesList.get(i).getTimestampDate())+ values.get(i) + val);
									}
								}
							}
						}				
					}
					skip += limit;
				}
				List<Object> xAxis = new ArrayList<Object>();
				Map<String, Long> prodMap = new LinkedHashMap<String, Long>(); 
				
				for(Map.Entry<Date, Long> mp :pbMap.entrySet()){
					if(mp.getValue() != null){
					prodMap.put(sdf.format(mp.getKey()), mp.getValue());
					xAxis.add(sdf.format(mp.getKey()));
					}
				}
				
				
				Series series = new Series();
				series.setName("Traffic");
				List<Object> data = new ArrayList<Object>();
				for(Map.Entry<String, Long> entry : prodMap.entrySet()) {
					Data dta = new Data();					
					Date d = sdf.parse(entry.getKey());				
					dta.setX(d.getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Traffic: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#348781");
				series.setShowInLegend(false);
				List<Series> seriesList = new ArrayList<Series>();
				seriesList.add(series);

				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("pu utilized traffic series 90 days"));

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
				return newCache.getData();
			}
			return cache.getData();
		}
	
	
	public Map<String, Object> c360suutilizedtrafficseriesegnyte(
			Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object>  paramMap, Integer cacheHours) throws Exception {

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
				

				DecimalFormat df1Precision = new DecimalFormat("#");
				SimpleDateFormat sdf = new SimpleDateFormat("MMM dd, yyyy");
				List<Long> values = new ArrayList<Long>();
			
				Calendar cal1 = Calendar.getInstance();		
				cal1.set(Calendar.HOUR_OF_DAY, 0);
				cal1.set(Calendar.AM_PM, 0);
				cal1.set(Calendar.HOUR, 0);
				cal1.set(Calendar.MINUTE, 0);
				cal1.set(Calendar.SECOND, 0);
				cal1.set(Calendar.MILLISECOND, 0);
				cal1.add(Calendar.DATE, -90);
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();
				for(int p = 0; p < 90; p++){				
					values.add(0l);
					cal1.add(Calendar.DATE, 1);
					pbMap.put(cal1.getTime(), 0l);								
				}
				
				int skip = 0;
				int limit = 200;
				
				
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
						fieldMap.clear();
						fieldMap.put("accountId", account.getId());
						fieldMap.put("customerId", customer.getId());

						fieldMap.clear();
						fieldMap.put("accountId", account.getId());
						fieldMap.put("customerId", customer.getId());
						fieldMap.put("fieldName", "su_utilized");
						List<TimeSeriesDate> timeSeriesList = new TimeSeriesDateDAO().findAll(fieldMap, "timestamp", true);

						if(timeSeriesList.size() > 0  && timeSeriesList.size() <= values.size()){
							for(int i = 0 ; i < timeSeriesList.size() ; i++){
								if(timeSeriesList.get(i) != null && timeSeriesList.get(i).getValue() != null){			
									long val =	Long.parseLong(df1Precision.format(Double.parseDouble(timeSeriesList.get(i).getValue().toString())));
									values.set(i, values.get(i) + val);
									if(pbMap.containsKey(timeSeriesList.get(i).getTimestampDate())){
										pbMap.put(timeSeriesList.get(i).getTimestampDate(), pbMap.get(timeSeriesList.get(i).getTimestampDate())+ values.get(i) + val);
									}
								}
							}			
						}				
					}
												
					skip += limit;
				}
							
				List<Object> xAxis = new ArrayList<Object>();
				Map<String, Long> prodMap = new LinkedHashMap<String, Long>(); 
				
				for(Map.Entry<Date, Long> mp :pbMap.entrySet()){
					if(mp.getValue() != null){
					prodMap.put(sdf.format(mp.getKey()), mp.getValue());
					xAxis.add(sdf.format(mp.getKey()));
					}
				}
				
				
				Series series = new Series();
				series.setName("Traffic");
				List<Object> data = new ArrayList<Object>();
				for(Map.Entry<String, Long> entry : prodMap.entrySet()) {
					Data dta = new Data();					
					Date d = sdf.parse(entry.getKey());				
					dta.setX(d.getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Traffic: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#03AACC");
				series.setShowInLegend(false);
				List<Series> seriesList = new ArrayList<Series>();
				seriesList.add(series);

				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("su utilized traffic series 90 days"));

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
				return newCache.getData();
			}
			return cache.getData();
		}
	
	public Map<String, Object> c360currentmrrtrafficseriesegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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
		
			SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");
			TimeseriesByDayDAO timeseriesByDayDAO = new TimeseriesByDayDAO();
			CustomerDAO customerDAO = new CustomerDAO();
			DecimalFormat df1Precision = new DecimalFormat("#");
	
			Calendar calendar = Calendar.getInstance();		
			calendar.set(Calendar.HOUR_OF_DAY, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MILLISECOND, 0);
			
			Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
			Date rangeEnd = calendar.getTime();
			calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
			Date rangeStart = calendar.getTime();
			String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
			
//			logger.debug(paramMap.get("rangeStartDate") + "\t" + paramMap.get("rangeEndDate"));
			
			Double initialMrr = 0d;
			
			int skip = 0;
			int limit = 200;
			while(true) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				for(Map.Entry<String, Object> mp : paramMap.entrySet()){
					if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
						fieldMap.put(mp.getKey(), mp.getValue()); 
					}
				}
				List<Customer> customers = customerDAO.findAll(fieldMap, skip, limit);
				if(customers.size() == 0) {
					break;
				}
				for(Customer customer : customers) {
					if(customer.getInitialMrr() != null) {
						initialMrr += customer.getInitialMrr();
					}
				}
				skip += limit;
			}
			
			if(paramMap.get("rangeStartDate") != null && paramMap.get("rangeEndDate") != null) {
				subTitle = paramMap.get("rangeStartDate").toString() + " - " + paramMap.get("rangeEndDate").toString();
				calendar.setTime(dateFormatter.parse(paramMap.get("rangeStartDate").toString()));
				rangeStart = calendar.getTime();
				calendar.setTime(dateFormatter.parse(paramMap.get("rangeEndDate").toString()));
				rangeEnd = calendar.getTime();
			}
//			logger.debug(rangeStart + "\t" + rangeEnd);
			long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
			
			List<TimeseriesByDay> list = timeseriesByDayDAO.getData(account.getId(), "currentMrr", rangeStart, rangeEnd);
			
			Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();
			for(TimeseriesByDay ctd : list) {
				if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
					pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
				} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
					pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
				}
			}
			
			List<Series> seriesList = new ArrayList<Series>();
			Series series = new Series();
			series.setName("MRR");
			List<Object> data = new ArrayList<Object>();
			
			Series series2 = new Series();
			series2.setName("Initial MRR");
			List<Object> data2 = new ArrayList<Object>();
			
			for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
				Data dta = new Data();				
				dta.setX(entry.getKey().getTime());					
				dta.setY(entry.getValue().longValue());
				dta.setInfo("MRR: " + entry.getValue().longValue());
				data.add(dta);
				
				Data dta2 = new Data();				
				dta2.setX(entry.getKey().getTime());					
				dta2.setY(initialMrr.longValue());
				dta2.setInfo("Initial MRR: " + initialMrr.longValue());
				data2.add(dta2);
			}
			series.setData(data);
			series.setColor("#808000");
			series.setShowInLegend(true);
			seriesList.add(series);
			
			series2.setData(data2);
			series2.setColor("#FFB90F");
			series2.setShowInLegend(true);
			seriesList.add(series2);
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("spline", null));
			highchart.setTitle(new Title("MRR"));
			if(!StringHelper.isEmpty(rangeFilter)) {
				highchart.setSubtitle(new Subtitle(subTitle));
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
	
	//PU Bought Utilized 90days
	public Map<String, Object> cdetailsboughttrafficseriesegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, String rangeStartDate, String rangeEndDate, String customerId, Integer cacheHours) throws Exception {
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
			params += "&rangeStartDate=" + rangeStartDate; 
			params += "&rangeEndDate=" + rangeEndDate; 
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
			SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");

			if(customer != null) {
				
				long startTime = new Date().getTime();
				
//				CustomerTimeseriesByDayDAO customerTimeseriesByDayDAO = new CustomerTimeseriesByDayDAO();
				CustomerTimeseriesByDayContentIndex indexer = new CustomerTimeseriesByDayContentIndex();
			
				Calendar calendar = Calendar.getInstance();		
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				
				Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
				Date rangeEnd = calendar.getTime();
				calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
				Date rangeStart = calendar.getTime();
				String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
				
				logger.debug(rangeStartDate + "\t" + rangeEndDate);
				if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
					subTitle = rangeStartDate + " - " + rangeEndDate;
					calendar.setTime(dateFormatter.parse(rangeStartDate));
					rangeStart = calendar.getTime();
					calendar.setTime(dateFormatter.parse(rangeEndDate));
					rangeEnd = calendar.getTime();
				}
				
				logger.debug(rangeStart + "\t" + rangeEnd);
				long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
				
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();				
				Map<Date, Long> sbMap = new LinkedHashMap<Date, Long>();
				Map<Date, Long> activeUserMap = new LinkedHashMap<Date, Long>();
				
				/*List<CustomerTimeseriesByDay> list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "pu_bought", rangeStart, rangeEnd);
				for(CustomerTimeseriesByDay ctd : list) {
					if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
			
				list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "pu_utilized", rangeStart, rangeEnd);
				for(CustomerTimeseriesByDay ctd : list) {
					if(!sbMap.containsKey(ctd.getDate()) && sbMap.size() < days && ctd.getFieldValue() != null) {
						sbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(sbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						sbMap.put(ctd.getDate(), sbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
				
				list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "active_users", rangeStart, rangeEnd);
				for(CustomerTimeseriesByDay ctd : list) {
					if(!activeUserMap.containsKey(ctd.getDate()) && activeUserMap.size() < days && ctd.getFieldValue() != null) {
						activeUserMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(activeUserMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						activeUserMap.put(ctd.getDate(), activeUserMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}*/
				
				Calendar tempDate = Calendar.getInstance();
				
				SearchResult sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "pu_bought", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						tempDate.setTime(ctd.getDate());
						tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
						
						if(!pbMap.containsKey(tempDate.getTime()) && pbMap.size() < days && ctd.getFieldValue() != null) {
							pbMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(pbMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
							pbMap.put(tempDate.getTime(), pbMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "pu_utilized", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						tempDate.setTime(ctd.getDate());
						tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
						
						if(!sbMap.containsKey(tempDate.getTime()) && sbMap.size() < days && ctd.getFieldValue() != null) {
							sbMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(sbMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
							sbMap.put(tempDate.getTime(), sbMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "active_users", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						tempDate.setTime(ctd.getDate());
						tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
						
						if(!activeUserMap.containsKey(tempDate.getTime()) && activeUserMap.size() < days && ctd.getFieldValue() != null) {
							activeUserMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(activeUserMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
							activeUserMap.put(tempDate.getTime(), activeUserMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				
				long pu_bought = 0;
				long pu_utilized = 0;
				long active_users = 0;
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					if(entry.getValue() >= 99999 && sbMap.get(entry.getKey()) != null && activeUserMap.get(entry.getKey()) != null) {
						pu_bought = entry.getValue();
						pu_utilized = sbMap.get(entry.getKey());
//						active_users = activeUserMap.get(entry.getKey());
						
						pu_bought = pu_utilized + ((pu_utilized * 40) / 100);
//						active_users += (active_users * 40) / 100;
						
//						sbMap.put(entry.getKey(), pu_utilized);
//						activeUserMap.put(entry.getKey(), active_users);
						
						pbMap.put(entry.getKey(), pu_bought);
					}
				}
				
				List<Series> seriesList = new ArrayList<Series>();
				Series series = new Series();
				
				series.setName("PU Bought");
				List<Object> data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					Data dta = new Data();				
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("PU Bought: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#FFB90F");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				series = new Series();
				
				series.setName("PU Utilized");
				data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : sbMap.entrySet()) {
					Data dta = new Data();								
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("PU Utilized: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#1BBA61");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				series = new Series();
				series.setName("Active Users");
				data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : activeUserMap.entrySet()) {
					Data dta = new Data();								
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Active Users: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#fe5400");
				series.setShowInLegend(true);
				seriesList.add(series);
				

				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("PU Bought/Utilized"));
				if(!StringHelper.isEmpty(rangeFilter)) {
					highchart.setSubtitle(new Subtitle(subTitle));
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
//				logger.debug("by cdetails by PU bought [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
				
			}
		}
		return cache.getData();
	}

	//SU bought Utilized 90days
	public Map<String, Object> cdetailsutilizedtrafficseriesegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, String rangeStartDate, String rangeEndDate, String customerId, Integer cacheHours) throws Exception {
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
			params += "&rangeStartDate=" + rangeStartDate; 
			params += "&rangeEndDate=" + rangeEndDate; 
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

			if(customer != null) {
				long startTime = new Date().getTime();
				
//				CustomerTimeseriesByDayDAO customerTimeseriesByDayDAO = new CustomerTimeseriesByDayDAO();
				CustomerTimeseriesByDayContentIndex indexer = new CustomerTimeseriesByDayContentIndex();
				SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");
			
				Calendar calendar = Calendar.getInstance();		
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				
				Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
				Date rangeEnd = calendar.getTime();
				calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
				Date rangeStart = calendar.getTime();
				String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
				
				logger.debug(rangeStartDate + "\t" + rangeEndDate);
				if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
					subTitle = rangeStartDate + " - " + rangeEndDate;
					calendar.setTime(dateFormatter.parse(rangeStartDate));
					rangeStart = calendar.getTime();
					calendar.setTime(dateFormatter.parse(rangeEndDate));
					rangeEnd = calendar.getTime();
				}
				
				logger.debug(rangeStart + "\t" + rangeEnd);
				long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
				
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();				
				Map<Date, Long> sbMap = new LinkedHashMap<Date, Long>();
				Map<Date, Long> activeUserMap = new LinkedHashMap<Date, Long>();
				
				/*List<CustomerTimeseriesByDay> list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "su_bought", rangeStart, rangeEnd);
				for(CustomerTimeseriesByDay ctd : list) {
					if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
			
				list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "su_utilized", rangeStart, rangeEnd);
				for(CustomerTimeseriesByDay ctd : list) {
					if(!sbMap.containsKey(ctd.getDate()) && sbMap.size() < days && ctd.getFieldValue() != null) {
						sbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(sbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						sbMap.put(ctd.getDate(), sbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
				
				list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "active_users", rangeStart, rangeEnd);
				for(CustomerTimeseriesByDay ctd : list) {
					if(!activeUserMap.containsKey(ctd.getDate()) && activeUserMap.size() < days && ctd.getFieldValue() != null) {
						activeUserMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(activeUserMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						activeUserMap.put(ctd.getDate(), activeUserMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}*/
				Calendar tempDate = Calendar.getInstance();
				
				SearchResult sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "su_bought", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						tempDate.setTime(ctd.getDate());
						tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
						
						if(!pbMap.containsKey(tempDate.getTime()) && pbMap.size() < days && ctd.getFieldValue() != null) {
							pbMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(pbMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
							pbMap.put(tempDate.getTime(), pbMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "su_utilized", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						tempDate.setTime(ctd.getDate());
						tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
						
						if(!sbMap.containsKey(tempDate.getTime()) && sbMap.size() < days && ctd.getFieldValue() != null) {
							sbMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(sbMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
							sbMap.put(tempDate.getTime(), sbMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "active_users", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						tempDate.setTime(ctd.getDate());
						tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
						
						if(!activeUserMap.containsKey(tempDate.getTime()) && activeUserMap.size() < days && ctd.getFieldValue() != null) {
							activeUserMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(activeUserMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
							activeUserMap.put(tempDate.getTime(), activeUserMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				
				long su_bought = 0;
				long su_utilized = 0;
				long active_users = 0;
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					if(entry.getValue() >= 99999 && sbMap.get(entry.getKey()) != null && activeUserMap.get(entry.getKey()) != null) {
						su_bought = entry.getValue();
						su_utilized = sbMap.get(entry.getKey());
//						active_users = activeUserMap.get(entry.getKey());
						
						su_bought = su_utilized + ((su_utilized * 40) / 100);
//						active_users += (active_users * 40) / 100;
						
//						sbMap.put(entry.getKey(), su_utilized);
//						activeUserMap.put(entry.getKey(), active_users);
						pbMap.put(entry.getKey(), su_bought);
					}
				}
				
				List<Series> seriesList = new ArrayList<Series>();
				Series series = new Series();

				series.setName("SU Bought");
				List<Object> data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					Data dta = new Data();				
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("SU Bought: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#4E387E");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				series = new Series();
				
				series.setName("SU Utilized");
				data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : sbMap.entrySet()) {
					Data dta = new Data();								
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("SU Utilized: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#03AACC");
				series.setShowInLegend(true);
				seriesList.add(series);

				series = new Series();
				series.setName("Active Users");
				data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : activeUserMap.entrySet()) {
					Data dta = new Data();								
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Active Users: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#fe5400");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("SU Bought/Utilized"));
				if(!StringHelper.isEmpty(rangeFilter)) {
					highchart.setSubtitle(new Subtitle(subTitle));
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
//				logger.debug("by cdetails SU bought Utilized [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
				
			}
		}
		return cache.getData();
	}
	
	//PU Bought Utilized
	public Map<String, Object> c360boughttrafficseriesegnyte(
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
			
				SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy"); 
				TimeseriesByDayDAO timeseriesByDayDAO = new TimeseriesByDayDAO();
				DecimalFormat df1Precision = new DecimalFormat("#");
		
				Calendar calendar = Calendar.getInstance();		
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				
				Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
				Date rangeEnd = calendar.getTime();
				calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
				Date rangeStart = calendar.getTime();
				String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
				long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
				
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();				
				Map<Date, Long> sbMap = new LinkedHashMap<Date, Long>();
				Map<Date, Long> activeUserMap = new LinkedHashMap<Date, Long>();
				
				if(paramMap.get("rangeStartDate") != null && paramMap.get("rangeEndDate") != null) {
					subTitle = paramMap.get("rangeStartDate").toString() + " - " + paramMap.get("rangeEndDate").toString();
					calendar.setTime(dateFormatter.parse(paramMap.get("rangeStartDate").toString()));
					rangeStart = calendar.getTime();
					calendar.setTime(dateFormatter.parse(paramMap.get("rangeEndDate").toString()));
					rangeEnd = calendar.getTime();
				}
				
				Calendar tempDate = Calendar.getInstance();
				
				List<TimeseriesByDay> list = timeseriesByDayDAO.getData(account.getId(), "pu_bought", rangeStart, rangeEnd);
				for(TimeseriesByDay ctd : list) {
					tempDate.setTime(ctd.getDate());
					tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
					
					
					if(!pbMap.containsKey(tempDate.getTime()) && pbMap.size() < days && ctd.getFieldValue() != null) {
						pbMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(pbMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
						pbMap.put(tempDate.getTime(), pbMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
			
				list = timeseriesByDayDAO.getData(account.getId(), "pu_utilized", rangeStart, rangeEnd);
				
				for(TimeseriesByDay ctd : list) {
					tempDate.setTime(ctd.getDate());
					tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
					
					if(!sbMap.containsKey(tempDate.getTime()) && sbMap.size() < days && ctd.getFieldValue() != null) {
						sbMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(sbMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
						sbMap.put(tempDate.getTime(), sbMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
				
				list = timeseriesByDayDAO.getData(account.getId(), "active_users", rangeStart, rangeEnd);
				for(TimeseriesByDay ctd : list) {
					tempDate.setTime(ctd.getDate());
					tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
					
					if(!activeUserMap.containsKey(tempDate.getTime()) && activeUserMap.size() < days && ctd.getFieldValue() != null) {
						activeUserMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(activeUserMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
						activeUserMap.put(tempDate.getTime(), activeUserMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
				
				/*long value = 0;
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					if(entry.getValue() >= 10000) {
						value = 0;
						
					}
				}*/
				
				List<Series> seriesList = new ArrayList<Series>();
				Series series = new Series();
				
				series.setName("PU Bought");
				List<Object> data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					Data dta = new Data();				
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("PU Bought: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#FFB90F");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				series = new Series();
				
				series.setName("PU Utilized");
				data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : sbMap.entrySet()) {
					Data dta = new Data();								
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("PU Utilized: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#1BBA61");
				series.setShowInLegend(true);
				seriesList.add(series);

				
				series = new Series();
				
				series.setName("Active Users");
				data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : activeUserMap.entrySet()) {
					Data dta = new Data();								
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Active Users: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#fe5400");
				series.setShowInLegend(true);
				seriesList.add(series);

				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("PU Bought/Utilized"));
				if(!StringHelper.isEmpty(rangeFilter)) {
					highchart.setSubtitle(new Subtitle(subTitle));
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
//				logger.debug("by c360customerby PU bought [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
			}
			return cache.getData();
		}
	
	   //SU Bought vs SU Utilized
		public Map<String, Object> c360utilizedtrafficseriesegnyte(
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
				
					SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy"); 
					TimeseriesByDayDAO timeseriesByDayDAO = new TimeseriesByDayDAO();
					DecimalFormat df1Precision = new DecimalFormat("#");
			
					Calendar calendar = Calendar.getInstance();		
					calendar.set(Calendar.HOUR_OF_DAY, 0);
					calendar.set(Calendar.MINUTE, 0);
					calendar.set(Calendar.SECOND, 0);
					calendar.set(Calendar.MILLISECOND, 0);
					
					Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
					Date rangeEnd = calendar.getTime();
					calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
					Date rangeStart = calendar.getTime();
					String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";			
					long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
					
					Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();				
					Map<Date, Long> sbMap = new LinkedHashMap<Date, Long>();
					Map<Date, Long> activeUserMap = new LinkedHashMap<Date, Long>();
					
					if(paramMap.get("rangeStartDate") != null && paramMap.get("rangeEndDate") != null) {
						subTitle = paramMap.get("rangeStartDate").toString() + " - " + paramMap.get("rangeEndDate").toString();
						calendar.setTime(dateFormatter.parse(paramMap.get("rangeStartDate").toString()));
						rangeStart = calendar.getTime();
						calendar.setTime(dateFormatter.parse(paramMap.get("rangeEndDate").toString()));
						rangeEnd = calendar.getTime();
					}
					
					List<TimeseriesByDay> list = timeseriesByDayDAO.getData(account.getId(), "su_bought", rangeStart, rangeEnd);				
					for(TimeseriesByDay ctd : list) {
						if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
							pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
							pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				
					list = timeseriesByDayDAO.getData(account.getId(), "su_utilized", rangeStart, rangeEnd);
					
					for(TimeseriesByDay ctd : list) {
						if(!sbMap.containsKey(ctd.getDate()) && sbMap.size() < days && ctd.getFieldValue() != null) {
							sbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(sbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
							sbMap.put(ctd.getDate(), sbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
					
					list = timeseriesByDayDAO.getData(account.getId(), "active_users", rangeStart, rangeEnd);
					for(TimeseriesByDay ctd : list) {
						if(!activeUserMap.containsKey(ctd.getDate()) && activeUserMap.size() < days && ctd.getFieldValue() != null) {
							activeUserMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(activeUserMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
							activeUserMap.put(ctd.getDate(), activeUserMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
					
					List<Series> seriesList = new ArrayList<Series>();
					Series series = new Series();

					series.setName("SU Bought");
					List<Object> data = new ArrayList<Object>();
					for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
						Data dta = new Data();				
						dta.setX(entry.getKey().getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("SU Bought: " + entry.getValue().longValue());
						data.add(dta);
					}
						
					series.setData(data);
					series.setColor("#4E387E");
					series.setShowInLegend(true);
					seriesList.add(series);
					
					series = new Series();
					
					series.setName("SU Utilized");
					data = new ArrayList<Object>();
					for(Map.Entry<Date, Long> entry : sbMap.entrySet()) {
						Data dta = new Data();								
						dta.setX(entry.getKey().getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("SU Utilized: " + entry.getValue().longValue());
						data.add(dta);
					}
						
					series.setData(data);
					series.setColor("#03AACC");
					series.setShowInLegend(true);
					seriesList.add(series);

					series = new Series();
					series.setName("Active Users");
					data = new ArrayList<Object>();
					for(Map.Entry<Date, Long> entry : activeUserMap.entrySet()) {
						Data dta = new Data();								
						dta.setX(entry.getKey().getTime());					
						dta.setY(entry.getValue().longValue());
						dta.setInfo("Active Users: " + entry.getValue().longValue());
						data.add(dta);
					}
						
					series.setData(data);
					series.setColor("#fe5400");
					series.setShowInLegend(true);
					seriesList.add(series);
					
					Highchart highchart = new Highchart();

					highchart.setChart(new Chart("spline", null));
					highchart.setTitle(new Title("SU Bought/Utilized"));
					if(!StringHelper.isEmpty(rangeFilter)) {
						highchart.setSubtitle(new Subtitle(subTitle));
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
//					logger.debug("by c360customerby SU bought Utilized [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

					return newCache.getData();
				}
				return cache.getData();
			}
	
	//Storage Bought Utilized 90days
	public Map<String, Object> cdetailsstoragetrafficseriesegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, String rangeStartDate, String rangeEndDate, String customerId, Integer cacheHours) throws Exception {
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
			params += "&rangeStartDate=" + rangeStartDate; 
			params += "&rangeEndDate=" + rangeEndDate; 
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

			if(customer != null) {
				long startTime = new Date().getTime();
			
				SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");
//				CustomerTimeseriesByDayDAO customerTimeseriesByDayDAO = new CustomerTimeseriesByDayDAO();
				CustomerTimeseriesByDayContentIndex indexer = new CustomerTimeseriesByDayContentIndex();
		
				Calendar calendar = Calendar.getInstance();		
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				
				Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
				Date rangeEnd = calendar.getTime();
				calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
				Date rangeStart = calendar.getTime();
				String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
				
				logger.debug(rangeStartDate + "\t" + rangeEndDate);
				if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
					subTitle = rangeStartDate + " - " + rangeEndDate;
					calendar.setTime(dateFormatter.parse(rangeStartDate));
					rangeStart = calendar.getTime();
					calendar.setTime(dateFormatter.parse(rangeEndDate));
					rangeEnd = calendar.getTime();
				}
				
				logger.debug(rangeStart + "\t" + rangeEnd);
				long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
				
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();				
				Map<Date, Long> sbMap = new LinkedHashMap<Date, Long>();
				Map<Date, Long> activeUserMap = new LinkedHashMap<Date, Long>();
				
				/*List<CustomerTimeseriesByDay> list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "storage_bought_mb_", rangeStart, rangeEnd);
				for(CustomerTimeseriesByDay ctd : list) {
					if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
			
				list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "storage_used_mb_", rangeStart, rangeEnd);
				for(CustomerTimeseriesByDay ctd : list) {
					if(!sbMap.containsKey(ctd.getDate()) && sbMap.size() < days && ctd.getFieldValue() != null) {
						sbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(sbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						sbMap.put(ctd.getDate(), sbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
				
				list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "active_users", rangeStart, rangeEnd);
				for(CustomerTimeseriesByDay ctd : list) {
					if(!activeUserMap.containsKey(ctd.getDate()) && activeUserMap.size() < days && ctd.getFieldValue() != null) {
						activeUserMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(activeUserMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						activeUserMap.put(ctd.getDate(), activeUserMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}*/
				
				Calendar tempDate = Calendar.getInstance();
				
				SearchResult sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "storage_bought_mb_", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						tempDate.setTime(ctd.getDate());
						tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
						
						if(!pbMap.containsKey(tempDate.getTime()) && pbMap.size() < days && ctd.getFieldValue() != null) {
							pbMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(pbMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
							pbMap.put(tempDate.getTime(), pbMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "storage_used_mb_", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						tempDate.setTime(ctd.getDate());
						tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
						
						if(!sbMap.containsKey(tempDate.getTime()) && sbMap.size() < days && ctd.getFieldValue() != null) {
							sbMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(sbMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
							sbMap.put(tempDate.getTime(), sbMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "active_users", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						tempDate.setTime(ctd.getDate());
						tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
						
						if(!activeUserMap.containsKey(tempDate.getTime()) && activeUserMap.size() < days && ctd.getFieldValue() != null) {
							activeUserMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(activeUserMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
							activeUserMap.put(tempDate.getTime(), activeUserMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				
				long storage_bought = 0;
				long storage_utilized = 0;
				long active_users = 0;
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					if(entry.getValue() >= 99999 && sbMap.get(entry.getKey()) != null && activeUserMap.get(entry.getKey()) != null) {
						storage_bought = entry.getValue();
						storage_utilized = sbMap.get(entry.getKey());
//						active_users = activeUserMap.get(entry.getKey());
						
						storage_bought = storage_utilized + ((storage_utilized * 40) / 100);
//						active_users += (active_users * 40) / 100;
						
//						sbMap.put(entry.getKey(), storage_utilized);
//						activeUserMap.put(entry.getKey(), active_users);
						pbMap.put(entry.getKey(), storage_bought);
					}
				}
				
				List<Series> seriesList = new ArrayList<Series>();
				Series series = new Series();
				
				series.setName("Storage Bought");
				List<Object> data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					Data dta = new Data();				
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Storage Bought: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#8ABB20");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				series = new Series();
				series.setName("Storage Utilized");
				data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : sbMap.entrySet()) {
					Data dta = new Data();								
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Storage Utilized: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#494348");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				series = new Series();
				series.setName("Active Users");
				data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : activeUserMap.entrySet()) {
					Data dta = new Data();								
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Active Users: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#fe5400");
				series.setShowInLegend(true);
				seriesList.add(series);

				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("Storage Bought/Utilized"));
				if(!StringHelper.isEmpty(rangeFilter)) {
					highchart.setSubtitle(new Subtitle(subTitle));
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
//				logger.debug("by c360customerby Storage bought Utilized [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
				
			}
		}
		return cache.getData();
	}
	
	//storage bought Utilized 90days
	public Map<String, Object> c360storagetrafficseriesegnyte(
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
			
				SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");
				TimeseriesByDayDAO timeseriesByDayDAO = new TimeseriesByDayDAO();
				DecimalFormat df1Precision = new DecimalFormat("#");
			
				Calendar calendar = Calendar.getInstance();		
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				
				Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
				Date rangeEnd = calendar.getTime();
				calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
				Date rangeStart = calendar.getTime();
				String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
				long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
				
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();				
				Map<Date, Long> sbMap = new LinkedHashMap<Date, Long>();
				Map<Date, Long> activeUserMap = new LinkedHashMap<Date, Long>();
				
				if(paramMap.get("rangeStartDate") != null && paramMap.get("rangeEndDate") != null) {
					subTitle = paramMap.get("rangeStartDate").toString() + " - " + paramMap.get("rangeEndDate").toString();
					calendar.setTime(dateFormatter.parse(paramMap.get("rangeStartDate").toString()));
					rangeStart = calendar.getTime();
					calendar.setTime(dateFormatter.parse(paramMap.get("rangeEndDate").toString()));
					rangeEnd = calendar.getTime();
				}
				
				List<TimeseriesByDay> list = timeseriesByDayDAO.getData(account.getId(), "storage_bought_mb_", rangeStart, rangeEnd);
				for(TimeseriesByDay ctd : list) {
					if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
			
				list = timeseriesByDayDAO.getData(account.getId(), "storage_used_mb_", rangeStart, rangeEnd);
				
				for(TimeseriesByDay ctd : list) {
					if(!sbMap.containsKey(ctd.getDate()) && sbMap.size() < days && ctd.getFieldValue() != null) {
						sbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(sbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						sbMap.put(ctd.getDate(), sbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
				
				list = timeseriesByDayDAO.getData(account.getId(), "active_users", rangeStart, rangeEnd);
				for(TimeseriesByDay ctd : list) {
					if(!activeUserMap.containsKey(ctd.getDate()) && activeUserMap.size() < days && ctd.getFieldValue() != null) {
						activeUserMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(activeUserMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						activeUserMap.put(ctd.getDate(), activeUserMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
				
				List<Series> seriesList = new ArrayList<Series>();
				Series series = new Series();
				
				series.setName("Storage Bought");
				List<Object> data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					Data dta = new Data();				
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Storage Bought: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#8ABB20");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				series = new Series();
				
				series.setName("Storage Utilized");
				data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : sbMap.entrySet()) {
					Data dta = new Data();								
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Storage Utilized: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#494348");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				series = new Series();
				series.setName("Active Users");
				data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : activeUserMap.entrySet()) {
					Data dta = new Data();								
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Active Users: " + entry.getValue().longValue());
					data.add(dta);
				}
					
				series.setData(data);
				series.setColor("#fe5400");
				series.setShowInLegend(true);
				seriesList.add(series);

				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("Storage Bought/Utilized"));
				if(!StringHelper.isEmpty(rangeFilter)) {
					highchart.setSubtitle(new Subtitle(subTitle));
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
//				logger.debug("by c360customerby Storage bought Utilized [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
			}
			return cache.getData();
		}
	
	public Map<String, Object> c360customersbypurateegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, int page, String sortBy, boolean ascending) throws Exception {

		InformationDAO informationDAO = new InformationDAO();
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		
		if(true) {
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("entity", "CUSTOMER");
			fieldMap.put("type", "UPSELL");
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				fieldMap.put("rangeFilter", (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter));
			}
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			
			if(StringHelper.isEmpty(sortBy)) {
				sortBy = "puUtilizedPercent";
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
			cachedData.put("listName", "c360customersbypurateegnyte");
			cachedData.put("apiName", "c360customersbypurateegnyte");
			cachedData.put("sortBy", sortBy);
			cachedData.put("ascending", ascending);
			cachedData.put("rangeFilter", rangeFilter);
			cachedData.put("podId", podId);
			cachedData.put("customerTypeId", fieldMap.get("customerTypes"));
			cachedData.put("title", "Customers by highest PU Utilization");
			cachedData.put("informations", informations);
			cachedData.put("pageCount", pageCount);
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					cachedData.put(mp.getKey(), mp.getValue());
				}
			}
					
			cachedData.put("C360_CUSTOMERS_BY_PU_RATE_EGNYTE", "yes");
		}
		
		return cachedData;
		
	}
	
	public Map<String, Object> c360customersbysurateegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, int page, String sortBy, boolean ascending) throws Exception {
 
	
		InformationDAO informationDAO = new InformationDAO();
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		if(true) {
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("entity", "CUSTOMER");
			fieldMap.put("type", "UPSELL");
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				fieldMap.put("rangeFilter", (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter));
			}
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
		
			
			if(StringHelper.isEmpty(sortBy)) {
				sortBy = "suUtilizedPercent";
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
			cachedData.put("listName", "c360customersbysurateegnyte");
			cachedData.put("apiName", "c360customersbysurateegnyte");
			cachedData.put("sortBy", sortBy);
			cachedData.put("ascending", ascending);
			cachedData.put("rangeFilter", rangeFilter);
			cachedData.put("podId", podId);
			cachedData.put("customerTypeId", fieldMap.get("customerTypes"));
			cachedData.put("title", "Customers by highest SU Utilization");
			cachedData.put("informations", informations);
			cachedData.put("pageCount", pageCount);
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					cachedData.put(mp.getKey(), mp.getValue());
				}
			}
			
			cachedData.put("C360_CUSTOMERS_BY_SU_RATE_EGNYTE", "yes");
		}
		
		return cachedData;
		
	}
	
	public Map<String, Object> c360customersbystoragerateegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, int page, String sortBy, boolean ascending) throws Exception {

		
		InformationDAO informationDAO = new InformationDAO();
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		
		if(true) {
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("entity", "CUSTOMER");
			fieldMap.put("type", "UPSELL");
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				fieldMap.put("rangeFilter", (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter));
			}
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			
			if(StringHelper.isEmpty(sortBy)) {
				sortBy = "storageUtilizedPercent";
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
			cachedData.put("listName", "c360customersbystoragerateegnyte");
			cachedData.put("apiName", "c360customersbystoragerateegnyte");
			cachedData.put("sortBy", sortBy);
			cachedData.put("ascending", ascending);
			cachedData.put("rangeFilter", rangeFilter);
			cachedData.put("podId", podId);
			cachedData.put("customerTypeId", fieldMap.get("customerTypes"));
			cachedData.put("title", "Customers by highest Storage Utilization");
			cachedData.put("informations", informations);
			cachedData.put("pageCount", pageCount);
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					cachedData.put(mp.getKey(), mp.getValue());
				}
			}
		
			cachedData.put("C360_CUSTOMERS_BY_STORAGE_RATE_EGNYTE", "yes");
		}
		
		return cachedData;
	}
	
	public Map<String, Object> c360docustomermetricscalculations(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

		InformationDAO informationDAO = new InformationDAO();
		Calendar calendar = Calendar.getInstance();
		
		Date to = calendar.getTime();
		calendar.add(Calendar.HOUR, cacheHours);
		Date from = calendar.getTime();
		
		boolean infoCached = false;
		Information cachedInfo = informationDAO.findCachedDataEnhanced(account.getId(), rangeFilter, from, to, "UPSELL", "CUSTOMER");
		if(cachedInfo != null) {
			infoCached = true;
		}
		
		if(!infoCached) {
			informationDAO.deleteCachedData(account.getId(), rangeFilter, null, "UPSELL", "CUSTOMER");
			
			Map<String, Object> fieldMap = new HashMap<String, Object>();
			CustomerDAO customerDAO = new CustomerDAO();
			DecimalFormat df = new DecimalFormat("#.###");
			
			Calendar range = Calendar.getInstance();		
			range.set(Calendar.HOUR_OF_DAY, 0);
			range.set(Calendar.MINUTE, 0);
			range.set(Calendar.SECOND, 0);
			range.set(Calendar.MILLISECOND, 0);
			
			Date rangeEnd = calendar.getTime();
			calendar.add(Calendar.DATE, -((!StringHelper.isEmpty(rangeFilter) && !rangeFilter.equalsIgnoreCase("date_range")) ? Integer.parseInt(rangeFilter) : 90));
			Date rangeStart = calendar.getTime();
			long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
			
			Double percent = 0d;

			int skip = 0;
			int limit = 200;
			while(true) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
				if(customers.size() == 0) {
					break;
				}
				for(Customer customer : customers) {
					try {
						
						Information info = new Information();
						info.setAccountId(account.getId());
						info.setType("UPSELL");
						info.setEntity("CUSTOMER");
						info.setEntityId(customer.getId());
						info.setName(customer.getName());
						info.setIgnored(customer.getIgnored());
						info.setCustomerTypeId(customer.getCustomerTypeId());
						info.setPartnerId(customer.getPartnerId());
						info.setAccountManager(customer.getAccountManager());
						info.setCsm(customer.getCsm());
						info.setCem(customer.getCem());
						info.setCurrentMrr((customer.getCurrentMrr() != null) ? Double.valueOf(df.format(customer.getCurrentMrr())) : 0d);
						info.setDate(to);
						info.setPuBought(0d);
						info.setPuUtilized(0d);
						info.setPuUtilizedPercent(0d);
						info.setActiveUsers(0d);
						
						info.setSuBought(0d);
						info.setSuUtilized(0d);
						info.setSuUtilizedPercent(0d);
						
						info.setStorageBought(0d);
						info.setStorageUtilized(0d);
						info.setStorageUtilizedPercent(0d);
						info.setActiveUsersUtilizedPercent(0d);
						
						info.setCustomerTypes((customer.getCustomerTypes() != null && customer.getCustomerTypes().size() > 0 ? customer.getCustomerTypes() : null));
						info.setPsManager((!StringHelper.isEmpty(customer.getPsManager()) ? customer.getPsManager() : null));

//						activeUsers activeUsersLast90Days declinePercent				
						if(customer.getCustomFields() != null) {
							percent = 0d;
							for(Field field : customer.getCustomFields()) {
								if(field.getName().equals("pu_bought") && field.getValue() != null) {
									info.setPuBought(info.getPuBought() + Double.parseDouble(field.getValue().toString()));
								} else if(field.getName().equals("pu_utilized") && field.getValue() != null) {
									info.setPuUtilized(info.getPuUtilized() + Double.parseDouble(field.getValue().toString()));
								} else if(field.getName().equals("su_bought") && field.getValue() != null) {
									info.setSuBought(info.getSuBought() + Double.parseDouble(field.getValue().toString()));
								} else if(field.getName().equals("su_utilized") && field.getValue() != null) {
									info.setSuUtilized(info.getSuUtilized() + Double.parseDouble(field.getValue().toString()));
								} else if(field.getName().equals("storage_bought_mb_") && field.getValue() != null) {
									info.setStorageBought(info.getStorageBought() + Double.parseDouble(field.getValue().toString()));
								} else if(field.getName().equals("storage_used_mb_") && field.getValue() != null) {
									info.setStorageUtilized(info.getStorageUtilized() + Double.parseDouble(field.getValue().toString()));
								} else if(field.getName().equals("customer_segment") && field.getValue() != null) {
									info.setSegment(field.getValue().toString());
								} else if(field.getName().equals("active_users") && field.getValue() != null) {
									info.setActiveUsers( info.getActiveUsers() + Double.parseDouble(field.getValue().toString()));
								} else if(field.getName().equals("unique_logins") && field.getValue() != null){
									info.setUniqueLogins(Double.parseDouble(field.getValue().toString()));
								} else if(field.getName().equals("unique_files_downloaded") && field.getValue() != null){
									info.setFileDownloads(Double.parseDouble(field.getValue().toString()));
								}
								
							}
					
							percent = info.getPuUtilized() * 100;
							info.setPuUtilizedPercent((info.getPuBought() != 0) ? Double.valueOf(df.format(percent / info.getPuBought())) : 0);
							percent = 0d;
							percent = info.getSuUtilized() * 100;
							info.setSuUtilizedPercent((info.getSuBought() != 0) ? Double.valueOf(df.format(percent / info.getSuBought())) : 0);
							percent = 0d;
							percent = info.getStorageUtilized() * 100;
							info.setStorageUtilizedPercent((info.getStorageBought() != 0) ? Double.valueOf(df.format(percent / info.getStorageBought())) : 0);
							
							percent = 0d;
							percent = info.getActiveUsers() * 100;
							info.setActiveUsersUtilizedPercent((info.getSuBought() != 0) ? Double.valueOf(df.format(percent / info.getSuBought())) : 0);
							
						}
						
						informationDAO.insert(info);
					} catch(Exception e) {
						logger.error("" + e);
					}
				}
				skip += limit;
			}
		}
		
		return null;
	}
	
	public Map<String, Object> c360docustomertimeseriescalculations(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

		InformationDAO informationDAO = new InformationDAO();
		Calendar calendar = Calendar.getInstance();
		
		Date to = calendar.getTime();
		calendar.add(Calendar.HOUR, cacheHours);
		Date from = calendar.getTime();
		
		boolean infoCached = false;
		Information cachedInfo = informationDAO.findCachedDataEnhanced(account.getId(), rangeFilter, from, to, "TIMESERIES", "CUSTOMER");
		if(cachedInfo != null) {
			infoCached = true;
		}
		
		if(!infoCached) {
			/*logger.debug("range: " + rangeFilter);
			long startTime = new Date().getTime();
			informationDAO.deleteCachedDataWithRange(account.getId(), rangeFilter, null, "TIMESERIES", "CUSTOMER");
			
			Map<String, Object> fieldMap = new HashMap<String, Object>();
			CustomerDAO customerDAO = new CustomerDAO();
			CustomerTimeseriesByDayDAO customerTimeseriesByDayDAO = new CustomerTimeseriesByDayDAO();
			DecimalFormat df = new DecimalFormat("#.###");
			
			Calendar range = Calendar.getInstance();		
			range.set(Calendar.HOUR_OF_DAY, 0);
			range.set(Calendar.MINUTE, 0);
			range.set(Calendar.SECOND, 0);
			range.set(Calendar.MILLISECOND, 0);
			
			Date rangeEnd = calendar.getTime();
			calendar.add(Calendar.DATE, -((!StringHelper.isEmpty(rangeFilter) && !rangeFilter.equalsIgnoreCase("date_range")) ? Integer.parseInt(rangeFilter) : 90));
			Date rangeStart = calendar.getTime();
			long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
			
			Double percent = 0d;
			
			long timeseriesQueryStartTime = 0;
			long timeseriesQueryEndTime = 0;

			int skip = 0;
			int limit = 200;
			while(true) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				
				long customerQueryStartTime = new Date().getTime();
				
				List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
				
				long customerQueryEndTime = new Date().getTime();
				logger.debug("1000 customers: [MS] " + (customerQueryEndTime - customerQueryStartTime) + " [S] " + TimeUnit.MILLISECONDS.toSeconds(customerQueryEndTime - customerQueryStartTime));
				
				if(customers.size() == 0) {
					break;
				}
				for(Customer customer : customers) {
					try {
						
						Information info = new Information();
						info.setAccountId(account.getId());
						info.setType("TIMESERIES");
						info.setEntity("CUSTOMER");
						info.setEntityId(customer.getId());
						info.setName(customer.getName());
						info.setIgnored(customer.getIgnored());
						info.setCustomerTypeId(customer.getCustomerTypeId());
						info.setPartnerId(customer.getPartnerId());
						info.setAccountManager(customer.getAccountManager());
						info.setCsm(customer.getCsm());
						info.setCem(customer.getCem());
						info.setCurrentMrr((customer.getCurrentMrr() != null) ? Double.valueOf(df.format(customer.getCurrentMrr())) : 0d);
						info.setDate(to);
						info.setActiveUsers(0d);
						info.setSuBought(0d);
						info.setUniqueLogins(0d);
						info.setFileDownloads(0d);
						
						info.setCustomerTypes((customer.getCustomerTypes() != null && customer.getCustomerTypes().size() > 0 ? customer.getCustomerTypes() : null));
						info.setPsManager((!StringHelper.isEmpty(customer.getPsManager()) ? customer.getPsManager() : null));
						info.setRangeFilter((!StringHelper.isEmpty(rangeFilter)) ? Long.parseLong(rangeFilter) : null);
						info.setDays(days);
						info.setDateRange((!StringHelper.isEmpty(rangeFilter)) ? "Last " + rangeFilter + " days" : null);
				
						if(customer.getCustomFields() != null) {
							percent = 0d;
							for(Field field : customer.getCustomFields()) {
								if(field.getName().equals("su_bought") && field.getValue() != null) {
									info.setSuBought(info.getSuBought() + Double.parseDouble(field.getValue().toString()));
								} else if(field.getName().equals("customer_segment") && field.getValue() != null) {
									info.setSegment(field.getValue().toString());
								} else if(field.getName().equals("active_users") && field.getValue() != null) {
									info.setActiveUsers( info.getActiveUsers() + Double.parseDouble(field.getValue().toString()));
								} else if(field.getName().equals("unique_logins") && field.getValue() != null){
									info.setUniqueLogins(Double.parseDouble(field.getValue().toString()));
								} else if(field.getName().equals("unique_files_downloaded") && field.getValue() != null){
									info.setFileDownloads(Double.parseDouble(field.getValue().toString()));
								}
							}
						}
						
						timeseriesQueryStartTime = new Date().getTime();
						percent = 0d;
						List<CustomerTimeseriesByDay> timeseries = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "active_users", rangeStart, rangeEnd);
						if(timeseries.size() > 0) {
							info.setActiveUsersLast90Days((timeseries.get(0).getFieldValue() != null && timeseries.get(0).getFieldValue().toString() != null) ? Double.parseDouble(timeseries.get(0).getFieldValue().toString()) : 0);
							info.setDeclineActiveUsersLast90Days((info.getActiveUsers() != null && info.getActiveUsersLast90Days() != null) ? info.getActiveUsers() - info.getActiveUsersLast90Days() : 0);
							percent = (info.getActiveUsersLast90Days() != null && info.getActiveUsersLast90Days() > 0) ? info.getDeclineActiveUsersLast90Days() / info.getActiveUsersLast90Days() : 0;
							percent = percent * 100;
							info.setDeclinePercent(Double.valueOf(df.format(percent)));
						}
						timeseriesQueryEndTime = new Date().getTime();
//						logger.debug("customer ts : range[" + rangeFilter + "][active_users][MS] "+ (timeseriesQueryEndTime - timeseriesQueryStartTime) + " [S] " + TimeUnit.MILLISECONDS.toSeconds(timeseriesQueryEndTime - timeseriesQueryStartTime));
						
						timeseriesQueryStartTime = new Date().getTime();
						percent = 0d;
						List<CustomerTimeseriesByDay> unique_logins_timeseries = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "unique_logins", rangeStart, rangeEnd);
						if(unique_logins_timeseries.size() > 0) {
							info.setUniqueLoginsLast90Days((unique_logins_timeseries.get(0).getFieldValue() != null && unique_logins_timeseries.get(0).getFieldValue().toString() != null) ? Double.parseDouble(unique_logins_timeseries.get(0).getFieldValue().toString()) : 0);
							info.setDeclineUniqueLoginsLast90Days((info.getUniqueLogins() != null && info.getUniqueLoginsLast90Days() != null) ? info.getUniqueLogins() - info.getUniqueLoginsLast90Days() : 0);
							percent = (info.getUniqueLoginsLast90Days() != null && info.getUniqueLoginsLast90Days() > 0) ? info.getDeclineUniqueLoginsLast90Days() / info.getUniqueLoginsLast90Days() : 0;
							percent = percent * 100;
							info.setDeclinePercentInUniqueLogins(Double.valueOf(df.format(percent)));
						}
						timeseriesQueryEndTime = new Date().getTime();
//						logger.debug("customer ts : range[" + rangeFilter + "][unique_logins][MS] "+ (timeseriesQueryEndTime - timeseriesQueryStartTime) + " [S] " + TimeUnit.MILLISECONDS.toSeconds(timeseriesQueryEndTime - timeseriesQueryStartTime));
						
						timeseriesQueryStartTime = new Date().getTime();
						percent = 0d;
						List<CustomerTimeseriesByDay> file_downloads_timeseries = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "unique_files_downloaded", rangeStart, rangeEnd);
						if(file_downloads_timeseries.size() > 0) {
							info.setFileDownloadsLast90Days((file_downloads_timeseries.get(0).getFieldValue() != null && file_downloads_timeseries.get(0).getFieldValue().toString() != null) ? Double.parseDouble(file_downloads_timeseries.get(0).getFieldValue().toString()) : 0 );
							info.setDeclineFileDownloadsLast90Days((info.getFileDownloads() != null && info.getFileDownloadsLast90Days() != null) ? info.getFileDownloads() - info.getFileDownloadsLast90Days() : 0);
							percent = (info.getFileDownloadsLast90Days() != null && info.getFileDownloadsLast90Days() > 0) ? info.getDeclineFileDownloadsLast90Days() / info.getFileDownloadsLast90Days() : 0;
							percent = percent * 100;
							info.setDeclinePercentInfileDownloads(Double.valueOf(df.format(percent)));
						}
						timeseriesQueryEndTime = new Date().getTime();
//						logger.debug("customer ts : range[" + rangeFilter + "][unique_files_downloaded][MS] "+ (timeseriesQueryEndTime - timeseriesQueryStartTime) + " [S] " + TimeUnit.MILLISECONDS.toSeconds(timeseriesQueryEndTime - timeseriesQueryStartTime));
						
						informationDAO.insert(info);
					} catch(Exception e) {
						logger.error("" + e);
					}
				}
				skip += limit;
			}
			
			long endTime = new Date().getTime();
			logger.debug("pod complete time: [MS] " + (endTime - startTime) + " [S] " + TimeUnit.MILLISECONDS.toSeconds(endTime - startTime));*/
		}
		
		return null;
	}
	
	
	public Map<String, Object> c360customersbylast90daysactiveusersegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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
			
			InformationDAO informationDAO = new InformationDAO();
			
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			List<Information> informations = informationDAO.findAll(fieldMap, "activeUsers", false, 0, 20);

			

			List<Object> data = new ArrayList<Object>();
			List<Object> xAxis = new ArrayList<Object>();

			for(Information information : informations) {
				if(information.getActiveUsers() != null){
					Data d = new Data();
					d.setY(information.getActiveUsers().longValue());
					String name = information.getName().toString();
					if(name.length() > 15) {
						name = name.substring(0, 15) + "...";
					}
					d.setName(name);
					String url = "/customerdetails/" + information.getEntityId();
					d.setUrl(url);
					xAxis.add(name);
					data.add(d);
				}
			}

			List<Series> seriesList = new ArrayList<Series>();

			Series series = new Series();
			series.setData(data);
			series.setName("Customers");
			series.setType("column");
			series.setShowInLegend(true);
			seriesList.add(series);

			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("xy"));
			highchart.setTitle(new Title("Customers by Active Users"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
			//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Customer Count"), 0, new Labels("{value}", new Style("red")), false));
//			yAxisList.add(new YAxis(new Title("No. of Power Users"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, -90, -15, "#AAA"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerbyindustryegyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360customersby90daysdecliningactiveusersegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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

		Map<String, Object> fieldMap = new LinkedHashMap<String, Object>();
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
			
			InformationDAO informationDAO = new InformationDAO();
			
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			fieldMap.put("declineActiveUsersLast90Days", new BasicDBObject("$ne", null));
			List<Information> informations = informationDAO.findAll(fieldMap, "declineActiveUsersLast90Days", true, 0, 15);

			List<Object> xAxis = new ArrayList<Object>();
			List<Object> currentData = new ArrayList<Object>();
			List<Object> lastData = new ArrayList<Object>();
			for(Information information : informations) {
				String name = information.getName();
				if(name.length() > 15) {
					name = name.substring(0, 15) + "...";
				}
				xAxis.add(name);
				Data d = new Data();
				d.setY(information.getActiveUsers().longValue());
				d.setUrl("/customerdetails/" + information.getEntityId());
				currentData.add(d);
				d = new Data();
				d.setY(information.getActiveUsersLast90Days().longValue());
				d.setUrl("/customerdetails/" + information.getEntityId());
				lastData.add(d);
			}
			
			
			List<Series> seriesList = new ArrayList<Series>();
			
			Series series = new Series();
			series.setData(lastData);
			series.setName("Active Users 90 days Ago");
			series.setShowInLegend(true);
			series.setColor("#434348");
			seriesList.add(series);
			
			
			series = new Series();
			series.setName("Active Users Current");
			series.setData(currentData);
			series.setShowInLegend(true);
			series.setColor("#cd392f");
			seriesList.add(series);

			
			Highchart highchart = new Highchart();
			highchart.setChart(new Chart("column", null));
			highchart.setTitle(new Title("Declining Active Users - 90 days"));

			List<XAxis> xAxisList = new ArrayList<XAxis>();
			xAxisList.add(new XAxis(xAxis, new Labels(-90, 1), true));
			//					xAxisList.add(new XAxis(xAxis, new Labels(0, 1), true));
			highchart.setxAxis(xAxisList);

			List<YAxis> yAxisList = new ArrayList<YAxis>();
			yAxisList.add(new YAxis(new Title("Decline"), 0, new Labels("{value}", new Style("red")), false));
//			yAxisList.add(new YAxis(new Title("No. of Power Users"), 0, new Labels("{value}", new Style("green")), true));
			highchart.setyAxis(yAxisList);

			highchart.setPlotOptions(new PlotOptions(new Column(null, new DataLabels(true, -90, -15, "#AAA"), new Point(new Events("function(e){if(typeof this.url != 'undefined') { window.location = this.url;}}")))));

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360customerbyindustryegyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360customersbyactiveusersrateegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, int page, String sortBy, boolean ascending) throws Exception {
        
		InformationDAO informationDAO = new InformationDAO();
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");
		
		Calendar calendar = Calendar.getInstance();		
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		
		if(paramMap.get("rangeStartDate") != null && paramMap.get("rangeEndDate") != null) {
			logger.debug("inside active users custom range");
			
			TempDAO tempDAO = new TempDAO();
			
			String rangeStartDate = paramMap.get("rangeStartDate").toString();
			String rangeEndDate = paramMap.get("rangeEndDate").toString();
			
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("entity", "CUSTOMER");
			fieldMap.put("type", "CUSTOMTIMESERIES");
			fieldMap.put("dateRange", rangeStartDate + " - " + rangeEndDate);
			
			Temp temp = tempDAO.findOne(fieldMap);
			if(temp == null) {
				long startTime = new Date().getTime();
				
				CustomerTimeseriesByDayContentIndex indexer = new CustomerTimeseriesByDayContentIndex();
				CustomerDAO customerDAO = new CustomerDAO();
				DecimalFormat df = new DecimalFormat("#.###");
				
				Double percent = 0d;
				Date to = dateFormatter.parse(rangeEndDate);
				Date from = dateFormatter.parse(rangeStartDate);

				Calendar range = Calendar.getInstance();
				range.setTime(to);
				range.set(Calendar.HOUR_OF_DAY, 0);
				range.set(Calendar.MINUTE, 0);
				range.set(Calendar.SECOND, 0);
				range.set(Calendar.MILLISECOND, 0);

				to = range.getTime();

				range.setTime(from);
				range.set(Calendar.HOUR_OF_DAY, 0);
				range.set(Calendar.MINUTE, 0);
				range.set(Calendar.SECOND, 0);
				range.set(Calendar.MILLISECOND, 0);

				from = range.getTime();
				
				long days = TimeUnit.MILLISECONDS.toDays(to.getTime() - from.getTime());
				
				logger.debug("from: " + from + "\t" + to);
				
				int skip = 0;
				int limit = 200;
				while(true) {
					SearchResult sr = indexer.searchDateSpecificData(account.getId(), "active_users", from, to, limit, skip);
					if(sr != null && sr.getDocuments() != null) {
						List<CustomerTimeseriesByDayContentDoc> timeseries = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
						if(timeseries == null || timeseries.size() == 0) {
							break;
						}
						System.out.println("\thits: " + sr.getHits() + "\tdocs: " + timeseries.size());

						for(CustomerTimeseriesByDayContentDoc t : timeseries) {
							try {
								Customer customer = customerDAO.findOneById(t.getCustomerId());
								if(customer != null && customer.getCustomFields() != null) {
									
									Temp info = new Temp();
									info.setPodId(podId);
									info.setAccountId(account.getId());
									info.setType("CUSTOMTIMESERIES");
									info.setEntity("CUSTOMER");
									info.setEntityId(customer.getId());
									info.setName(customer.getName());
									info.setIgnored(customer.getIgnored());
									info.setCustomerTypeId(customer.getCustomerTypeId());
									info.setPartnerId(customer.getPartnerId());
									info.setAccountManager(customer.getAccountManager());
									info.setCsm(customer.getCsm());
									info.setCem(customer.getCem());
									info.setCurrentMrr((customer.getCurrentMrr() != null) ? Double.valueOf(df.format(customer.getCurrentMrr())) : 0d);
									info.setDate(to);
									info.setCustomerTypes((customer.getCustomerTypes() != null && customer.getCustomerTypes().size() > 0 ? customer.getCustomerTypes() : null));
									info.setGroups((customer.getGroups() != null && customer.getGroups().size() > 0) ? customer.getGroups() : null);
									info.setPsManager((!StringHelper.isEmpty(customer.getPsManager()) ? customer.getPsManager() : null));

									info.setActiveUsers(0d);
									info.setActiveUsersLast90Days(0d);
									info.setDeclineActiveUsersLast90Days(0d);
									info.setDeclinePercent(0d);

									info.setRangeFilter(days);
									info.setDays(days);
									info.setDateRange(rangeStartDate + " - " + rangeEndDate);

									for(com.crucialbits.cy.model.Field field : customer.getCustomFields()) {
										if(field.getName().equals("customer_segment") && field.getValue() != null) {
											info.setSegment(field.getValue().toString());
										} else if(field.getName().equals("active_users") && field.getValue() != null) {
											info.setActiveUsers(Double.parseDouble(field.getValue().toString()));
										}
									}

									info.setActiveUsersLast90Days((t.getFieldValue() != null) ? Double.parseDouble(t.getFieldValue().toString()) : 0d);
									info.setDeclineActiveUsersLast90Days(info.getActiveUsers() - info.getActiveUsersLast90Days());
									percent = (info.getActiveUsersLast90Days() != 0) ? info.getDeclineActiveUsersLast90Days() / info.getActiveUsersLast90Days() : 0;
									percent = percent * 100;
									info.setDeclinePercent(Double.valueOf(df.format(percent)));

									tempDAO.insert(info);

								}
							} catch(Exception e) {
								e.printStackTrace();
							}
						}


					} else {
						break;
					}

					skip += limit;
				}
				
				long endTime = new Date().getTime();
				long diff = endTime - startTime;
				logger.debug("by c360customersbyactiveusersrateegnyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));
			}
			
			paramMap.remove("rangeStartDate");
			paramMap.remove("rangeEndDate");
			
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("entity", "CUSTOMER");
			fieldMap.put("type", "CUSTOMTIMESERIES");
			fieldMap.put("dateRange", rangeStartDate + " - " + rangeEndDate);
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			
			if(StringHelper.isEmpty(sortBy)) {
				sortBy = "declinePercent";
			}
			
			int skip = 0;
			int limit = 15;
			int numTabsToShown = 7;
			skip = (page - 1) * limit;
			
			for(Map.Entry<String, Object> m : fieldMap.entrySet()) {
				logger.debug(m.getKey() + "\t" + m.getValue());
			}
			
			List<Temp> informations = tempDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
			long totalCount = tempDAO.countByFilters(account.getId(), "CUSTOMTIMESERIES", "CUSTOMER", fieldMap);

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

			cachedData.put("dateRange", (informations.size() > 0) ? informations.get(0).getDateRange() : null);
			cachedData.put("requestOn", "yes");
			
			cachedData.put("listName", "c360customersbyactiveusersrateegnyte");
			cachedData.put("apiName", "c360customersbyactiveusersrateegnyte");
			cachedData.put("sortBy", sortBy);
			cachedData.put("ascending", ascending);
			cachedData.put("rangeFilter", rangeFilter);
			cachedData.put("podId", podId);
			cachedData.put("customerTypeId", paramMap.get("customerTypes"));
			cachedData.put("groupId", paramMap.get("groups"));
			cachedData.put("rangeStartDate", rangeStartDate);
			cachedData.put("rangeEndDate", rangeEndDate);
			cachedData.put("title", "Customers By Change In Active Users");
			String subTitle = rangeStartDate + " - " + rangeEndDate;
			cachedData.put("subTitle", subTitle);
			cachedData.put("informations", informations);
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					cachedData.put(mp.getKey(), mp.getValue());
				}
			}
			cachedData.put("C360_CUSTOMERS_BY_ACTIVE_USERS_RATE_EGNYTE", "yes");
			
			logger.debug("returning before custom C360_CUSTOMERS_BY_ACTIVE_USERS_RATE_EGNYTE");
			return cachedData;
		}
		
		if(true) {
			Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
			Date rangeEnd = calendar.getTime();
			calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
			Date rangeStart = calendar.getTime();
			String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
			
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("entity", "CUSTOMER");
			fieldMap.put("type", "TIMESERIES");
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				fieldMap.put("rangeFilter", rangeValue);
			}
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			
			if(StringHelper.isEmpty(sortBy)) {
				sortBy = "declinePercent";
			}
			
			int skip = 0;
			int limit = 15;
			int numTabsToShown = 7;
			skip = (page - 1) * limit;
			
			List<Information> informations = informationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
			long totalCount = informationDAO.countByFilters(account.getId(), "TIMESERIES", "CUSTOMER", fieldMap);
			
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

			cachedData.put("days", (informations.size() > 0) ? informations.get(0).getDays() : "x");
			cachedData.put("dateRange", (informations.size() > 0) ? informations.get(0).getDateRange() : null);
			cachedData.put("requestOn", "yes");
			cachedData.put("listName", "c360customersbyactiveusersrateegnyte");
			cachedData.put("apiName", "c360customersbyactiveusersrateegnyte");
			cachedData.put("sortBy", sortBy);
			cachedData.put("ascending", ascending);
			cachedData.put("rangeFilter", rangeFilter);
			cachedData.put("podId", podId);
			cachedData.put("customerTypeId", paramMap.get("customerTypes"));
			cachedData.put("groupId", paramMap.get("groups"));
			cachedData.put("title", "Customers By Change In Active Users");
			cachedData.put("subTitle", subTitle);
			cachedData.put("informations", informations);
			cachedData.put("pageCount", pageCount);
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					cachedData.put(mp.getKey(), mp.getValue());
				}
			}
			cachedData.put("C360_CUSTOMERS_BY_ACTIVE_USERS_RATE_EGNYTE", "yes");
			
			logger.debug("returning before existing C360_CUSTOMERS_BY_ACTIVE_USERS_RATE_EGNYTE");
		}
		
		return cachedData;
	}
	
	public Map<String, Object> c360customersbyuniqueloginsrateegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, int page, String sortBy, boolean ascending) throws Exception {
        
		InformationDAO informationDAO = new InformationDAO();
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy"); 
		
		Calendar calendar = Calendar.getInstance();		
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		
		if(paramMap.get("rangeStartDate") != null && paramMap.get("rangeEndDate") != null) {
			logger.debug("inside unique custom range");
			
			TempDAO tempDAO = new TempDAO();
			
			String rangeStartDate = paramMap.get("rangeStartDate").toString();
			String rangeEndDate = paramMap.get("rangeEndDate").toString();
			
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("entity", "CUSTOMER");
			fieldMap.put("type", "CUSTOMTIMESERIES");
			fieldMap.put("dateRange", rangeStartDate + " - " + rangeEndDate);
			
			Temp temp = tempDAO.findOne(fieldMap);
			if(temp == null) {
				long startTime = new Date().getTime();
				
				CustomerTimeseriesByDayContentIndex indexer = new CustomerTimeseriesByDayContentIndex();
				CustomerDAO customerDAO = new CustomerDAO();
				DecimalFormat df = new DecimalFormat("#.###");
				
				Double percent = 0d;
				Date to = dateFormatter.parse(rangeEndDate);
				Date from = dateFormatter.parse(rangeStartDate);

				Calendar range = Calendar.getInstance();
				range.setTime(to);
				range.set(Calendar.HOUR_OF_DAY, 0);
				range.set(Calendar.MINUTE, 0);
				range.set(Calendar.SECOND, 0);
				range.set(Calendar.MILLISECOND, 0);

				to = range.getTime();

				range.setTime(from);
				range.set(Calendar.HOUR_OF_DAY, 0);
				range.set(Calendar.MINUTE, 0);
				range.set(Calendar.SECOND, 0);
				range.set(Calendar.MILLISECOND, 0);

				from = range.getTime();
				
				long days = TimeUnit.MILLISECONDS.toDays(to.getTime() - from.getTime());
				
				logger.debug("from: " + from + "\t" + to);
				
				int skip = 0;
				int limit = 200;
				while(true) {
					SearchResult sr = indexer.searchDateSpecificData(account.getId(), "unique_logins", from, to, limit, skip);
					if(sr != null && sr.getDocuments() != null) {
						List<CustomerTimeseriesByDayContentDoc> timeseries = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
						if(timeseries == null || timeseries.size() == 0) {
							break;
						}
						System.out.println("\thits: " + sr.getHits() + "\tdocs: " + timeseries.size());

						for(CustomerTimeseriesByDayContentDoc t : timeseries) {
							try {
								Customer customer = customerDAO.findOneById(t.getCustomerId());
								if(customer != null && customer.getCustomFields() != null) {
									
									Temp info = new Temp();
									info.setPodId(podId);
									info.setAccountId(account.getId());
									info.setType("CUSTOMTIMESERIES");
									info.setEntity("CUSTOMER");
									info.setEntityId(customer.getId());
									info.setName(customer.getName());
									info.setIgnored(customer.getIgnored());
									info.setCustomerTypeId(customer.getCustomerTypeId());
									info.setPartnerId(customer.getPartnerId());
									info.setAccountManager(customer.getAccountManager());
									info.setCsm(customer.getCsm());
									info.setCem(customer.getCem());
									info.setCurrentMrr((customer.getCurrentMrr() != null) ? Double.valueOf(df.format(customer.getCurrentMrr())) : 0d);
									info.setDate(to);
									info.setCustomerTypes((customer.getCustomerTypes() != null && customer.getCustomerTypes().size() > 0 ? customer.getCustomerTypes() : null));
									info.setGroups((customer.getGroups() != null && customer.getGroups().size() > 0) ? customer.getGroups() : null);
									info.setPsManager((!StringHelper.isEmpty(customer.getPsManager()) ? customer.getPsManager() : null));

									info.setUniqueLogins(0d);
									info.setUniqueLoginsLast90Days(0d);
									info.setDeclineUniqueLoginsLast90Days(0d);
									info.setDeclinePercent(0d);

									info.setRangeFilter(days);
									info.setDays(days);
									info.setDateRange(rangeStartDate + " - " + rangeEndDate);

									for(com.crucialbits.cy.model.Field field : customer.getCustomFields()) {
										if(field.getName().equals("customer_segment") && field.getValue() != null) {
											info.setSegment(field.getValue().toString());
										} else if(field.getName().equals("unique_logins") && field.getValue() != null) {
											info.setUniqueLogins(Double.parseDouble(field.getValue().toString()));
										}
									}

									info.setUniqueLoginsLast90Days((t.getFieldValue() != null) ? Double.parseDouble(t.getFieldValue().toString()) : 0d);
									info.setDeclineUniqueLoginsLast90Days(info.getUniqueLogins() - info.getUniqueLoginsLast90Days());
									percent = (info.getUniqueLoginsLast90Days() != 0) ? info.getDeclineUniqueLoginsLast90Days() / info.getUniqueLoginsLast90Days() : 0;
									percent = percent * 100;
									info.setDeclinePercentInUniqueLogins(Double.valueOf(df.format(percent)));

									tempDAO.insert(info);

								}
							} catch(Exception e) {
								e.printStackTrace();
							}
						}


					} else {
						break;
					}

					skip += limit;
				}
				
				long endTime = new Date().getTime();
				long diff = endTime - startTime;
				logger.debug("by c360customersbyuniqueloginsrateegnyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));
			}
			
			paramMap.remove("rangeStartDate");
			paramMap.remove("rangeEndDate");
			
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("podId", podId);
			fieldMap.put("entity", "CUSTOMER");
			fieldMap.put("type", "CUSTOMTIMESERIES");
			fieldMap.put("dateRange", rangeStartDate + " - " + rangeEndDate);
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			
			if(StringHelper.isEmpty(sortBy)) {
				sortBy = "declinePercent";
			}
			
			int skip = 0;
			int limit = 15;
			int numTabsToShown = 7;
			skip = (page - 1) * limit;
			
			for(Map.Entry<String, Object> m : fieldMap.entrySet()) {
				logger.debug(m.getKey() + "\t" + m.getValue());
			}
			
			List<Temp> informations = tempDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
			long totalCount = tempDAO.countByFilters(account.getId(), "CUSTOMTIMESERIES", "CUSTOMER", fieldMap);

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

			cachedData.put("dateRange", (informations.size() > 0) ? informations.get(0).getDateRange() : null);
			cachedData.put("requestOn", "yes");
			
			cachedData.put("listName", "c360customersbyuniqueloginsrateegnyte");
			cachedData.put("apiName", "c360customersbyuniqueloginsrateegnyte");
			cachedData.put("sortBy", sortBy);
			cachedData.put("ascending", ascending);
			cachedData.put("rangeFilter", rangeFilter);
			cachedData.put("podId", podId);
			cachedData.put("customerTypeId", paramMap.get("customerTypes"));
			cachedData.put("groupId", paramMap.get("groups"));
			cachedData.put("rangeStartDate", rangeStartDate);
			cachedData.put("rangeEndDate", rangeEndDate);
			cachedData.put("title", "Customers By Change In Unique Logins");
			cachedData.put("subTitle", rangeStartDate + " - " + rangeEndDate);
			cachedData.put("informations", informations);
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					cachedData.put(mp.getKey(), mp.getValue());
				}
			}
			cachedData.put("C360_CUSTOMERS_BY_UNIQUE_LOGINS_RATE_EGNYTE", "yes");
			
			logger.debug("returning before custom C360_CUSTOMERS_BY_UNIQUE_LOGINS_RATE_EGNYTE");
			return cachedData;
		}
		
		if(true) {
			Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
			Date rangeEnd = calendar.getTime();
			calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
			Date rangeStart = calendar.getTime();
			String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
			
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("entity", "CUSTOMER");
			fieldMap.put("type", "TIMESERIES");
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				fieldMap.put("rangeFilter", (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter));
			}
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			
			if(StringHelper.isEmpty(sortBy)) {
				sortBy = "declinePercentInUniqueLogins";
			}
			
			int skip = 0;
			int limit = 15;
			int numTabsToShown = 7;
			skip = (page - 1) * limit;
			
			List<Information> informations = informationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
			long totalCount = informationDAO.countByFilters(account.getId(), "TIMESERIES", "CUSTOMER", fieldMap);
			
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

			cachedData.put("days", (informations.size() > 0) ? informations.get(0).getDays() : "x");
			cachedData.put("dateRange", (informations.size() > 0) ? informations.get(0).getDateRange() : null);
			cachedData.put("requestOn", "yes");
			cachedData.put("listName", "c360customersbyuniqueloginsrateegnyte");
			cachedData.put("apiName", "c360customersbyuniqueloginsrateegnyte");
			cachedData.put("sortBy", sortBy);
			cachedData.put("ascending", ascending);
			cachedData.put("rangeFilter", rangeFilter);
			cachedData.put("podId", podId);
			cachedData.put("customerTypeId", paramMap.get("customerTypes"));
			cachedData.put("groupId", paramMap.get("groups"));
			cachedData.put("title", "Customers By Change In Unique Logins");
			cachedData.put("subTitle", subTitle);
			cachedData.put("informations", informations);
			cachedData.put("pageCount", pageCount);
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					cachedData.put(mp.getKey(), mp.getValue());
				}
			}
			cachedData.put("C360_CUSTOMERS_BY_UNIQUE_LOGINS_RATE_EGNYTE", "yes");
		}
		
		return cachedData;
	}

public Map<String, Object> c360customersbyfiledownloadsrateegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, int page, String sortBy, boolean ascending) throws Exception {
    
	InformationDAO informationDAO = new InformationDAO();
	Map<String, Object> fieldMap = new HashMap<String, Object>();
	Map<String, Object> cachedData = new HashMap<String, Object>();
	SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy"); 
	
	Calendar calendar = Calendar.getInstance();		
	calendar.set(Calendar.HOUR_OF_DAY, 0);
	calendar.set(Calendar.MINUTE, 0);
	calendar.set(Calendar.SECOND, 0);
	calendar.set(Calendar.MILLISECOND, 0);
	
	if(paramMap.get("rangeStartDate") != null && paramMap.get("rangeEndDate") != null) {
		logger.debug("inside file downloads custom range");
		
		TempDAO tempDAO = new TempDAO();
		
		String rangeStartDate = paramMap.get("rangeStartDate").toString();
		String rangeEndDate = paramMap.get("rangeEndDate").toString();
		
		fieldMap.clear();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("entity", "CUSTOMER");
		fieldMap.put("type", "CUSTOMTIMESERIES");
		fieldMap.put("dateRange", rangeStartDate + " - " + rangeEndDate);
		
		Temp temp = tempDAO.findOne(fieldMap);
		if(temp == null) {
			long startTime = new Date().getTime();
			
			CustomerTimeseriesByDayContentIndex indexer = new CustomerTimeseriesByDayContentIndex();
			CustomerDAO customerDAO = new CustomerDAO();
			DecimalFormat df = new DecimalFormat("#.###");
			
			Double percent = 0d;
			Date to = dateFormatter.parse(rangeEndDate);
			Date from = dateFormatter.parse(rangeStartDate);

			Calendar range = Calendar.getInstance();
			range.setTime(to);
			range.set(Calendar.HOUR_OF_DAY, 0);
			range.set(Calendar.MINUTE, 0);
			range.set(Calendar.SECOND, 0);
			range.set(Calendar.MILLISECOND, 0);

			to = range.getTime();

			range.setTime(from);
			range.set(Calendar.HOUR_OF_DAY, 0);
			range.set(Calendar.MINUTE, 0);
			range.set(Calendar.SECOND, 0);
			range.set(Calendar.MILLISECOND, 0);

			from = range.getTime();
			
			long days = TimeUnit.MILLISECONDS.toDays(to.getTime() - from.getTime());
			
			logger.debug("from: " + from + "\t" + to);
			
			int skip = 0;
			int limit = 200;
			while(true) {
				SearchResult sr = indexer.searchDateSpecificData(account.getId(), "unique_files_downloaded", from, to, limit, skip);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> timeseries = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					if(timeseries == null || timeseries.size() == 0) {
						break;
					}
					System.out.println("\thits: " + sr.getHits() + "\tdocs: " + timeseries.size());

					for(CustomerTimeseriesByDayContentDoc t : timeseries) {
						try {
							Customer customer = customerDAO.findOneById(t.getCustomerId());
							if(customer != null && customer.getCustomFields() != null) {
								
								Temp info = new Temp();
								info.setPodId(podId);
								info.setAccountId(account.getId());
								info.setType("CUSTOMTIMESERIES");
								info.setEntity("CUSTOMER");
								info.setEntityId(customer.getId());
								info.setName(customer.getName());
								info.setIgnored(customer.getIgnored());
								info.setCustomerTypeId(customer.getCustomerTypeId());
								info.setPartnerId(customer.getPartnerId());
								info.setAccountManager(customer.getAccountManager());
								info.setCsm(customer.getCsm());
								info.setCem(customer.getCem());
								info.setCurrentMrr((customer.getCurrentMrr() != null) ? Double.valueOf(df.format(customer.getCurrentMrr())) : 0d);
								info.setDate(to);
								info.setCustomerTypes((customer.getCustomerTypes() != null && customer.getCustomerTypes().size() > 0 ? customer.getCustomerTypes() : null));
								info.setGroups((customer.getGroups() != null && customer.getGroups().size() > 0) ? customer.getGroups() : null);
								info.setPsManager((!StringHelper.isEmpty(customer.getPsManager()) ? customer.getPsManager() : null));

								info.setFileDownloads(0d);
								info.setFileDownloadsLast90Days(0d);
								info.setDeclineFileDownloadsLast90Days(0d);
								info.setDeclinePercent(0d);

								info.setRangeFilter(days);
								info.setDays(days);
								info.setDateRange(rangeStartDate + " - " + rangeEndDate);

								for(com.crucialbits.cy.model.Field field : customer.getCustomFields()) {
									if(field.getName().equals("customer_segment") && field.getValue() != null) {
										info.setSegment(field.getValue().toString());
									} else if(field.getName().equals("unique_files_downloaded") && field.getValue() != null) {
										info.setActiveUsers(Double.parseDouble(field.getValue().toString()));
									}
								}

								info.setFileDownloadsLast90Days((t.getFieldValue() != null) ? Double.parseDouble(t.getFieldValue().toString()) : 0d);
								info.setDeclineFileDownloadsLast90Days(info.getFileDownloads() - info.getFileDownloadsLast90Days());
								percent = (info.getFileDownloadsLast90Days() != 0) ? info.getDeclineFileDownloadsLast90Days() / info.getFileDownloadsLast90Days() : 0;
								percent = percent * 100;
								info.setDeclinePercentInfileDownloads(Double.valueOf(df.format(percent)));

								tempDAO.insert(info);

							}
						} catch(Exception e) {
							e.printStackTrace();
						}
					}


				} else {
					break;
				}

				skip += limit;
			}
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
			logger.debug("by c360customersbyfiledownloadsrateegnyte [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));
		}
		
		paramMap.remove("rangeStartDate");
		paramMap.remove("rangeEndDate");
		
		fieldMap.clear();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("podId", podId);
		fieldMap.put("entity", "CUSTOMER");
		fieldMap.put("type", "CUSTOMTIMESERIES");
		fieldMap.put("dateRange", rangeStartDate + " - " + rangeEndDate);
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				fieldMap.put(mp.getKey(), mp.getValue()); 
			}
		}
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "declinePercent";
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		for(Map.Entry<String, Object> m : fieldMap.entrySet()) {
			logger.debug(m.getKey() + "\t" + m.getValue());
		}
		
		List<Temp> informations = tempDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = tempDAO.countByFilters(account.getId(), "CUSTOMTIMESERIES", "CUSTOMER", fieldMap);

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

		cachedData.put("dateRange", (informations.size() > 0) ? informations.get(0).getDateRange() : null);
		cachedData.put("requestOn", "yes");
		
		cachedData.put("listName", "c360customersbyfiledownloadsrateegnyte");
		cachedData.put("apiName", "c360customersbyfiledownloadsrateegnyte");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerTypeId", paramMap.get("customerTypes"));
		cachedData.put("groupId", paramMap.get("groups"));
		cachedData.put("rangeStartDate", rangeStartDate);
		cachedData.put("rangeEndDate", rangeEndDate);
		cachedData.put("title", "Customers By Change In File Downloads");
		cachedData.put("subTitle", rangeStartDate + " - " + rangeEndDate);
		cachedData.put("informations", informations);
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				cachedData.put(mp.getKey(), mp.getValue());
			}
		}
		cachedData.put("C360_CUSTOMERS_BY_FILE_DOWNLOADS_RATE_EGNYTE", "yes");
		
		logger.debug("returning before custom C360_CUSTOMERS_BY_FILE_DOWNLOADS_RATE_EGNYTE");
		return cachedData;
	}
	
	if(true) {
		Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
		Date rangeEnd = calendar.getTime();
		calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
		Date rangeStart = calendar.getTime();
		String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
		
		fieldMap.clear();
		fieldMap.put("accountId", account.getId());
		fieldMap.put("entity", "CUSTOMER");
		fieldMap.put("type", "TIMESERIES");
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			fieldMap.put("rangeFilter", (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter));
		}
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				fieldMap.put(mp.getKey(), mp.getValue()); 
			}
		}
		
		if(StringHelper.isEmpty(sortBy)) {
			sortBy = "declinePercentInfileDownloads";
		}
		
		int skip = 0;
		int limit = 15;
		int numTabsToShown = 7;
		skip = (page - 1) * limit;
		
		List<Information> informations = informationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
		long totalCount = informationDAO.countByFilters(account.getId(), "TIMESERIES", "CUSTOMER", fieldMap);
		
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

		cachedData.put("days", (informations.size() > 0) ? informations.get(0).getDays() : "x");
		cachedData.put("dateRange", (informations.size() > 0) ? informations.get(0).getDateRange() : null);
		cachedData.put("requestOn", "yes");
		cachedData.put("listName", "c360customersbyfiledownloadsrateegnyte");
		cachedData.put("apiName", "c360customersbyfiledownloadsrateegnyte");
		cachedData.put("sortBy", sortBy);
		cachedData.put("ascending", ascending);
		cachedData.put("rangeFilter", rangeFilter);
		cachedData.put("podId", podId);
		cachedData.put("customerTypeId", paramMap.get("customerTypes"));
		cachedData.put("groupId", paramMap.get("groups"));
		cachedData.put("title", "Customers By Change In File Downloads");
		cachedData.put("subTitle", subTitle);
		cachedData.put("informations", informations);
		cachedData.put("pageCount", pageCount);
		for(Map.Entry<String, Object> mp : paramMap.entrySet()){
			if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
				cachedData.put(mp.getKey(), mp.getValue());
			}
		}
		cachedData.put("C360_CUSTOMERS_BY_FILE_DOWNLOADS_RATE_EGNYTE", "yes");
	}
	
	return cachedData;
}


//SU Bought vs Active Users
	public Map<String, Object> c360activevssuboughttrafficseriesegnyte(
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
		
			SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy"); 
			TimeseriesByDayDAO timeseriesByDayDAO = new TimeseriesByDayDAO();
			DecimalFormat df1Precision = new DecimalFormat("#");
	
			Calendar calendar = Calendar.getInstance();		
			calendar.set(Calendar.HOUR_OF_DAY, 0);
			calendar.set(Calendar.MINUTE, 0);
			calendar.set(Calendar.SECOND, 0);
			calendar.set(Calendar.MILLISECOND, 0);
			
			Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
			Date rangeEnd = calendar.getTime();
			calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
			Date rangeStart = calendar.getTime();
			String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";		
			long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
			
			Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();				
			Map<Date, Long> sbMap = new LinkedHashMap<Date, Long>();
			
			if(paramMap.get("rangeStartDate") != null && paramMap.get("rangeEndDate") != null) {
				subTitle = paramMap.get("rangeStartDate").toString() + " - " + paramMap.get("rangeEndDate").toString();
				calendar.setTime(dateFormatter.parse(paramMap.get("rangeStartDate").toString()));
				rangeStart = calendar.getTime();
				calendar.setTime(dateFormatter.parse(paramMap.get("rangeEndDate").toString()));
				rangeEnd = calendar.getTime();
			}
			
			List<TimeseriesByDay> list = timeseriesByDayDAO.getData(account.getId(), "su_bought", rangeStart, rangeEnd);
			for(TimeseriesByDay ctd : list) {
				if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
					pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
				} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
					pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
				}
			}
		
			list = timeseriesByDayDAO.getData(account.getId(), "active_users", rangeStart, rangeEnd);
			
			for(TimeseriesByDay ctd : list) {
				if(!sbMap.containsKey(ctd.getDate()) && sbMap.size() < days && ctd.getFieldValue() != null) {
					sbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
				} else if(sbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
					sbMap.put(ctd.getDate(), sbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
				}
			}
			
			List<Series> seriesList = new ArrayList<Series>();
			Series series = new Series();
			series.setName("Active Users");
			List<Object> data = new ArrayList<Object>();
			for(Map.Entry<Date, Long> entry : sbMap.entrySet()) {
				Data dta = new Data();								
				dta.setX(entry.getKey().getTime());					
				dta.setY(entry.getValue().longValue());
				dta.setInfo("Active Users: " + entry.getValue().longValue());
				data.add(dta);
			}
				
			series.setData(data);
			series.setColor("#fe5400");
			series.setShowInLegend(true);
			seriesList.add(series);
			
			series = new Series();
			data = new ArrayList<Object>();
			series.setName("Licensed Users");
			for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
				Data dta = new Data();				
				dta.setX(entry.getKey().getTime());					
				dta.setY(entry.getValue().longValue());
				dta.setInfo("Licensed Users: " + entry.getValue().longValue());
				data.add(dta);
			}
				
			series.setData(data);
			series.setColor("#4E387E");
			series.setShowInLegend(true);
			seriesList.add(series);
			
			
			Highchart highchart = new Highchart();

			highchart.setChart(new Chart("spline", null));
			highchart.setTitle(new Title("Active/Licensed Users"));
			if(!StringHelper.isEmpty(rangeFilter)) {
				highchart.setSubtitle(new Subtitle(subTitle));
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
//			logger.debug("by c360customerby SU bought Utilized [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

			return newCache.getData();
		}
		return cache.getData();
	}

	public Map<String, Object> cdetailsactivevssuboughttrafficseriesegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, String rangeStartDate, String rangeEndDate, String customerId, Integer cacheHours) throws Exception {
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
			params += "&rangeStartDate=" + rangeStartDate; 
			params += "&rangeEndDate=" + rangeEndDate; 
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

			if(customer != null) {
				long startTime = new Date().getTime();
				
				SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");
//				CustomerTimeseriesByDayDAO customerTimeseriesByDayDAO = new CustomerTimeseriesByDayDAO();
				CustomerTimeseriesByDayContentIndex indexer = new CustomerTimeseriesByDayContentIndex();
			
				Calendar calendar = Calendar.getInstance();		
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				
				Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
				Date rangeEnd = calendar.getTime();
				calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
				Date rangeStart = calendar.getTime();
				String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
				
				logger.debug(rangeStartDate + "\t" + rangeEndDate);
				if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
					subTitle = rangeStartDate + " - " + rangeEndDate;
					calendar.setTime(dateFormatter.parse(rangeStartDate));
					rangeStart = calendar.getTime();
					calendar.setTime(dateFormatter.parse(rangeEndDate));
					rangeEnd = calendar.getTime();
				}
				
				logger.debug(rangeStart + "\t" + rangeEnd);
				long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
				
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();				
				Map<Date, Long> sbMap = new LinkedHashMap<Date, Long>();
				
				/*List<CustomerTimeseriesByDay> list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "su_bought", rangeStart, rangeEnd);
				
				for(CustomerTimeseriesByDay ctd : list) {
					if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}
			
				list = customerTimeseriesByDayDAO.getData(account.getId(), customer.getId(), "active_users", rangeStart, rangeEnd);
				
				for(CustomerTimeseriesByDay ctd : list) {
					if(!sbMap.containsKey(ctd.getDate()) && sbMap.size() < days && ctd.getFieldValue() != null) {
						sbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					} else if(sbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
						sbMap.put(ctd.getDate(), sbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
					}
				}*/
				
				Calendar tempDate = Calendar.getInstance();
				
				SearchResult sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "su_bought", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						tempDate.setTime(ctd.getDate());
						tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
						
						if(!pbMap.containsKey(tempDate.getTime()) && pbMap.size() < days && ctd.getFieldValue() != null) {
							pbMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(pbMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
							pbMap.put(tempDate.getTime(), pbMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "active_users", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						tempDate.setTime(ctd.getDate());
						tempDate = Utility.getInstance().setTimeToBeginningOfDay(tempDate);
						
						if(!sbMap.containsKey(tempDate.getTime()) && sbMap.size() < days && ctd.getFieldValue() != null) {
							sbMap.put(tempDate.getTime(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(sbMap.containsKey(tempDate.getTime()) && ctd.getFieldValue() != null) {
							sbMap.put(tempDate.getTime(), sbMap.get(tempDate.getTime()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				
				long su_bought = 0;
				long active_users = 0;
				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					if(entry.getValue() >= 99999 && sbMap.get(entry.getKey()) != null) {
						su_bought = entry.getValue();
						active_users = sbMap.get(entry.getKey());
						
						su_bought = active_users + ((active_users * 40) / 100);
						
//						sbMap.put(entry.getKey(), active_users);
						pbMap.put(entry.getKey(), su_bought);
					}
				}
				
				List<Series> seriesList = new ArrayList<Series>();

				Series series = new Series();
				series.setName("Active Users");
				List<Object>	data = new ArrayList<Object>();
				for(Map.Entry<Date, Long> entry : sbMap.entrySet()) {
					Data dta = new Data();								
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Active Users: " + entry.getValue().longValue());
					data.add(dta);
				}

				series.setData(data);
				series.setColor("#fe5400");
				series.setShowInLegend(true);
				seriesList.add(series);

				series = new Series();
				series.setName("Licensed Users");
				data = new ArrayList<Object>();			

				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					Data dta = new Data();				
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue().longValue());
					dta.setInfo("Licensed Users: " + entry.getValue().longValue());
					data.add(dta);
				}

				series.setData(data);
				series.setColor("#4E387E");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				
				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("Active/Licensed Users"));
				if(!StringHelper.isEmpty(rangeFilter)) {
					highchart.setSubtitle(new Subtitle(subTitle));
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
//				logger.debug("by cdetails SU bought Utilized [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
				
			}
		}
		return cache.getData();
	}
	
	//..........
	public Map<String, Object> c360customersbyactivevssuboughtrateegnyte(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, int page, String sortBy, boolean ascending) throws Exception {
        
		InformationDAO informationDAO = new InformationDAO();
		Map<String, Object> fieldMap = new HashMap<String, Object>();
		Map<String, Object> cachedData = new HashMap<String, Object>();
		
		if(true) {
			fieldMap.clear();
			fieldMap.put("accountId", account.getId());
			fieldMap.put("entity", "CUSTOMER");
			fieldMap.put("type", "UPSELL");
			if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
				fieldMap.put("rangeFilter", (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter));
			}
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					fieldMap.put(mp.getKey(), mp.getValue()); 
				}
			}
			
			if(StringHelper.isEmpty(sortBy)) {
				sortBy = "suboughtUtilizedPercent";
			}
			
			int skip = 0;
			int limit = 15;
			int numTabsToShown = 7;
			skip = (page - 1) * limit;
			
			List<Information> informations = informationDAO.findAll(fieldMap, sortBy, ascending, skip, limit);
			long totalCount = informationDAO.countByFilters(account.getId(), "UPSELL", "CUSTOMER", fieldMap);
			
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

			cachedData.put("days", (informations.size() > 0) ? informations.get(0).getDays() : "x");
			cachedData.put("dateRange", (informations.size() > 0) ? informations.get(0).getDateRange() : null);
			cachedData.put("requestOn", "yes");
			cachedData.put("listName", "c360customersbyactivevssuboughtrateegnyte");
			cachedData.put("apiName", "c360customersbyactivevssuboughtrateegnyte");
			cachedData.put("sortBy", sortBy);
			cachedData.put("ascending", ascending);
			cachedData.put("rangeFilter", rangeFilter);
			cachedData.put("podId", podId);
			cachedData.put("customerTypeId", fieldMap.get("customerTypes"));
			cachedData.put("title", "Customers By Highest Users Utilization");
			cachedData.put("informations", informations);
			cachedData.put("pageCount", pageCount);
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					cachedData.put(mp.getKey(), mp.getValue());
				}
			}
			cachedData.put("C360_CUSTOMERS_BY_ACTIVE_VS_SU_BOUGHT_RATE_EGNYTE", "yes");
		}
		
		return cachedData;
	}
	
	public Map<String, Object> cdetailsengagementstatusmetric(Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
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

				long puBought = 0;
				long puUtilized = 0;
				long activeUsers = 0;
				String engStatus = "-";
				
				if(customer.getCustomFields() != null) {
					for(Field field : customer.getCustomFields()) {
						if(field.getName().equals("pu_bought") && field.getValue() != null) {
							puBought += Long.parseLong(field.getValue().toString());
						} else if(field.getName().equals("pu_utilized") && field.getValue() != null) {
							puUtilized += Long.parseLong(field.getValue().toString());
						} else if(field.getName().equals("active_users") && field.getValue() != null) {
							activeUsers += Long.parseLong(field.getValue().toString());
						} else if(field.getName().equals("engagement_status__c") && field.getValue() != null) {
							engStatus = field.getValue().toString();
						}
					}
				}

				for(Properties p : Utility.getInstance().getBaseEngagementStatusValues()) {
					if(p.getValue().toString().equalsIgnoreCase(engStatus)) {
						engStatus = p.getKey();
						break;
					}
				}
				
				metricsData.put("value01", engStatus.replaceAll(" ", ""));
				metricsData.put("value02", Utility.getInstance().calculateUpDownPercentage(puUtilized, puBought));
				metricsData.put("value03", Utility.getInstance().calculateUpDownPercentage(activeUsers, puUtilized));

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
	
	public Map<String, Object> c360engagementstatusmetric(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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
			
			CustomerDAO customerDAO = new CustomerDAO();
			Map<String, Object> metricsData = new HashMap<String, Object>();

			long engagementStatusTwo = 0;
			long engagementStatusThree = 0;
			
			int skip = 0;
			int limit = 200;
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
					if(customer.getCustomFields() != null) {
						for(Field field : customer.getCustomFields()) {
							if(field.getName().equals("engagement_status__c") && field.getValue() != null) {
								String engStatus = field.getValue().toString();
								if(engStatus.equalsIgnoreCase("2")) {
									engagementStatusTwo++;
								} else if(engStatus.equalsIgnoreCase("3")) {
									engagementStatusThree++;
								}
							}
						}
					}
				}
				
				skip += limit;
			}

			metricsData.put("value01", NumberHelper.format(engagementStatusThree));
			metricsData.put("value02", NumberHelper.format(engagementStatusTwo));
			metricsData.put("value03", null);

			String url1 = "/managecustomers?c__engagement_status__c=3";
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url1 +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			
			String url2 = "/managecustomers?c__engagement_status__c=2";
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url2 +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}

			metricsData.put("label01Url", url1);
			metricsData.put("label02Url", url2);
//			metricsData.put("label03Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType);

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360pumetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));
			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360currentmrrmetric(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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
			
			Calendar calendar = Calendar.getInstance();
			CustomerTimeseriesByDayContentIndex indexer = new CustomerTimeseriesByDayContentIndex();
			CustomerDAO customerDAO = new CustomerDAO();
			TimeseriesByDayDAO timeseriesByDayDAO = new TimeseriesByDayDAO();
			Map<String, Object> metricsData = new HashMap<String, Object>();
			
			boolean calculateAggregate = true;
			
			for(Map.Entry<String, Object> entry : paramMap.entrySet()) {
//				logger.debug(entry.getKey() + "\t" + entry.getValue());
				if(!(entry.getKey().equals("ignored") || entry.getKey().equals("customerTypes")) && entry.getValue() != null && !StringHelper.isEmpty(entry.getValue().toString())) {
//					logger.debug("\t" + entry.getKey() + entry.getValue() + "calculate with filters");
					calculateAggregate = false;
				}
			}
			
			Map<Integer, Double> values = new LinkedHashMap<Integer, Double>();
			
			Date firstOfThisMonth = Utility.getInstance().setMonthStartDate(Utility.getInstance().setTimeToBeginningOfDay(calendar)).getTime();
			Date lastOfThisMonth = Utility.getInstance().setMonthEndDate(Utility.getInstance().setTimeToEndOfDay(calendar)).getTime();
			values.put(calendar.get(Calendar.MONTH), 0d);
			
			calendar.add(Calendar.MONTH, -1);
			
			Date firstOfLastMonth = Utility.getInstance().setMonthStartDate(Utility.getInstance().setTimeToBeginningOfDay(calendar)).getTime();
			Date lastOfLastMonth = Utility.getInstance().setMonthEndDate(Utility.getInstance().setTimeToEndOfDay(calendar)).getTime();
			values.put(calendar.get(Calendar.MONTH), 0d);
			
			calendar.add(Calendar.MONTH, -1);
			
			Date firstOfLast2Month = Utility.getInstance().setMonthStartDate(Utility.getInstance().setTimeToBeginningOfDay(calendar)).getTime();
			Date lastOfLast2Month = Utility.getInstance().setMonthEndDate(Utility.getInstance().setTimeToEndOfDay(calendar)).getTime();
			values.put(calendar.get(Calendar.MONTH), 0d);
			
			Double currentMrr = 0d;
			Double months1Mrr = 0d;
			Double months2Mrr = 0d;
			
			if(!calculateAggregate) {
				int skip = 0;
				int limit = 200;
				while(true) {
					
					fieldMap.clear();
					fieldMap.put("accountId", account.getId());
					
					for(Map.Entry<String, Object> mp : paramMap.entrySet()){
						if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
							fieldMap.put(mp.getKey(), mp.getValue()); 
						}
					}
					
					List<Customer> customers = customerDAO.findAll(fieldMap, "normalizedName", true, skip, limit);
//					logger.debug("customers: " + customers.size()) ;
					if(customers.size() == 0) {
						break;
					}
					for(Customer customer : customers) {
						SearchResult sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "currentMrr", firstOfThisMonth, lastOfThisMonth, 365, 0);
						List<CustomerTimeseriesByDayContentDoc> documents = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
						if(documents != null && documents.size() > 0) {
							currentMrr = Double.parseDouble(documents.get(0).getFieldValue().toString());
							/*for(CustomerTimeseriesByDayContentDoc doc : documents) {
								if(doc.getFieldValue() != null) {
									currentMrr += Double.parseDouble(doc.getFieldValue().toString());
								}
							}*/
						}
						sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "currentMrr", firstOfLastMonth, lastOfLastMonth, 365, 0);
						documents = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
						if(documents != null && documents.size() > 0) {
							months1Mrr = Double.parseDouble(documents.get(0).getFieldValue().toString());
							/*for(CustomerTimeseriesByDayContentDoc doc : documents) {
								if(doc.getFieldValue() != null) {
									months1Mrr += Double.parseDouble(doc.getFieldValue().toString());
								}
							}*/
						}
						sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "currentMrr", firstOfLast2Month, lastOfLast2Month, 365, 0);
						documents = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
						if(documents != null && documents.size() > 0) {
							months2Mrr = Double.parseDouble(documents.get(0).getFieldValue().toString());
							/*for(CustomerTimeseriesByDayContentDoc doc : documents) {
								if(doc.getFieldValue() != null) {
									months2Mrr += Double.parseDouble(doc.getFieldValue().toString());
								}
							}*/
						}
						
					}
					
					skip += limit;
				}
				
			} else {
				
				List<TimeseriesByDay> timeseriesByDays = timeseriesByDayDAO.getData(account.getId(), "currentMrr", firstOfThisMonth, lastOfThisMonth);
				if(timeseriesByDays.size() > 0) {
					currentMrr = Double.parseDouble(timeseriesByDays.get(0).getFieldValue().toString());
				}
				/*for(TimeseriesByDay timeseriesByDay : timeseriesByDays) {
					if(timeseriesByDay.getFieldValue() != null) {
						currentMrr += Double.parseDouble(timeseriesByDay.getFieldValue().toString());
					}
				}*/
				timeseriesByDays = timeseriesByDayDAO.getData(account.getId(), "currentMrr", firstOfLastMonth, lastOfLastMonth);
				if(timeseriesByDays.size() > 0) {
					months1Mrr = Double.parseDouble(timeseriesByDays.get(0).getFieldValue().toString());
				}
				/*for(TimeseriesByDay timeseriesByDay : timeseriesByDays) {
					if(timeseriesByDay.getFieldValue() != null) {
						months1Mrr += Double.parseDouble(timeseriesByDay.getFieldValue().toString());
					}
				}*/
				timeseriesByDays = timeseriesByDayDAO.getData(account.getId(), "currentMrr", firstOfLast2Month, lastOfLast2Month);
				if(timeseriesByDays.size() > 0) {
					months2Mrr = Double.parseDouble(timeseriesByDays.get(0).getFieldValue().toString());
				}
				/*for(TimeseriesByDay timeseriesByDay : timeseriesByDays) {
					if(timeseriesByDay.getFieldValue() != null) {
						months2Mrr += Double.parseDouble(timeseriesByDay.getFieldValue().toString());
					}
				}*/
			}

			metricsData.put("value01", NumberHelper.format(currentMrr.longValue()));
			metricsData.put("value02", NumberHelper.format(months1Mrr.longValue()));
			metricsData.put("value03", NumberHelper.format(months2Mrr.longValue()));
			
			/*metricsData.put("value01", currentMrr);
			metricsData.put("value02", months1Mrr);
			metricsData.put("value03", months2Mrr);*/

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360pumetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));
			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsengagementstatustimeseries(Account account, PodConfiguration pc, String podId, String rangeFilter, String rangeStartDate, String rangeEndDate, String customerId, Integer cacheHours) throws Exception {
		PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
		String params = "podId=" + podId;
		if(!StringHelper.isEmpty(rangeFilter) && pc.isRangeApplied()) {
			params += "&rangeFilter=" + rangeFilter; 
		}
		if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
			params += "&rangeStartDate=" + rangeStartDate; 
			params += "&rangeEndDate=" + rangeEndDate; 
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

			if(customer != null) {
				long startTime = new Date().getTime();
				
				SimpleDateFormat dateFormatter = new SimpleDateFormat("MMM dd, yyyy");
				CustomerTimeseriesByDayContentIndex indexer = new CustomerTimeseriesByDayContentIndex();
			
				Calendar calendar = Calendar.getInstance();		
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				
				Long rangeValue = (rangeFilter.equalsIgnoreCase("date_range")) ? 90 : Long.parseLong(rangeFilter);
				Date rangeEnd = calendar.getTime();
				calendar.add(Calendar.DATE, -Integer.parseInt(String.valueOf(rangeValue)));
				Date rangeStart = calendar.getTime();
				String subTitle = dateFormatter.format(rangeStart) + " - " + dateFormatter.format(rangeEnd) + " (Last " + rangeValue + " days)";
				
				logger.debug(rangeStartDate + "\t" + rangeEndDate);
				if(!StringHelper.isEmpty(rangeStartDate) && !StringHelper.isEmpty(rangeEndDate)) {
					subTitle = rangeStartDate + " - " + rangeEndDate;
					calendar.setTime(dateFormatter.parse(rangeStartDate));
					rangeStart = calendar.getTime();
					calendar.setTime(dateFormatter.parse(rangeEndDate));
					rangeEnd = calendar.getTime();
				}
				
				logger.debug(rangeStart + "\t" + rangeEnd);
				long days = TimeUnit.MILLISECONDS.toDays(rangeEnd.getTime() - rangeStart.getTime());
				
				Map<Date, Long> pbMap = new LinkedHashMap<Date, Long>();
				
				SearchResult sr = indexer.searchDateRangeSpecificData(account.getId(), customer.getId(), "engagement_status__c", rangeStart, rangeEnd, Integer.parseInt(String.valueOf(days)), 0);
				if(sr != null && sr.getDocuments() != null) {
					List<CustomerTimeseriesByDayContentDoc> docs = (List<CustomerTimeseriesByDayContentDoc>) sr.getDocuments();
					for(CustomerTimeseriesByDayContentDoc ctd : docs) {
						if(!pbMap.containsKey(ctd.getDate()) && pbMap.size() < days && ctd.getFieldValue() != null) {
							pbMap.put(ctd.getDate(), Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						} else if(pbMap.containsKey(ctd.getDate()) && ctd.getFieldValue() != null) {
							pbMap.put(ctd.getDate(), pbMap.get(ctd.getDate()) + Long.parseLong(df1Precision.format(Double.parseDouble(ctd.getFieldValue().toString()))));
						}
					}
				}
				
				List<Series> seriesList = new ArrayList<Series>();

				Series series = new Series();
				series.setName("Engagement Status");
				List<Object> data = new ArrayList<Object>();			

				for(Map.Entry<Date, Long> entry : pbMap.entrySet()) {
					Data dta = new Data();				
					dta.setX(entry.getKey().getTime());					
					dta.setY(entry.getValue());
					dta.setInfo("Engagement Status: " + entry.getValue());
					data.add(dta);
				}

				series.setData(data);
				series.setColor("#4E387E");
				series.setShowInLegend(true);
				seriesList.add(series);
				
				
				Highchart highchart = new Highchart();

				highchart.setChart(new Chart("spline", null));
				highchart.setTitle(new Title("Engagement Status"));
				if(!StringHelper.isEmpty(rangeFilter)) {
					highchart.setSubtitle(new Subtitle(subTitle));
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
//				logger.debug("by cdetails SU bought Utilized [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));

				return newCache.getData();
				
			}
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360engagementstatusbadmetric(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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
			
			CustomerDAO customerDAO = new CustomerDAO();
			Map<String, Object> metricsData = new HashMap<String, Object>();

			long engagementStatusZero = 0;
			long engagementStatusOne = 0;
			
			int skip = 0;
			int limit = 200;
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
					if(customer.getCustomFields() != null) {
						for(Field field : customer.getCustomFields()) {
							if(field.getName().equals("engagement_status__c") && field.getValue() != null) {
								String engStatus = field.getValue().toString();
								if(engStatus.equalsIgnoreCase("0")) {
									engagementStatusZero++;
								} else if(engStatus.equalsIgnoreCase("1")) {
									engagementStatusOne++;
								}
							}
						}
					}
				}
				
				skip += limit;
			}

			metricsData.put("value01", NumberHelper.format(engagementStatusZero));
			metricsData.put("value02", NumberHelper.format(engagementStatusOne));
			metricsData.put("value03", null);
			
			String url1 = "/managecustomers?c__engagement_status__c=0";
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url1 +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}
			
			String url2 = "/managecustomers?c__engagement_status__c=1";
			for(Map.Entry<String, Object> mp : paramMap.entrySet()){
				if(mp.getValue() != null && !StringHelper.isEmpty(mp.getValue().toString())) {
					url2 +=  "&"+mp.getKey()+"=" + mp.getValue();							
				}
			}

			metricsData.put("label01Url", url1);
			metricsData.put("label02Url", url2);
//			metricsData.put("label03Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType);

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360pumetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));
			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> c360groupedengagementstatusmetric(Account account, PodConfiguration pc, String podId, String rangeFilter, Map<String, Object> paramMap, Integer cacheHours) throws Exception {

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
		if(cache == null && paramMap.get("groups") != null && !StringHelper.isEmpty(paramMap.get("groups").toString())) {
			
			long startTime = new Date().getTime();
			
			CustomerDAO customerDAO = new CustomerDAO();
			CustomerGroupDAO customerGroupDAO = new CustomerGroupDAO();
			Map<String, Object> metricsData = new HashMap<String, Object>();

			double grouped_pu_bought = 0;
			double grouped_max_pu_bought = 0;
			double grouped_pu_utilized = 0;
			double grouped_active_users = 0;
			Double grouped_utilized_percent = 0d;
			Double grouped_active_users_percemt = 0d;
			
			Object engStatus = new Object();
			
			CustomerGroup customerGroup = customerGroupDAO.findOneById(paramMap.get("groups").toString());
			if(customerGroup != null) {
				fieldMap.clear();
				fieldMap.put("accountId", account.getId());
				fieldMap.put("groups", customerGroup.getId());
				List<Customer> customers = customerDAO.findAll(fieldMap);
				for(Customer customer : customers) {
					if(customer.getCustomFields() != null) {
						for(com.crucialbits.cy.model.Field f : customer.getCustomFields()) {
							if(f.getName().equalsIgnoreCase("pu_bought") && f.getValue() != null) {
								grouped_pu_bought = Double.parseDouble(f.getValue().toString());
								if(grouped_pu_bought > grouped_max_pu_bought) {
									grouped_max_pu_bought = grouped_pu_bought;
								}
							} else if(f.getName().equalsIgnoreCase("pu_utilized") && f.getValue() != null) {
								grouped_pu_utilized += Double.parseDouble(f.getValue().toString());
							} else if(f.getName().equalsIgnoreCase("active_users") && f.getValue() != null) {
								grouped_active_users += Double.parseDouble(f.getValue().toString());
							}
						}
					}
				}
				
				logger.debug("puB: " + grouped_max_pu_bought);
				logger.debug("puU: " + grouped_pu_utilized);
				logger.debug("acU: " + grouped_active_users);
				
				String p = Utility.getInstance().calculateUpDownPercentage(grouped_pu_utilized, grouped_max_pu_bought);
				if(!StringHelper.isEmpty(p) && !p.contains("-")) {
					grouped_utilized_percent = Double.parseDouble(p);
				}
				logger.debug("guP: " + grouped_utilized_percent);
				
				p = Utility.getInstance().calculateUpDownPercentage(grouped_active_users, grouped_pu_utilized);
				if(!StringHelper.isEmpty(p) && !p.contains("-")) {
					grouped_active_users_percemt = Double.parseDouble(p);
				}
				logger.debug("auP: " + grouped_active_users_percemt);

				if(grouped_utilized_percent >= 90 && grouped_active_users_percemt >= 70) {
					engStatus = 3;
				} else if(grouped_utilized_percent >= 80 && grouped_active_users_percemt >= 60) {
					engStatus = 2;
				} else if(grouped_utilized_percent < 80 && grouped_active_users_percemt < 60) {
					engStatus = 0;
				} else if(grouped_utilized_percent < 80 && grouped_active_users_percemt >= 60) {
					engStatus = 1;
				} else if(grouped_utilized_percent >= 80 && grouped_active_users_percemt < 60) {
					engStatus = 1;
				}
				
				for(Properties prop : Utility.getInstance().getBaseEngagementStatusValues()) {
					if(prop.getValue().toString().equalsIgnoreCase(engStatus.toString())) {
						engStatus = prop.getKey();
						break;
					}
				}
				logger.debug("eng: " + engStatus);
			}

			metricsData.put("value01", engStatus.toString().replaceAll(" ", ""));
			metricsData.put("value02", NumberHelper.format(grouped_utilized_percent.longValue()));
			metricsData.put("value03", NumberHelper.format(grouped_active_users_percemt.longValue()));

//			metricsData.put("label01Url", url1);
//			metricsData.put("label02Url", url2);
//			metricsData.put("label03Url", "/supporttickets?accountmanager=" + accountManager + "&customertype=" + customerType);

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
			
			long endTime = new Date().getTime();
			long diff = endTime - startTime;
//			logger.debug("by c360pumetric [S]: " + TimeUnit.MILLISECONDS.toSeconds(diff) + "\t[M]: " + TimeUnit.MILLISECONDS.toMinutes(diff));
			
			return newCache.getData();
		}
		return cache.getData();
	}
	
	public Map<String, Object> cdetailsrenewalthreeboxmetricforegnyte(
			Account account, PodConfiguration pc, String podId, String rangeFilter, String customerId, Integer cacheHours) throws Exception {
		/*PodDataCacheDAO cacheDAO = new PodDataCacheDAO();
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
			cacheDAO.delete(fieldMap);*/

			Map<String, Object> fieldMap = new HashMap<String, Object>();
			Customer customer = new CustomerDAO().findOneById(customerId);
			if(customer != null) {

				String renewalDaysRemaining = "-";
				String startDate = "-";
				String endDate = "-";
				
				fieldMap.clear();
				fieldMap.put("accountId", customer.getAccountId());
				fieldMap.put("customerId", customer.getId());
				LicenseDAO licenseDAO = new LicenseDAO();
				List<License> licenses = licenseDAO.findAll(fieldMap, "renewalDate", false, 0, 0);
				Date today = new Date();
				Map<Integer, License> activeLicenses = new HashMap<Integer, License>();
				Map<Integer, License> inactiveLicenses = new HashMap<Integer, License>();

				for(int x = 0; x < licenses.size(); x++) {
					if(licenses.get(x).getRenewalDate() != null && licenses.get(x).getRenewalDate().after(today)) {
						activeLicenses.put(x, licenses.get(x));
					} else {
						inactiveLicenses.put(x, licenses.get(x));
					}
				}

				long df = -1;
				for(Map.Entry<Integer, License> entry : activeLicenses.entrySet()) {
					long diff = entry.getValue().getRenewalDate().getTime() - today.getTime();
					long daysDiff = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
					if(df == -1 || df > daysDiff) {
						df = daysDiff;
						startDate = entry.getValue().getFormattedContractDate();
						endDate = entry.getValue().getFormattedRenewalDate();
					}
				}
				if(df != -1) {
					renewalDaysRemaining = String.valueOf(df);
				} else {
					df = -1;
					for(Map.Entry<Integer, License> entry : inactiveLicenses.entrySet()) {
						if(entry.getValue().getRenewalDate() != null) {
							long diff = entry.getValue().getRenewalDate().getTime() - today.getTime();
							long daysDiff = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
							if(df == -1 || df < daysDiff) {
								df = daysDiff;
								startDate = entry.getValue().getFormattedContractDate();
								endDate = entry.getValue().getFormattedRenewalDate();
							}
						}
					}
					if(df != -1) {
						renewalDaysRemaining = String.valueOf(df);
					}
				}
				long billingCycle = 0;
				if(customer.getCustomFields() != null){
					
					for(Field field : customer.getCustomFields()){
						
						if(field.getName().equalsIgnoreCase("billing_cycle") && field.getValue() != null){
							billingCycle = Long.parseLong(field.getValue().toString());
						}
					}
				}
				
				
				Map<String, Object> podData = new HashMap<String, Object>();
				podData.put("customer", customer);
//				podData.put("renewalDaysRemaining", renewalDaysRemaining);
				podData.put("value01", renewalDaysRemaining);
				
				podData.put("value02", billingCycle);
				podData.put("value03", endDate.replaceAll(" ", "&nbsp;"));
				
				podData.put("forStringValues", "yes");
				
				
				PodDataCache newCache = new PodDataCache();
				newCache.setAccountId(account.getId());
				newCache.setPodId(podId);
				/*newCache.setParamHash(paramHash);*/
				newCache.setData(podData);
				newCache.setCreatedAt(new Date());

				/*newCache = cacheDAO.insert(newCache);*/
				return newCache.getData();
			}
		/*}
		return cache.getData();*/
		return null;
	}
}	