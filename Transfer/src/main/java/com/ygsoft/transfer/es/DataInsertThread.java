package com.ygsoft.transfer.es;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataInsertThread extends Thread {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataInsertThread.class);
	private static String index = "server_kpi_value";
	private static String type = "server_kpi_value";
	private List<Map<String, Object>> plist;

	public DataInsertThread(List<Map<String, Object>> plist) {
		this.plist = plist;
	}

	@Override
	public void run() {
		System.out.println(this.toString() + "插入数据开始: ");
		try {
			TransportClient client = ElasticSearchUtil.get();
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (Map<String, Object> map : plist) {
				String id = map.get("id").toString();
				IndexRequest request = client.prepareIndex(index, type, id).setSource(map).request();

				bulkRequest.add(request);
			}

			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			LOGGER.info("bulk save:=>Thread:{}, rsp:{}", this.getName(), bulkResponse);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
