package com.ang.consumer.service;

import org.springframework.stereotype.Service;

/**
 * @author taylor
 */
@Service
public class IndexServiceImpl implements IndexService {

	@Override
	public String getUserName() {
		 System.out.println("你要知道，当这句话打印了，说明你成功了。");
		return "谢谢你访问了我！";
	}

}
