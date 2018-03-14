package com.ang.consume.controller;

import com.ang.consumer.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;


/**
 * @author taylor
 */
@Controller
public class IndexController {
	
	@Autowired
	private IndexService indexService;

	@ResponseBody
	@RequestMapping(value = "/index", method = RequestMethod.GET)
	public String findServer(){
		return indexService.getUserName();
	}

	@ResponseBody
	@RequestMapping(value = "/test", method = RequestMethod.GET)
	public String test(){
		return indexService.getUserName();
	}


}
